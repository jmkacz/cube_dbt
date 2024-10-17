import re
from cube_dbt.dump import dump

# As of 2024-10-17, the valid "Dimension Types" listed on
# https://cube.dev/docs/reference/data-model/types-and-formats#dimension-types
# are: time, string, number, boolean, and geo
VALID_DIMENSION_TYPES = [
  'boolean',
  'geo',
  'number',
  'string',
  'time',
]
# Other System's Type => Cube Type
TYPE_MAPPINGS = {
  # Unknown
  'bool': 'boolean',

  # Snowflake (numeric)
  # 'number': 'number',
  'decimal': 'number',
  'numeric': 'number',
  'int': 'number',
  'integer': 'number',
  'bigint': 'number',
  'smallint': 'number',
  'tinyint': 'number',
  'byteint': 'number',
  'float': 'number',
  'float4': 'number',
  'float8': 'number',
  'double': 'number',
  'double precision': 'number',
  'real': 'number',

  # Snowflake (string & binary)
  'varchar': 'string',
  'char': 'string',
  'character': 'string',
  # 'string': 'string',
  'text': 'string',
  'binary': 'string',
  'varbinary': 'string',

  # Snowflake (boolean)
  # 'boolean': 'boolean',

  # Snowflake (data & time)
  'date': 'time',
  'datetime': 'time',
  # 'time': 'time',
  'timestamp': 'time',
  'timestamp_ltz': 'time',
  'timestamp_ntz': 'time',
  'timestamp_tz': 'time',

  # Snowflake (semi-structured)
  'variant': 'string',
  'object': 'string',
  'array': 'string',

  # Snowflake (geospatial)
  'geography': 'geo',
  'geometry': 'string',

  # Snowflake (vector)
  'vector': 'string',
}

class Column:
  def __init__(self, model_name: str, column_dict: dict) -> None:
    self._model_name = model_name
    self._column_dict = column_dict
    pass
  
  def __repr__(self) -> str:
    return str(self._column_dict)
  
  @property
  def name(self) -> str:
    return self._column_dict['name']
  
  @property
  def description(self) -> str:
    return self._column_dict['description']
  
  @property
  def sql(self) -> str:
    return self._column_dict['name']
  
  @property
  def type(self) -> str:
    if not 'data_type' in self._column_dict or self._column_dict['data_type'] == None:
      return 'string'
  
    # Normalize the data_type value, downcasing it, and removing extra information. 
    # ex. STRING => string
    source_data_type = self._column_dict['data_type'].lower()
    # ex. timestamp(3) => timestamp
    source_data_type = re.sub(r'\(\d+\)', '', source_data_type)
    # ex. number(38, 0) => number
    source_data_type = re.sub(r'\(\d+,\s*\d+\)', '', source_data_type)

    if source_data_type in TYPE_MAPPINGS:
      cube_data_type = TYPE_MAPPINGS[source_data_type]
    else:
      cube_data_type = source_data_type

    if cube_data_type not in VALID_DIMENSION_TYPES:
      raise RuntimeError(f"Unknown column type of {self._model_name}.{self.name}: {self._column_dict['data_type']}")

    return cube_data_type
  
  @property
  def meta(self) -> dict:
    return self._column_dict['meta']
  
  @property
  def primary_key(self) -> bool:
    """
    Convention: if the column is marked with the 'primary_key' tag,
    it will be mapped to a primary key dimension
    """
    return 'primary_key' in self._column_dict['tags']

  def _as_dimension(self) -> dict:
    data = {}
    data['name'] = self.name
    if self.description:
      data['description'] = self.description
    data['sql'] = self.sql
    data['type'] = self.type
    if self.primary_key:
      data['primary_key'] = True
    if self.meta:
      data['meta'] = self.meta
    return data
  
  def as_dimension(self) -> str:
    """
    For use in Jinja:
    {{ dbt.model('name').column('name').as_dimension() }}
    """
    return dump(self._as_dimension(), indent=8)
