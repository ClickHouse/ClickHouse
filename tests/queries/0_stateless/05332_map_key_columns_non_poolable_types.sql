-- Types whose serializations cannot be pooled (`JSON`) must not break building the
-- `with_key_columns` serialization. A Map inside a `Variant` keeps the basic layout,
-- so its key type is not restricted; a Map value with dynamic subcolumns is rejected.

CREATE TABLE t_map_kc_json (id UInt64, m Variant(Map(JSON, UInt64), UInt8))
ENGINE = MergeTree ORDER BY id SETTINGS map_serialization_version = 'with_key_columns';
DROP TABLE t_map_kc_json;

CREATE TABLE t_map_kc_json (id UInt64, m Map(String, JSON))
ENGINE = MergeTree ORDER BY id SETTINGS map_serialization_version = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_map_kc_json (id UInt64, m Map(String, Array(Dynamic)))
ENGINE = MergeTree ORDER BY id SETTINGS map_serialization_version = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE t_map_kc_json (id UInt64, m Variant(Map(String, UInt64), JSON))
ENGINE = MergeTree ORDER BY id SETTINGS map_serialization_version = 'with_key_columns';
DROP TABLE t_map_kc_json;
