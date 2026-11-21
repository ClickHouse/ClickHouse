-- Regression for UUID2 support in dictionary attributes.
-- Dictionary attributes live in `dictionary_attributes_list` (not `columns_list`), so they need their own
-- UUID2 handling: `DictionaryStructure` must accept a `UUID2` attribute, and the `uuid_type_version` setting
-- must materialize a bare `UUID` attribute to `UUID2` on the initiator, exactly like table columns.

DROP DICTIONARY IF EXISTS 04541_dict_explicit;
DROP DICTIONARY IF EXISTS 04541_dict_bare;
DROP TABLE IF EXISTS 04541_src;

CREATE TABLE 04541_src (id UInt64, u UUID2, n Nullable(UUID2)) ENGINE = Memory;
INSERT INTO 04541_src VALUES (1, '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61f0c404-5cb3-11e7-907b-a6006ad3dba0');

-- An explicit UUID2 attribute must be accepted and returned with its canonical (correctly-sorting) value.
CREATE DICTIONARY 04541_dict_explicit (id UInt64, u UUID2, n Nullable(UUID2))
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '04541_src' DB currentDatabase()))
LAYOUT(HASHED())
LIFETIME(0);

SELECT dictGet('04541_dict_explicit', 'u', toUInt64(1)) AS v, toTypeName(v);
SELECT dictGet('04541_dict_explicit', 'n', toUInt64(1)) AS v, toTypeName(v);

-- Under uuid_type_version = 2 a bare `UUID` attribute (including nested inside `Nullable`) must materialize to
-- `UUID2` on the initiator, so the stored type and the value read back are the correctly-sorting variant.
SET uuid_type_version = 2;

CREATE DICTIONARY 04541_dict_bare (id UInt64, u UUID, n Nullable(UUID))
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '04541_src' DB currentDatabase()))
LAYOUT(HASHED())
LIFETIME(0);

SELECT dictGet('04541_dict_bare', 'u', toUInt64(1)) AS v, toTypeName(v);
SELECT dictGet('04541_dict_bare', 'n', toUInt64(1)) AS v, toTypeName(v);

-- The materialized attribute types are visible in the stored definition.
SELECT arraySort(`attribute.types`) FROM system.dictionaries
WHERE database = currentDatabase() AND name = '04541_dict_bare';

DROP DICTIONARY 04541_dict_explicit;
DROP DICTIONARY 04541_dict_bare;
DROP TABLE 04541_src;
