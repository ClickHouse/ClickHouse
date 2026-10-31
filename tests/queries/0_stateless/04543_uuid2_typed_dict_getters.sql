-- Regression for the typed dictionary getters with UUID2.
-- `dictGetUUID2` / `dictGetUUID2OrDefault` must exist, and because what a bare `UUID` in a dictionary
-- definition resolves to depends on the `uuid_type_version` setting, both `dictGetUUID*` and `dictGetUUID2*`
-- must accept an attribute of either logical type and convert the result losslessly (the textual value is
-- preserved) instead of failing existing queries.

DROP DICTIONARY IF EXISTS 04543_dict_uuid2;
DROP DICTIONARY IF EXISTS 04543_dict_uuid;
DROP TABLE IF EXISTS 04543_src;

CREATE TABLE 04543_src (id UInt64, u UUID, u2 UUID2, x UInt64) ENGINE = Memory;
INSERT INTO 04543_src VALUES (1, '61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61f0c404-5cb3-11e7-907b-a6006ad3dba0', 42);

CREATE DICTIONARY 04543_dict_uuid2 (id UInt64, u2 UUID2, x UInt64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '04543_src' DB currentDatabase()))
LAYOUT(HASHED())
LIFETIME(0);

CREATE DICTIONARY 04543_dict_uuid (id UInt64, u UUID, x UInt64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '04543_src' DB currentDatabase()))
LAYOUT(HASHED())
LIFETIME(0);

-- The matching typed getters.
SELECT dictGetUUID2('04543_dict_uuid2', 'u2', toUInt64(1)) AS v, toTypeName(v);
SELECT dictGetUUID2OrDefault('04543_dict_uuid2', 'u2', toUInt64(1), toUUID2('550e8400-0000-0000-0000-000000000000')) AS v, toTypeName(v);
SELECT dictGetUUID2OrDefault('04543_dict_uuid2', 'u2', toUInt64(42), toUUID2('550e8400-0000-0000-0000-000000000000')) AS v, toTypeName(v);

-- The cross-type getters: the value keeps its textual form, only the returned logical type differs.
SELECT dictGetUUID('04543_dict_uuid2', 'u2', toUInt64(1)) AS v, toTypeName(v);
SELECT dictGetUUID2('04543_dict_uuid', 'u', toUInt64(1)) AS v, toTypeName(v);
SELECT dictGetUUIDOrDefault('04543_dict_uuid2', 'u2', toUInt64(42), toUUID('550e8400-0000-0000-0000-000000000000')) AS v, toTypeName(v);
SELECT dictGetUUID2OrDefault('04543_dict_uuid', 'u', toUInt64(42), toUUID2('550e8400-0000-0000-0000-000000000000')) AS v, toTypeName(v);

-- The non-constant default goes through the short-circuit path and must be converted the same way.
SELECT dictGetUUIDOrDefault('04543_dict_uuid2', 'u2', number + 1, materialize(toUUID('550e8400-0000-0000-0000-000000000000'))) AS v, toTypeName(v) FROM numbers(2) ORDER BY v;
SELECT dictGetUUID2OrDefault('04543_dict_uuid', 'u', number + 1, materialize(toUUID2('550e8400-0000-0000-0000-000000000000'))) AS v, toTypeName(v) FROM numbers(2) ORDER BY v;

-- A genuinely different attribute type must still be rejected.
SELECT dictGetUUID2('04543_dict_uuid2', 'x', toUInt64(1)); -- { serverError TYPE_MISMATCH }
SELECT dictGetUUID('04543_dict_uuid', 'x', toUInt64(1)); -- { serverError TYPE_MISMATCH }

DROP DICTIONARY 04543_dict_uuid2;
DROP DICTIONARY 04543_dict_uuid;
DROP TABLE 04543_src;
