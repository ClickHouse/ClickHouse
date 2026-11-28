CREATE TABLE src (k UInt64, v UInt32) ENGINE = Memory;
INSERT INTO src VALUES (1, 100), (2, 200), (7, 700);

SET dictionary_validate_primary_key_type = 1;

CREATE DICTIONARY dict_lc_uint64 (`k` LowCardinality(UInt64), `v` UInt32)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'src'))
LIFETIME(0)
LAYOUT(FLAT());

DESCRIBE dict_lc_uint64;

SELECT dictGet(currentDatabase() || '.dict_lc_uint64', 'v', toUInt64(1));
SELECT dictGet(currentDatabase() || '.dict_lc_uint64', 'v', toUInt64(7));
SELECT dictHas(currentDatabase() || '.dict_lc_uint64', toUInt64(2));
SELECT dictHas(currentDatabase() || '.dict_lc_uint64', toUInt64(99));
SELECT * FROM dict_lc_uint64 ORDER BY k;
SELECT status FROM system.dictionaries WHERE database = currentDatabase() AND name = 'dict_lc_uint64';

CREATE DICTIONARY dict_lc_string (`k` LowCardinality(String), `v` UInt32)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'src'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT()); -- { serverError BAD_ARGUMENTS }

CREATE DICTIONARY dict_nullable_uint64 (`k` Nullable(UInt64), `v` UInt32)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'src'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT()); -- { serverError BAD_ARGUMENTS }

CREATE DICTIONARY dict_lc_nullable_uint64 (`k` LowCardinality(Nullable(UInt64)), `v` UInt32)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'src'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT()); -- { serverError BAD_ARGUMENTS }

CREATE DICTIONARY dict_uint32 (`k` UInt32, `v` UInt32)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'src'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT()); -- { serverError BAD_ARGUMENTS }

SET dictionary_validate_primary_key_type = 0;

CREATE DICTIONARY dict_lc_uint64_unvalidated (`k` LowCardinality(UInt64), `v` UInt32)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'src'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

DESCRIBE dict_lc_uint64_unvalidated;

SET dictionary_validate_primary_key_type = 1;
SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE src_lc (k LowCardinality(UInt64), v UInt32) ENGINE = Memory;
INSERT INTO src_lc VALUES (3, 300), (4, 400);

CREATE DICTIONARY dict_lc_source (`k` LowCardinality(UInt64), `v` UInt32)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'src_lc'))
LIFETIME(0)
LAYOUT(FLAT());

SELECT dictGet(currentDatabase() || '.dict_lc_source', 'v', toUInt64(3));
SELECT dictHas(currentDatabase() || '.dict_lc_source', toUInt64(4));
SELECT dictHas(currentDatabase() || '.dict_lc_source', toUInt64(99));
SELECT * FROM dict_lc_source ORDER BY k;
