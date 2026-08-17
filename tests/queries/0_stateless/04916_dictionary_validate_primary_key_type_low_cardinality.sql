SET dictionary_validate_primary_key_type = 1;

CREATE DICTIONARY dict_lc_uint64 (`k` LowCardinality(UInt64), `v` UInt32)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'src'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

DESCRIBE dict_lc_uint64;

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
