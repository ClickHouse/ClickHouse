-- Tags: no-fasttest, no-parallel
-- no-fasttest: a test with a fail point must run alone, and tests that run alone are kept out of the fast test.
-- no-parallel: the failpoint is server-wide and fires once, so another test that loads a cache dictionary can consume it.

-- A failure to reserve the update batches fails the load of the dictionary.
-- The next load updates keys, and dropping the dictionary stops its update threads.

DROP DICTIONARY IF EXISTS cache_dict_reserve_fault;
DROP TABLE IF EXISTS source_reserve_fault;

CREATE TABLE source_reserve_fault (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO source_reserve_fault SELECT number, toString(number * 3) FROM numbers(100);

CREATE DICTIONARY cache_dict_reserve_fault (id UInt64, value String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_reserve_fault'))
LIFETIME(MIN 1000 MAX 2000)
LAYOUT(CACHE(SIZE_IN_CELLS 128 MAX_THREADS_FOR_UPDATES 1 ALLOW_READ_EXPIRED_KEYS 0));

SYSTEM ENABLE FAILPOINT cache_dictionary_update_queue_throw_on_reserve;
SYSTEM RELOAD DICTIONARY cache_dict_reserve_fault; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT cache_dictionary_update_queue_throw_on_reserve;

SYSTEM RELOAD DICTIONARY cache_dict_reserve_fault;
SELECT countIf(dictGet('cache_dict_reserve_fault', 'value', number) != toString(number * 3)) FROM numbers(100);

DROP DICTIONARY cache_dict_reserve_fault;
DROP TABLE source_reserve_fault;
