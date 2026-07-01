-- This test bounds the resident memory of a NAIVE_BAYES dictionary to a small number of bytes per
-- n-gram, so that a change which makes the dictionary store much more per n-gram than it needs to
-- will fail here. The current representation keeps each n-gram in well under 256 bytes.
--
-- A dictionary load does not run under the triggering query's max_memory_usage tracker, so this
-- test checks the resident bytes_allocated reported by system.dictionaries rather than the peak
-- memory used while the dictionary is being built.

DROP DICTIONARY IF EXISTS nb_mem;
DROP TABLE IF EXISTS nb_mem_src;

CREATE TABLE nb_mem_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);

-- Build a source table of 100000 distinct n-grams spread evenly across 5 classes.
INSERT INTO nb_mem_src SELECT number % 5, toString(number), 1 + (number % 100) FROM numbers(100000);

CREATE DICTIONARY nb_mem
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_mem_src' DB currentDatabase()))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);

SYSTEM RELOAD DICTIONARY nb_mem;

-- The first column confirms every n-gram was indexed, so the per-n-gram ratio is meaningful. The
-- next two pin that ratio to a band: more than 30 bytes means the index really was built and
-- populated, and fewer than 256 bytes means the footprint stays compact. All three are 1 when healthy.
SELECT
    element_count = 100000,
    bytes_allocated / element_count > 30,
    bytes_allocated / element_count < 256
FROM system.dictionaries WHERE database = currentDatabase() AND name = 'nb_mem';

-- store_source retains the source rows, so the same model with store_source enabled reports a larger
-- footprint than without it (the retained columns are counted in bytes_allocated).
CREATE DICTIONARY nb_mem_store
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_mem_src' DB currentDatabase()))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' store_source 1))
LIFETIME(0);
SYSTEM RELOAD DICTIONARY nb_mem_store;

SELECT
    (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'nb_mem_store')
    > (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'nb_mem');

DROP DICTIONARY nb_mem_store;
DROP DICTIONARY nb_mem;
DROP TABLE nb_mem_src;
