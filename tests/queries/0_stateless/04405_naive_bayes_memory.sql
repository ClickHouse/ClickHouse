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
LAYOUT(NAIVE_BAYES(n 1 mode 'token'))
LIFETIME(0);

SYSTEM RELOAD DICTIONARY nb_mem;

-- Confirm every n-gram was indexed, so that the per-n-gram ratio checked below is meaningful.
SELECT throwIf(element_count != 100000, 'NaiveBayes dictionary did not load all n-grams')
FROM system.dictionaries WHERE database = currentDatabase() AND name = 'nb_mem' FORMAT Null;

-- Fail if each n-gram costs more than 256 bytes of resident memory.
SELECT throwIf(bytes_allocated / element_count > 256, 'NaiveBayes dictionary uses too much memory per n-gram')
FROM system.dictionaries WHERE database = currentDatabase() AND name = 'nb_mem' FORMAT Null;

DROP DICTIONARY nb_mem;
DROP TABLE nb_mem_src;
