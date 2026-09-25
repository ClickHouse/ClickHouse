-- A key the map does not hold reads the map value type's default. For `FixedString(N)` that default is
-- a run of N NUL bytes, which a pattern can match, while a text index on `mapValues(m)` stores terms
-- only for the elements the map holds, so the granule holding such a row could be skipped.

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET enable_full_text_index = 1;
-- Pinned because the runner randomizes them and every arm below depends on which spelling of the map
-- element reaches index analysis, and on the pattern being long enough to be answered by the index.
SET optimize_functions_to_subcolumns = 1;
SET text_index_like_min_pattern_length = 4;

DROP TABLE IF EXISTS t_fs;
CREATE TABLE t_fs (id UInt32, m Map(String, FixedString(6)),
                   INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_fs VALUES (1, map('other', 'abcdef')), (2, map('k', 'hello'));

-- S1: row 1 has no key `k`, so `m['k']` is six NUL bytes and matches. Both spellings of the map
-- element are admitted by index analysis, so both are pinned.
SELECT 'S1 oracle', count() FROM t_fs WHERE m['k'] LIKE concat(char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S1 subcolumn', count() FROM t_fs WHERE m['k'] LIKE concat(char(0), char(0), char(0), '%');
SELECT 'S1 arrayElement', count() FROM t_fs WHERE m['k'] LIKE concat(char(0), char(0), char(0), '%') SETTINGS optimize_functions_to_subcolumns = 0;

-- S2: a pattern the default cannot match must still prune, otherwise the fix costs every ordinary query.
SELECT 'S2 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_fs WHERE m['k'] LIKE '%zzz%') WHERE explain LIKE '%Granules: 0/2%';
SELECT 'S2 rows', count() FROM t_fs WHERE m['k'] LIKE '%zzz%';
SELECT 'S2 present rows', count() FROM t_fs WHERE m['k'] LIKE '%hel%';

DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (id UInt32, m Map(String, String),
                    INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_str VALUES (1, map('other', 'abcdef')), (2, map('k', 'hello'));

-- S3: a variable-width value type defaults to the empty string, which no such pattern matches, so
-- nothing changes for it.
SELECT 'S3 oracle', count() FROM t_str WHERE m['k'] LIKE concat(char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S3 rows', count() FROM t_str WHERE m['k'] LIKE concat(char(0), char(0), char(0), '%');
SELECT 'S3 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_str WHERE m['k'] LIKE concat(char(0), char(0), char(0), '%')) WHERE explain LIKE '%Granules: 0/2%';

DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (id UInt32, m Map(String, Nullable(FixedString(6))),
                     INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_null VALUES (1, map('other', 'abcdef')), (2, map('k', 'hello'));

-- S4: a Nullable value type defaults to NULL, for which the pattern is NULL and not true, so the
-- index stays usable.
SELECT 'S4 oracle', count() FROM t_null WHERE m['k'] LIKE concat(char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S4 rows', count() FROM t_null WHERE m['k'] LIKE concat(char(0), char(0), char(0), '%');
SELECT 'S4 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null WHERE m['k'] LIKE concat(char(0), char(0), char(0), '%')) WHERE explain LIKE '%Granules: 0/2%';

DROP TABLE IF EXISTS t_dyn;
CREATE TABLE t_dyn (id UInt32, k String, nk Nullable(String), m Map(String, FixedString(6)),
                    INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_dyn VALUES (1, 'k', 'k', map('other', 'abcdef')), (2, 'k', 'k', map('k', 'hello'));

-- S5: a non-constant map key is admitted too. The absent-key row must be returned, and a pattern the
-- default cannot match must still prune - a fix that declined for every non-constant key would pass
-- the first assertion and fail the second.
SELECT 'S5 oracle', count() FROM t_dyn WHERE m[k] LIKE concat(char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S5 rows', count() FROM t_dyn WHERE m[k] LIKE concat(char(0), char(0), char(0), '%');
SELECT 'S5 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dyn WHERE m[k] LIKE '%zzz%') WHERE explain LIKE '%Granules: 0/2%';

-- S6: a Nullable key makes the map element Nullable(FixedString(6)) while an absent key still reads
-- six NUL bytes, so the value to test has to come from the map value type and not from that type.
SELECT 'S6 oracle', count() FROM t_dyn WHERE m[nk] LIKE concat(char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S6 rows', count() FROM t_dyn WHERE m[nk] LIKE concat(char(0), char(0), char(0), '%');

DROP TABLE t_fs;
DROP TABLE t_str;
DROP TABLE t_null;
DROP TABLE t_dyn;
