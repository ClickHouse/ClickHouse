-- A key the map does not hold reads the map value type's default. For `FixedString(N)` that default is
-- a run of N NUL bytes, which a pattern can match, while a text index on `mapValues(m)` stores terms
-- only for the elements the map holds, so the index could skip such a row. The arms check that it is
-- returned, and that a predicate the default does not satisfy still prunes.

SET explain_query_plan_default = 'legacy';
SET enable_full_text_index = 1;
-- The runner randomizes `optimize_functions_to_subcolumns`, which decides which spelling of the map
-- element reaches index analysis. S8's pattern has four literal bytes, which the index answers only
-- while `text_index_like_min_pattern_length` is at most 4.
SET optimize_functions_to_subcolumns = 1;
SET text_index_like_min_pattern_length = 4;

DROP TABLE IF EXISTS t_fs;
CREATE TABLE t_fs (id UInt32, m Map(String, FixedString(6)),
                   INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_fs VALUES (1, map('other', 'abcdef')), (2, map('k', 'hello'));

-- S1: row 1 has no key `k`, so `m['k']` is six NUL bytes and matches. Both spellings of the map
-- element are admitted by index analysis, so both are pinned.
SELECT 'S1 oracle', count() FROM t_fs WHERE m['k'] LIKE concat('%', char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S1 subcolumn', count() FROM t_fs WHERE m['k'] LIKE concat('%', char(0), char(0), char(0), '%');
SELECT 'S1 arrayElement', count() FROM t_fs WHERE m['k'] LIKE concat('%', char(0), char(0), char(0), '%') SETTINGS optimize_functions_to_subcolumns = 0;

-- S2: a pattern the default cannot match still prunes.
SELECT 'S2 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_fs WHERE m['k'] LIKE '%zzz%') WHERE explain LIKE '%Granules: 0/2%';
SELECT 'S2 present rows', count() FROM t_fs WHERE m['k'] LIKE '%hel%';

DROP TABLE IF EXISTS t_str;
CREATE TABLE t_str (id UInt32, m Map(String, String),
                    INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_str VALUES (1, map('other', 'abcdef')), (2, map('k', 'hello'));

-- S3: a variable-width value type defaults to the empty string, which no such pattern matches, so
-- nothing changes for it.
SELECT 'S3 oracle', count() FROM t_str WHERE m['k'] LIKE concat('%', char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S3 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_str WHERE m['k'] LIKE concat('%', char(0), char(0), char(0), '%')) WHERE explain LIKE '%Granules: 0/2%';

DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (id UInt32, m Map(String, Nullable(FixedString(6))),
                     INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_null VALUES (1, map('other', 'abcdef')), (2, map('k', 'hello'));

-- S4: a Nullable value type defaults to NULL, for which the pattern is NULL and not true, so the
-- index stays usable.
SELECT 'S4 oracle', count() FROM t_null WHERE m['k'] LIKE concat('%', char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S4 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null WHERE m['k'] LIKE concat('%', char(0), char(0), char(0), '%')) WHERE explain LIKE '%Granules: 0/2%';

DROP TABLE IF EXISTS t_dyn;
CREATE TABLE t_dyn (id UInt32, k String, nk Nullable(String), m Map(String, FixedString(6)),
                    INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_dyn VALUES (1, 'k', 'k', map('other', 'abcdef')), (2, 'k', 'k', map('k', 'hello'));

-- S5: a non-constant map key is admitted too. The absent-key row must be returned, and a pattern the
-- default cannot match must still prune - a fix that declined for every non-constant key would pass
-- the first assertion and fail the second.
SELECT 'S5 oracle', count() FROM t_dyn WHERE m[k] LIKE concat('%', char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S5 rows', count() FROM t_dyn WHERE m[k] LIKE concat('%', char(0), char(0), char(0), '%');
SELECT 'S5 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dyn WHERE m[k] LIKE '%zzz%') WHERE explain LIKE '%Granules: 0/2%';

-- S6: a Nullable key makes the map element Nullable(FixedString(6)) while an absent key still reads
-- six NUL bytes, so the value to test has to come from the map value type and not from that type.
SELECT 'S6 oracle', count() FROM t_dyn WHERE m[nk] LIKE concat('%', char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S6 rows', count() FROM t_dyn WHERE m[nk] LIKE concat('%', char(0), char(0), char(0), '%');

-- S7: as S1 and S2, for regular expressions.
SELECT 'S7 oracle', count() FROM t_fs WHERE match(m['k'], concat(char(0), char(0), char(0), '.*')) SETTINGS use_skip_indexes = 0;
SELECT 'S7 subcolumn', count() FROM t_fs WHERE match(m['k'], concat(char(0), char(0), char(0), '.*'));
SELECT 'S7 arrayElement', count() FROM t_fs WHERE match(m['k'], concat(char(0), char(0), char(0), '.*')) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'S7 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_fs WHERE match(m['k'], 'zzz')) WHERE explain LIKE '%Granules: 0/2%';

DROP TABLE IF EXISTS t_arr;
CREATE TABLE t_arr (id UInt32, m Map(String, FixedString(6)),
                    INDEX tix mapValues(m) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_arr VALUES (1, map('other', 'abcdef')), (2, map('k', 'hellos'));

-- S8: as S1, for ILIKE on an `array` index, which answers the pattern by scanning its dictionary.
SELECT 'S8 oracle', count() FROM t_arr WHERE m['k'] ILIKE concat('%', char(0), char(0), char(0), char(0), '%') SETTINGS use_skip_indexes = 0;
SELECT 'S8 rows', count() FROM t_arr WHERE m['k'] ILIKE concat('%', char(0), char(0), char(0), char(0), '%');
SELECT 'S8 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_arr WHERE m['k'] ILIKE '%zzzz%') WHERE explain LIKE '%Granules: 0/2%';

DROP TABLE IF EXISTS t_post;
CREATE TABLE t_post (id UInt32, m Map(String, FixedString(5)),
                     INDEX tix mapValues(m) TYPE text(tokenizer = splitByString(['abc']), postprocessor = if(match(mapValues(m), '^[a-z]+$'), mapValues(m), 'x')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_post VALUES (1, map('k', 'hello')), (2, map('k', '12345'));

-- S9: the token searches apply the index postprocessor, which maps both the needle's five NUL bytes and
-- `12345` to `x`, so row 2 matches. The default holds the needle, and the index must still be used, or
-- the postprocessor is not applied and row 2 is lost.
SELECT 'S9 hasAnyTokens', groupArray(id) FROM (SELECT id FROM t_post WHERE hasAnyTokens(m['k'], concat(repeat(char(0), 5), 'abc'), 'splitByString([\'abc\'])') ORDER BY id);
SELECT 'S9 hasAllTokens', groupArray(id) FROM (SELECT id FROM t_post WHERE hasAllTokens(m['k'], concat(repeat(char(0), 5), 'abc'), 'splitByString([\'abc\'])') ORDER BY id);
SELECT 'S9 hasPhrase', groupArray(id) FROM (SELECT id FROM t_post WHERE hasPhrase(m['k'], concat(repeat(char(0), 5), 'abc'), 'splitByString([\'abc\'])') ORDER BY id);

DROP TABLE IF EXISTS t_cast;
CREATE TABLE t_cast (id UInt32, m Map(String, Nullable(String)),
                     INDEX tix mapValues(m) TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_cast VALUES (1, map('k', 'abcdef')), (2, map('k', 'hello'));

-- S10: the cast throws on the NULL default, but every row holds the key, so the query must not fail.
SELECT 'S10 subcolumn', count() FROM t_cast WHERE CAST(m['k'], 'String') LIKE '%hel%';
SELECT 'S10 arrayElement', count() FROM t_cast WHERE CAST(m['k'], 'String') LIKE '%hel%' SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_fs;
DROP TABLE t_str;
DROP TABLE t_null;
DROP TABLE t_dyn;
DROP TABLE t_arr;
DROP TABLE t_post;
DROP TABLE t_cast;
