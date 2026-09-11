-- Dots are legal in column names, so a table may declare both a Map `m` and a physical column named
-- exactly `m.key_nokey`. Skip-index analysis used to read that name as the map's element for the key
-- `nokey` and prune every granule whose map lacks that key, dropping rows the predicate matches.
-- One arm per skip-index condition class; every count must find the inserted row.

DROP TABLE IF EXISTS t_shadow_ngrambf;
CREATE TABLE t_shadow_ngrambf
(
    m Map(String, String),
    `m.key_nokey` String,
    INDEX idx mapKeys(m) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_ngrambf VALUES ({'abc' : 'x'}, 'hello');
SELECT 'ngrambf_v1 over mapKeys', count() FROM t_shadow_ngrambf WHERE `m.key_nokey` = 'hello';

DROP TABLE IF EXISTS t_shadow_bloom_filter;
CREATE TABLE t_shadow_bloom_filter
(
    m Map(String, String),
    `m.key_nokey` String,
    INDEX idx mapKeys(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_bloom_filter VALUES ({'abc' : 'x'}, 'hello');
SELECT 'bloom_filter over mapKeys, equals', count() FROM t_shadow_bloom_filter WHERE `m.key_nokey` = 'hello';
SELECT 'bloom_filter over mapKeys, in', count() FROM t_shadow_bloom_filter WHERE `m.key_nokey` IN ('hello', 'zzz');

DROP TABLE IF EXISTS t_shadow_text_keys;
CREATE TABLE t_shadow_text_keys
(
    m Map(String, String),
    `m.key_nokey` String,
    INDEX idx mapKeys(m) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_text_keys VALUES ({'abc' : 'x'}, 'hello');
SELECT 'text over mapKeys', count() FROM t_shadow_text_keys WHERE `m.key_nokey` = 'hello';

DROP TABLE IF EXISTS t_shadow_text_values;
CREATE TABLE t_shadow_text_values
(
    m Map(String, String),
    `m.key_nokey` String,
    INDEX idx mapValues(m) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_text_values VALUES ({'abc' : 'x'}, 'hello');
SELECT 'text over mapValues', count() FROM t_shadow_text_values WHERE `m.key_nokey` = 'hello';

-- Only the shadowed name loses the index. A genuine key subcolumn of the same map, in the same table,
-- must still prune: `m.key_zzz` is not a declared column, and the map has no key `zzz`.
SELECT 'genuine key subcolumn still prunes', trim(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_shadow_ngrambf WHERE m.key_zzz = 'x')
WHERE trim(explain) ILIKE 'Granules:%'
SETTINGS explain_query_plan_default = 'legacy';

DROP TABLE t_shadow_ngrambf;
DROP TABLE t_shadow_bloom_filter;
DROP TABLE t_shadow_text_keys;
DROP TABLE t_shadow_text_values;
