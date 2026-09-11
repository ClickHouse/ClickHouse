-- Dots are legal in column names, so a table may declare both a Map `m` and a physical column named
-- exactly `m.key_nokey`. Skip-index analysis used to read that name as the map's element for the key
-- `nokey` and prune every granule whose map lacks that key, dropping rows the predicate matches.
-- One arm per condition class per index expression: an index over mapValues resolves through a
-- different branch, which probes the filter with the predicate's own value instead of the map key.
-- Every count must find the inserted row.

SET use_skip_indexes = 1;

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

DROP TABLE IF EXISTS t_shadow_ngrambf_values;
CREATE TABLE t_shadow_ngrambf_values
(
    m Map(String, String),
    `m.key_nokey` String,
    INDEX idx mapValues(m) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_ngrambf_values VALUES ({'abc' : 'x'}, 'hello');
SELECT 'ngrambf_v1 over mapValues', count() FROM t_shadow_ngrambf_values WHERE `m.key_nokey` = 'hello';

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

DROP TABLE IF EXISTS t_shadow_bloom_filter_values;
CREATE TABLE t_shadow_bloom_filter_values
(
    m Map(String, String),
    `m.key_nokey` String,
    INDEX idx mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_bloom_filter_values VALUES ({'abc' : 'x'}, 'hello');
SELECT 'bloom_filter over mapValues', count() FROM t_shadow_bloom_filter_values WHERE `m.key_nokey` = 'hello';

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

-- Subcolumn names are flat, so the claimant of the name need not be a top-level column: a Tuple element
-- (or a typed JSON path) named `m.key_nokey` claims it too, and the predicate reads that element.
DROP TABLE IF EXISTS t_shadow_tuple_element;
CREATE TABLE t_shadow_tuple_element
(
    t Tuple(m Map(String, String), `m.key_nokey` String),
    INDEX idx mapKeys(t.m) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_tuple_element VALUES (({'abc' : 'x'}, 'hello'));
SELECT 'ngrambf_v1 over mapKeys, tuple element', count() FROM t_shadow_tuple_element WHERE t.`m.key_nokey` = 'hello';

-- Only the shadowed name loses the index. A genuine key subcolumn of the same map, in the same table,
-- must still prune: `m.key_zzz` is not a declared column, and the map has no key `zzz`.
-- Parallel replicas can plan the read entirely remotely, and EXPLAIN then reports no Indexes section
-- at all, so both plan assertions pin enable_parallel_replicas = 0.
SELECT 'genuine key subcolumn still prunes', trim(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_shadow_ngrambf WHERE m.key_zzz = 'x')
WHERE trim(explain) ILIKE 'Granules:%'
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

-- The counts above also pass whenever the index is merely absent from the plan, so pin the plan too.
-- Both values come from one row, so the line reddens if EXPLAIN stops reporting an index at all.
SELECT 'index applied: shadowed, genuine',
       (SELECT countIf(trim(explain) ILIKE 'Name: idx')
        FROM (EXPLAIN indexes = 1 SELECT count() FROM t_shadow_ngrambf WHERE `m.key_nokey` = 'hello')),
       (SELECT countIf(trim(explain) ILIKE 'Name: idx')
        FROM (EXPLAIN indexes = 1 SELECT count() FROM t_shadow_ngrambf WHERE m.key_zzz = 'x'))
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

-- The count arm of the Tuple case passes just as well if every key subcolumn of a nested map stopped
-- being indexable, so assert the other side there too: `t.m.key_zzz` is claimed by nothing, and the map
-- has no key `zzz`, so the index must be applied and must prune. Expecting `1 1`.
SELECT 'nested genuine key subcolumn still prunes',
       countIf(trim(explain) ILIKE 'Name: idx'),
       countIf(trim(explain) ILIKE 'Granules: 0/1')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_shadow_tuple_element WHERE t.m.key_zzz = 'x')
SETTINGS explain_query_plan_default = 'legacy', enable_parallel_replicas = 0;

DROP TABLE t_shadow_ngrambf;
DROP TABLE t_shadow_ngrambf_values;
DROP TABLE t_shadow_bloom_filter;
DROP TABLE t_shadow_bloom_filter_values;
DROP TABLE t_shadow_text_keys;
DROP TABLE t_shadow_text_values;
DROP TABLE t_shadow_tuple_element;
