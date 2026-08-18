-- A keyValuePairs text index must never change the result of any existing map / text-search function
-- versus a plain scan: it accelerates m['key'] = value and m['key'] IN (...) by granule pruning, and for
-- every other function it simply must not be consulted (no wrong pruning). Each check compares the same
-- data in a Memory table (no index) against a MergeTree table carrying the index; every result must be 1.
-- The map has no duplicate keys, so the m['key'] accessor is unambiguous across subcolumn settings.

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_idx;

CREATE TABLE t_mem (id UInt64, m Map(String, String)) ENGINE = Memory;
CREATE TABLE t_idx (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;

INSERT INTO t_mem VALUES
    (1, {'level':'error','svc':'api'}),
    (2, {'level':'info','svc':'web'}),
    (3, {'level':'warn'}),
    (4, {'env':'prod','svc':'api'}),
    (5, {'level':'error','host':'h1'}),
    (6, {});
INSERT INTO t_idx SELECT * FROM t_mem;

-- Helper shape: index result (skip indexes on, accessor lowered to arrayElement) must equal the Memory scan.
-- mapContains* / *Like existence functions (index must not be used for them here):
SELECT 'mapContainsKey' AS q, (SELECT arraySort(groupArray(id)) FROM t_mem WHERE mapContainsKey(m, 'level')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE mapContainsKey(m, 'level') SETTINGS use_skip_indexes = 1) AS ok;
SELECT 'mapContainsKey absent', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE mapContainsKey(m, 'nope')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE mapContainsKey(m, 'nope') SETTINGS use_skip_indexes = 1);
SELECT 'mapContainsValue', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE mapContainsValue(m, 'api')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE mapContainsValue(m, 'api') SETTINGS use_skip_indexes = 1);
SELECT 'mapContainsValue absent', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE mapContainsValue(m, 'nope')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE mapContainsValue(m, 'nope') SETTINGS use_skip_indexes = 1);
SELECT 'mapContainsKeyLike', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE mapContainsKeyLike(m, 'lev%')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE mapContainsKeyLike(m, 'lev%') SETTINGS use_skip_indexes = 1);
SELECT 'mapContainsValueLike', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE mapContainsValueLike(m, '%rror%')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE mapContainsValueLike(m, '%rror%') SETTINGS use_skip_indexes = 1);
SELECT 'mapContainsValueLike prefix', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE mapContainsValueLike(m, 'ap%')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE mapContainsValueLike(m, 'ap%') SETTINGS use_skip_indexes = 1);

-- Map subcolumn / array functions:
SELECT 'mapKeys', (SELECT arraySort(groupArray(arraySort(mapKeys(m)))) FROM t_mem) = (SELECT arraySort(groupArray(arraySort(mapKeys(m)))) FROM t_idx SETTINGS use_skip_indexes = 1);
SELECT 'mapValues', (SELECT arraySort(groupArray(arraySort(mapValues(m)))) FROM t_mem) = (SELECT arraySort(groupArray(arraySort(mapValues(m)))) FROM t_idx SETTINGS use_skip_indexes = 1);
SELECT 'has(mapKeys)', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE has(mapKeys(m), 'svc')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE has(mapKeys(m), 'svc') SETTINGS use_skip_indexes = 1);
SELECT 'mapExtractKeyLike', (SELECT arraySort(groupArray((id, mapExtractKeyLike(m, 's%')))) FROM t_mem) = (SELECT arraySort(groupArray((id, mapExtractKeyLike(m, 's%')))) FROM t_idx SETTINGS use_skip_indexes = 1);
SELECT 'mapExtractValueLike', (SELECT arraySort(groupArray((id, mapExtractValueLike(m, 'a%')))) FROM t_mem) = (SELECT arraySort(groupArray((id, mapExtractValueLike(m, 'a%')))) FROM t_idx SETTINGS use_skip_indexes = 1);

-- m['key'] accessor forms (equals and IN are index-accelerated; LIKE / startsWith / endsWith are not).
SELECT 'element equals', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE m['level'] = 'error') = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE m['level'] = 'error' SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0);
SELECT 'element equals subcol', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE m['level'] = 'error') = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE m['level'] = 'error' SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 1);
SELECT 'element IN', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE m['level'] IN ('error', 'warn')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE m['level'] IN ('error', 'warn') SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0);
SELECT 'element LIKE', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE m['level'] LIKE 'err%') = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE m['level'] LIKE 'err%' SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0);
SELECT 'startsWith', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE startsWith(m['level'], 'err')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE startsWith(m['level'], 'err') SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0);
SELECT 'endsWith', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE endsWith(m['level'], 'or')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE endsWith(m['level'], 'or') SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0);

-- Compound predicates mixing accelerated and non-accelerated forms:
SELECT 'AND', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE mapContainsKey(m, 'svc') AND m['level'] = 'error') = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE mapContainsKey(m, 'svc') AND m['level'] = 'error' SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0);
SELECT 'OR', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE mapContainsValue(m, 'api') OR m['level'] = 'warn') = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE mapContainsValue(m, 'api') OR m['level'] = 'warn' SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0);
SELECT 'NOT', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE NOT mapContainsKey(m, 'host')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE NOT mapContainsKey(m, 'host') SETTINGS use_skip_indexes = 1);
SELECT 'IN AND NOT', (SELECT arraySort(groupArray(id)) FROM t_mem WHERE m['level'] IN ('error', 'info') AND NOT mapContainsKey(m, 'host')) = (SELECT arraySort(groupArray(id)) FROM t_idx WHERE m['level'] IN ('error', 'info') AND NOT mapContainsKey(m, 'host') SETTINGS use_skip_indexes = 1, optimize_functions_to_subcolumns = 0);

DROP TABLE t_mem;
DROP TABLE t_idx;
