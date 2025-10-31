SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_text_index_add_hint = 1;

DROP TABLE IF EXISTS t_text_index_hint_map;

CREATE TABLE t_text_index_hint_map
(
    m Map(String, String),
    INDEX idx_mk (mapKeys(m)) TYPE text(tokenizer = array) GRANULARITY 4,
    INDEX idx_mv (mapValues(m)) TYPE text(tokenizer = array) GRANULARITY 4
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_text_index_hint_map SELECT (arrayMap(x -> 'k' || x, range(number % 30)), arrayMap(x -> 'v' || x, range(number % 30))) FROM numbers(100000);

SELECT count() FROM t_text_index_hint_map WHERE has(mapKeys(m), 'k18');

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint_map WHERE has(mapKeys(m), 'k18')
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM t_text_index_hint_map WHERE has(m, 'k18');

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint_map WHERE has(m, 'k18')
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM t_text_index_hint_map WHERE mapContainsKey(m, 'k18');

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint_map WHERE mapContainsKey(m, 'k18')
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM t_text_index_hint_map WHERE mapContainsValue(m, 'v18');

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint_map WHERE mapContainsValue(m, 'v18')
) WHERE explain ILIKE '%filter column%';

SELECT count() FROM t_text_index_hint_map WHERE m['k18'] = 'v18';

SELECT trim(explain) FROM
(
    EXPLAIN actions = 1 SELECT count() FROM t_text_index_hint_map WHERE m['k18'] = 'v18'
) WHERE explain ILIKE '%filter column%';

DROP TABLE IF EXISTS t_text_index_hint_map;
