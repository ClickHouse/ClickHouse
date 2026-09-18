DROP TABLE IF EXISTS t_index_subcolumn;

CREATE TABLE t_index_subcolumn
(
    json JSON,
    INDEX idx1(json.a.b::String) TYPE text(tokenizer=splitByNonAlpha, preprocessor=lower(json.a.b::String))
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1024, index_granularity_bytes = '10M';

INSERT INTO t_index_subcolumn SELECT toJSONString(map('a.b', 'Value_' || number)) FROM numbers(10000);

SELECT * FROM t_index_subcolumn WHERE json.a.b::String = 'Value_1234';
SELECT * FROM t_index_subcolumn WHERE json.a.b::String = 'value_1234';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT * FROM t_index_subcolumn WHERE json.a.b::String = 'Value_1234'
) WHERE explain LIKE '%Granules%';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1 SELECT * FROM t_index_subcolumn WHERE json.a.b::String = 'value_1234'
) WHERE explain LIKE '%Granules%';

DROP TABLE t_index_subcolumn;
