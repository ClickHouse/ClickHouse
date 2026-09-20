-- https://github.com/ClickHouse/ClickHouse/issues/105938
-- Incorrect results when OR combines conditions on columns with different skip index types (set + ngrambf_v1)

DROP TABLE IF EXISTS t_skip_index_set_ngrambf_or;

CREATE TABLE t_skip_index_set_ngrambf_or (
    s String,
    n LowCardinality(Nullable(String)),
    INDEX idx_n n TYPE set(100) GRANULARITY 2,
    INDEX idx_s s TYPE ngrambf_v1(4, 1024, 2, 0) GRANULARITY 3
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_skip_index_set_ngrambf_or SELECT 'has-republisher', NULL FROM numbers(100000);
INSERT INTO t_skip_index_set_ngrambf_or SELECT 'no-match', NULL FROM numbers(100);

SELECT count() FROM t_skip_index_set_ngrambf_or WHERE s LIKE '%republisher%';
SELECT count() FROM t_skip_index_set_ngrambf_or WHERE (n = 'republisher' OR s LIKE '%republisher%');
SELECT count() FROM t_skip_index_set_ngrambf_or WHERE (assumeNotNull(n) = 'republisher' OR s LIKE '%republisher%');

DROP TABLE t_skip_index_set_ngrambf_or;
