-- Results of `FilterTransform`, which evaluates its expression positionally against the fixed input header
-- and takes the column types from the header computed once: the filter column is kept, one input feeds
-- several positions, and the column filtered first to count the rows is chosen by Nullable/LowCardinality types.

SET allow_suspicious_low_cardinality_types = 1;
SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS t_filter_positional;
CREATE TABLE t_filter_positional (a UInt64, b String, n Nullable(UInt8), lc LowCardinality(Nullable(UInt8)))
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_filter_positional SELECT number, toString(number), if(number % 3 = 0, NULL, number % 5), if(number % 4 = 0, NULL, number % 7) FROM numbers(100);

SELECT 'filter column is kept';
SELECT a, b, a % 7 = 0 AS c FROM t_filter_positional WHERE c ORDER BY a LIMIT 3;

SELECT 'input column is used twice';
SELECT a, a + a FROM t_filter_positional WHERE a + a > 190 AND a > 90 ORDER BY a;

SELECT 'only Nullable and LowCardinality numeric columns pass the filter';
SELECT n, lc FROM t_filter_positional WHERE b LIKE '%7' ORDER BY n, lc;

DROP TABLE t_filter_positional;
