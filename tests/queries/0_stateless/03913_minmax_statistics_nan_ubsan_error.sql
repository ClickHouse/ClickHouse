SET use_statistics=1;
SET optimize_move_to_prewhere=1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (x Float64, y UInt64)
ENGINE = MergeTree
ORDER BY y
SETTINGS index_granularity=1, auto_statistics_types='minmax';

INSERT INTO test
SELECT if(number % 10 = 0, toFloat64('nan'), toFloat64(number)), number
FROM numbers(1000);

SELECT count() FROM test WHERE x < toFloat64('nan') AND y > 990;

SELECT count() FROM test WHERE x < toFloat64('nan') OR y > 990;
