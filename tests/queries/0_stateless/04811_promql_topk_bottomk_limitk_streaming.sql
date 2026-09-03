-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Regression test: PromQL topk over many series must run in bounded memory, streaming through the
-- timeSeriesTopKMasks aggregate function. With N = 3000 series and T = 200 steps, a plan
-- materializing per-step N x N matrices would need T * N^2 * 8 bytes = 14.4 GB, far over the 2 GB limit
-- used here, while the streaming selection state is about T * k * 16 bytes = 32 KB.

DROP TABLE IF EXISTS prometheus;

SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, time_series)
SELECT
    'big',
    map('inst', toString(number)),
    arrayMap(step -> (toDateTime64(100 + step * 10, 3), toFloat64(number + 1)), range(200))
FROM numbers(3000);

SELECT count(), sum(length(time_series))
FROM prometheusQueryRange('prometheus', 'topk(10, last_over_time(big[10]))', 100, 2090, 10)
SETTINGS max_memory_usage = 2000000000;

DROP TABLE prometheus;
