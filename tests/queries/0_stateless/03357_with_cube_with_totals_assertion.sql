SET enable_analyzer=1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_rows_to_group_by = 65535;
SET max_threads = 1;
SET max_block_size = 65536;
SET group_by_overflow_mode = 'any';
SET totals_mode = 'after_having_auto';
SET totals_auto_threshold = 0.5;

CREATE TABLE combinator_argMin_table_r1__fuzz_791
(
    `id` LowCardinality(Int32),
    `value`    String,
    `agg_time` UInt64
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO combinator_argMin_table_r1__fuzz_791
SELECT number % 10 AS id,
       number      AS value,
       '01-01-2024 00:00:00' + toIntervalDay(number)
FROM numbers(100);

-- No idea what's the right answer here, but definitely not a logical error!
SELECT maxArgMax(agg_time, value)
FROM combinator_argMin_table_r1__fuzz_791
GROUP BY id WITH CUBE WITH TOTALS FORMAT Null;
