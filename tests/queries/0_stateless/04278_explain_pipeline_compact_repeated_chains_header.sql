SELECT
    countIf(position(explain, '│') > 0) = 0,
    countIf(explain LIKE '%Header: toString(number) String%') = 1,
    countIf(explain LIKE '%Header: number UInt64%') >= 1
FROM
(
    EXPLAIN PIPELINE header = 1, compact_repeated_processor_chains = 1
    SELECT 1
    WHERE 1 IN (SELECT number FROM numbers(1))
      AND '1' IN (SELECT toString(number) FROM numbers(1))
)
FORMAT TSV;
