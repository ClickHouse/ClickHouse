SET allow_experimental_dynamic_type=1;

CREATE TABLE t (d Dynamic) ENGINE = Memory;

INSERT INTO t SELECT sumState(number) AS d FROM numbers(100);

SELECT finalizeAggregation(d.`AggregateFunction(sum, UInt64)`),
       sumMerge(d.`AggregateFunction(sum, UInt64)`)
FROM t GROUP BY d.`AggregateFunction(sum, UInt64)`;

