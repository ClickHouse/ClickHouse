SELECT hex(CAST(x, 'AggregateFunction(sum, Decimal(50, 10))')) FROM (SELECT arrayReduce('sumState', [toDecimal256('0.0000010.000001', 10)]) AS x) GROUP BY x;
