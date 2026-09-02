select arrayReduce('median', [toDecimal32OrNull('1', 2)]);
select arrayReduce('median', [toDecimal64OrNull('1', 2)]);
select arrayReduce('median', [toDecimal128OrZero('1', 2)]);
select arrayReduce('sum', [toDecimal128OrNull('1', 2)]);

select arrayReduce('median', [toDecimal128OrNull('1', 2)]);
select arrayReduce('quantile(0.2)', [toDecimal128OrNull('1', 2)]);
select arrayReduce('medianExact', [toDecimal128OrNull('1', 2)]);
