SELECT [toNullable(1)] AS x, x[toNullable(1)] AS y;
SELECT materialize([toNullable(1)]) AS x, x[toNullable(1)] AS y;
SELECT [toNullable(1)] AS x, x[materialize(toNullable(1))] AS y;
SELECT materialize([toNullable(1)]) AS x, x[materialize(toNullable(1))] AS y;
