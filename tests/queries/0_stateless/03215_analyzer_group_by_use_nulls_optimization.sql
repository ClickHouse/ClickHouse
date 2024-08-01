set group_by_use_nulls = true;

SELECT materialize(0), materialize(toNullable(0))
GROUP BY (dummy = 2) AND (dummy = 3) WITH ROLLUP;

SELECT materialize(0), materialize(toNullable(0))
GROUP BY (dummy = 2) AND (dummy = 3) WITH CUBE;
