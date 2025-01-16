set group_by_use_nulls = true;

SELECT materialize(0), materialize(toNullable(0))
GROUP BY (dummy = 2) AND (dummy = 3) WITH ROLLUP;

SELECT materialize(0), materialize(toNullable(0))
GROUP BY (dummy = 2) AND (dummy = 3) WITH CUBE;

SELECT
  ((0 AND toLowCardinality(3)) AND (((dummy = 3) AND 3 AND ((((0 = dummy) AND (dummy = 2) AND (dummy = 3)) AND 3) AND 3) AND 0) AND (3 AND 0)) AND 0) = toUInt256(materialize(3)),
  toNullable(0)
GROUP BY (dummy = 2) AND (dummy = 3) WITH CUBE
WITH TOTALS;
