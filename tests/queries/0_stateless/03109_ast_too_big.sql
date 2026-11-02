-- https://github.com/ClickHouse/ClickHouse/issues/32139

SET enable_analyzer=1;

WITH
  data AS (
    SELECT
      rand64() AS val1,
      rand64() AS val2,
      rand64() AS val3,
      rand64() AS val4,
      rand64() AS val5,
      rand64() AS val6,
      rand64() AS val7,
      rand64() AS val8,
      rand64() AS val9,
      rand64() AS val10,
      rand64() AS val11,
      rand64() AS val12,
      rand64() AS val13,
      rand64() AS val14
    FROM numbers(10)
  ),
  (SELECT avg(val1) FROM data) AS value1,
  (SELECT avg(val2) FROM data) AS value2,
  (SELECT avg(val3) FROM data) AS value3,
  (SELECT avg(val4) FROM data) AS value4,
  (SELECT avg(val5) FROM data) AS value5,
  (SELECT avg(val6) FROM data) AS value6,
  (SELECT avg(val7) FROM data) AS value7,
  (SELECT avg(val8) FROM data) AS value8,
  (SELECT avg(val9) FROM data) AS value9,
  (SELECT avg(val10) FROM data) AS value10,
  (SELECT avg(val11) FROM data) AS value11,
  (SELECT avg(val12) FROM data) AS value12,
  (SELECT avg(val13) FROM data) AS value13,
  (SELECT avg(val14) FROM data) AS value14
SELECT
  value1 AS v1,
  value2 AS v2,
  value3 AS v3,
  value4 AS v4,
  value5 AS v5,
  value6 AS v6,
  value7 AS v7,
  value8 AS v8,
  value9 AS v9,
  value10 AS v10,
  value11 AS v11,
  value12 AS v12,
  value13 AS v13,
  value14 AS v14
FORMAT Null;
