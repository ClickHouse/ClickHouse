 SELECT "number", CASE "number"
          WHEN 3 THEN 55
          WHEN 6 THEN 77
          WHEN 9 THEN 95
          ELSE CASE
          WHEN "number"=1 THEN 10
          WHEN "number"=10 THEN 100
          ELSE 555555
          END
          END AS "LONG_COL_0"
         FROM `system`.numbers
         LIMIT 20;
