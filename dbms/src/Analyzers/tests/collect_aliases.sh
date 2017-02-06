#!/bin/sh

echo "SELECT 1 AS x, x + 2 AS y FROM d.t AS t INNER JOIN (SELECT 1 AS xxx) AS u USING (abc, def AS ghi) ARRAY JOIN arr AS a, (arr1 AS z) + 1 AS b" | ./collect_aliases
