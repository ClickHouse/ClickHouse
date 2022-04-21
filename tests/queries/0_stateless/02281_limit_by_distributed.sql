SELECT k
FROM (
     SELECT k, abs(v) AS _v
     FROM remote('127.{1,2}', view(select materialize('foo') as k, -1 as v))
     ORDER BY _v ASC
     LIMIT 1 BY k
)
GROUP BY k;
