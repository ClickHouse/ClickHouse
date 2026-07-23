-- Tags: shard, no-old-analyzer
-- Regression test for issue #111547: WITH FILL ... INTERPOLATE over a network merge of two
-- empty sorted streams used to abort in FillingTransform::saveLastRow. Must return the suffix
-- rows. (QUALIFY requires the analyzer, hence no-old-analyzer.)

SELECT n, inter
FROM remote('127.0.0.1,127.0.0.2', view(
    SELECT number AS inter, toFloat32(number / 10) AS n
    FROM numbers(10) WHERE 1 = (number % 3) GROUP BY ALL
    QUALIFY equals(inter, toNullable(2)) LIMIT 667))
ORDER BY n ASC NULLS LAST WITH FILL FROM 0 TO 11.51 STEP 2. INTERPOLATE (`inter` AS 1023)
SETTINGS prefer_localhost_replica = 0;
