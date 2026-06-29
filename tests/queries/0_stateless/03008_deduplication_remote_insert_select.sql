DROP TABLE IF EXISTS src;

CREATE TABLE src (a UInt64, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/03008_deduplication_remote_insert_select/src', '{replica}')
    ORDER BY tuple();

INSERT INTO src SELECT number % 10 as a, number as b FROM numbers(100);

SET allow_experimental_parallel_reading_from_replicas=1;
SET max_parallel_replicas=3;
SET parallel_replicas_for_non_replicated_merge_tree=1;
SET cluster_for_parallel_replicas='parallel_replicas';

-- { echoOn }
SELECT count() FROM src;
SELECT a, sum(b), uniq(b), FROM src GROUP BY a ORDER BY a;
SELECT count() FROM remote('127.0.0.{1..2}', currentDatabase(), src);
-- { echoOff }

DROP TABLE IF EXISTS dst_null;
CREATE TABLE dst_null(a UInt64, b UInt64)
    ENGINE = Null;

DROP TABLE IF EXISTS mv_dst;
CREATE MATERIALIZED VIEW mv_dst
    ENGINE = AggregatingMergeTree()
    ORDER BY a
    AS SELECT
        a,
        sumState(b)  AS sum_b,
        uniqState(b) AS uniq_b
    FROM dst_null
    GROUP BY a;

-- { echoOn }
INSERT INTO dst_null
    SELECT a, b FROM src;

SELECT
    a,
    sumMerge(sum_b) AS sum_b,
    uniqMerge(uniq_b) AS uniq_b
FROM mv_dst
GROUP BY a
ORDER BY a;
-- { echoOff }

DROP TABLE src;
DROP TABLE mv_dst;
DROP TABLE dst_null;

