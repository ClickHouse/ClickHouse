DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS wv;
CREATE TABLE mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW wv ON CLUSTER test_shard_localhost TO input_deduplicated INNER ENGINE Memory WATERMARK=INTERVAL '1' SECOND AS SELECT count(a), hopStart(wid) AS w_start, hopEnd(wid) AS w_end FROM mt GROUP BY hop(timestamp, INTERVAL '3' SECOND, INTERVAL '5' SECOND) AS wid; -- { serverError 344 }
