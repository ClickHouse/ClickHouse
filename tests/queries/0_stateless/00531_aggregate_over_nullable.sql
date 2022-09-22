DROP TABLE IF EXISTS agg_over_nullable;
CREATE TABLE agg_over_nullable (
	partition Date,
	timestamp DateTime,
	user_id Nullable(UInt32),
	description Nullable(String)
) ENGINE = MergeTree(partition, timestamp, 8192);

INSERT INTO agg_over_nullable(partition, timestamp, user_id, description) VALUES(now(), now(), 1, 'ss');
INSERT INTO agg_over_nullable(partition, timestamp, user_id, description) VALUES(now(), now(), 1, NULL);
INSERT INTO agg_over_nullable(partition, timestamp, user_id, description) VALUES(now(), now(), 1, 'aa');

SELECT arraySort(groupUniqArray(description)) FROM agg_over_nullable;
SELECT arraySort(topK(3)(description)) FROM agg_over_nullable;

DROP TABLE agg_over_nullable;
