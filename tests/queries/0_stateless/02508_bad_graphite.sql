DROP TABLE IF EXISTS test_graphite;
create table test_graphite (key UInt32, Path String, Time DateTime('UTC'), Value UInt8, Version UInt32, col UInt64)
    engine = GraphiteMergeTree('graphite_rollup') order by key;

INSERT INTO test_graphite (key) VALUES (0); -- { serverError BAD_ARGUMENTS }
DROP TABLE test_graphite;
