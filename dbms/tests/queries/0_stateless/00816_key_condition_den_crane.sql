USE test;

drop table if exists testSegmtFault;
CREATE TABLE testSegmtFault (key1 Int32, id1 Int64, c1 Int64) ENGINE = MergeTree PARTITION BY id1 ORDER BY (key1);
insert into testSegmtFault values ( -1, 1, 0 );

SELECT count(*) FROM testSegmtFault PREWHERE id1 IN (1);

drop table testSegmtFault;
