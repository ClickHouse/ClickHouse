DROP TABLE IF EXISTS replace;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE replace ( EventDate Date,  Id UInt64,  Data String,  Version UInt32) ENGINE = ReplacingMergeTree(EventDate, Id, 8192, Version);
INSERT INTO replace VALUES ('2016-06-02', 1, 'version 1', 1);
INSERT INTO replace VALUES ('2016-06-02', 2, 'version 1', 1);
INSERT INTO replace VALUES ('2016-06-02', 1, 'version 0', 0);

SELECT * FROM replace ORDER BY Id, Version;
SELECT * FROM replace FINAL ORDER BY Id, Version;
SELECT * FROM replace FINAL WHERE Version = 0 ORDER BY Id, Version;

DROP TABLE replace;
