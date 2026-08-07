-- Tags: stateful
DROP TABLE IF EXISTS merge_hits;
CREATE TABLE IF NOT EXISTS merge_hits AS test.hits ENGINE = Merge(test, '^hits$');
SELECT count() FROM merge_hits WHERE AdvEngineID = 2;
SELECT count() FROM merge_hits PREWHERE AdvEngineID = 2;
DROP TABLE merge_hits;
