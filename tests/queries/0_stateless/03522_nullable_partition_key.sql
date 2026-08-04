CREATE TABLE t (c0 Nullable(Int)) ENGINE = MergeTree() ORDER BY (c0) PARTITION BY (c0) SETTINGS allow_nullable_key = 1;

INSERT INTO TABLE t (c0) VALUES (NULL),(1);
OPTIMIZE TABLE t FINAL;
SELECT c0, _part FROM t ORDER BY ALL;

CREATE TABLE taggr (c0 Nullable(Int)) ENGINE = AggregatingMergeTree() ORDER BY (c0) PARTITION BY (c0) SETTINGS allow_nullable_key = 1;
INSERT INTO TABLE taggr (c0) VALUES (1), (2);
INSERT INTO TABLE taggr (c0) VALUES (NULL), (1), (2);
SELECT c0, _part FROM taggr FINAL ORDER BY ALL;
