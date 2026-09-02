DROP TABLE IF EXISTS test_00615;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE test_00615
(
    dt Date,
    id Int32,
    key String,
    data Nullable(Int8)
) ENGINE = MergeTree(dt, (id, key, dt), 8192);

INSERT INTO test_00615 (dt,id, key,data) VALUES ('2000-01-01', 100, 'key', 100500);

alter table test_00615 drop column data;
alter table test_00615 add column data Nullable(Float64);

INSERT INTO test_00615 (dt,id, key,data) VALUES ('2000-01-01', 100, 'key', 100500);

SELECT * FROM test_00615 ORDER BY data NULLS FIRST;
OPTIMIZE TABLE test_00615;
SELECT * FROM test_00615 ORDER BY data NULLS FIRST;

DROP TABLE test_00615;
