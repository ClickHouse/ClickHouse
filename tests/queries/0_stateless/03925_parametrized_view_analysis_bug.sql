CREATE TABLE t1
(
    id Int64,
    val Nullable(Int64)
)
ENGINE = MergeTree order by tuple();

INSERT into t1 VALUES (1, 42);

create view test_view_1 as
SELECT
    CAST((select val from t1 where id = {kk2: Int64}), 'UInt64') AS ui;

select * from test_view_1(kk2 = 1) settings enable_analyzer=0;

select * from test_view_1(kk2 = 1) settings enable_analyzer=1;
