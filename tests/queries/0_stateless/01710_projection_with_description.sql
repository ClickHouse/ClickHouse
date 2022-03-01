-- Tags: no-s3-storage

drop table if exists t;

CREATE TABLE t
(
    `i` int,
    `j` int,
    `k` int,
    `v` String,
    PROJECTION p
    (
        i + j AS `a` CODEC(T64, LZ4),
        j AS `b`,
        INDEX i_k k TYPE minmax GRANULARITY 1,
        COMMENT 'test',
        SETTINGS index_granularity = 1
    )
    (
        SELECT i + j, k ORDER BY j
    )
)
ENGINE = MergeTree ORDER BY i;

insert into t values (1, 2, 3, 'test'), (4, 5, 6, 'test2'), (7, 8, 9, 'test3');

show create t;

select i + j from t where k = 3 settings force_optimize_projection = 1, max_rows_to_read = 1;

drop table t;
