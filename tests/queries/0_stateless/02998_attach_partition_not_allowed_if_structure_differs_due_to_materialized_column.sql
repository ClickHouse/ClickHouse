CREATE TABLE attach_partition_t7 (
                                     a UInt32,
                                     b UInt32
)
    ENGINE = MergeTree
PARTITION BY a ORDER BY a;

ALTER TABLE attach_partition_t7
    ADD COLUMN mat_column
        UInt32 MATERIALIZED a+b;

insert into attach_partition_t7 values (1, 2);

CREATE TABLE attach_partition_t8 (
                                     a UInt32,
                                     b UInt32
)
    ENGINE = MergeTree
PARTITION BY a ORDER BY a;

ALTER TABLE attach_partition_t8 ATTACH PARTITION ID '1' FROM attach_partition_t7; -- {serverError INCOMPATIBLE_COLUMNS};
