-- In some versions vertical merges after DROP COLUMN was broken in some cases

drop table if exists data;

create table data (
    key Int,
    `legacy_features_Map.id` Array(UInt8),
    `legacy_features_Map.count` Array(UInt32),
) engine=MergeTree()
order by key
settings
    min_bytes_for_wide_part=0,
    min_rows_for_wide_part=0,
    vertical_merge_algorithm_min_rows_to_activate=0,
    vertical_merge_algorithm_min_columns_to_activate=0;

insert into data (key) values (1);
insert into data (key) values (2);

alter table data add column `features_legacy_Map.id` Array(UInt8), add column `features_legacy_Map.count` Array(UInt32);

alter table data drop column legacy_features_Map settings mutations_sync=2;

optimize table data final;
DROP TABLE data;
