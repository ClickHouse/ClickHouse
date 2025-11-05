-- { echoOn }

drop table if exists mt1;

create table mt1 (time DateTime, projection proj (select min(time))) engine MergeTree order by () TTL time + interval 1 second settings remove_empty_parts=0, merge_with_ttl_timeout=0, deduplicate_merge_projection_mode='ignore';

system stop merges mt1;

insert into mt1 select number from numbers(4) settings max_block_size=1, min_insert_block_size_bytes=1;

system start merges mt1;

optimize table mt1 final;

optimize table mt1 final;
