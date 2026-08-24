-- Tags: shard

select * from remote('127.{1,2}', view(select 'foo.com' key), cityHash64(key)) where key = cutToFirstSignificantSubdomainCustom('foo.com', 'public_suffix_list') settings optimize_skip_unused_shards=1, force_optimize_skip_unused_shards=1;
select * from remote('127.{1,2}', view(select 'foo.com' key), cityHash64(key)) where key = cutToFirstSignificantSubdomainCustom('bar.com', 'public_suffix_list') settings optimize_skip_unused_shards=1, force_optimize_skip_unused_shards=1;
