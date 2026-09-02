ATTACH VIEW _ UUID 'baa1dcb1-92fa-4ff0-b880-6e611f2b95f8'
(
    `id` UInt64
)
AS SELECT *
FROM cluster(test_shard_localhost, merge('v114098c', '^zzz_cluster_merge_src$'))
