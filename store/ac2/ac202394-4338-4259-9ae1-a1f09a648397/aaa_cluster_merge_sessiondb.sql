ATTACH VIEW _ UUID '0a2eaccc-14e1-4dde-919e-a191ad853dae'
(
    `id` UInt64
)
AS SELECT *
FROM cluster(test_shard_localhost, merge('vfold', '^zzz_cluster_merge_src$'))
