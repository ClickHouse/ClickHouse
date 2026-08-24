DROP TABLE IF EXISTS most_ordinary_mt;

CREATE TABLE most_ordinary_mt
(
   Key UInt64
)
ENGINE = MergeTree()
ORDER BY tuple();

ALTER TABLE most_ordinary_mt RESET SETTING ttl; --{serverError BAD_ARGUMENTS}
ALTER TABLE most_ordinary_mt RESET SETTING allow_remote_fs_zero_copy_replication, xxx;  --{serverError BAD_ARGUMENTS}

DROP TABLE IF EXISTS most_ordinary_mt;
