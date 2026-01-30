| Parameter | Description |
|-----------|-------------|
| `endpoint` | AzureBlobStorage endpoint URL with container & prefix. Optionally can contain account_name if the authentication method used needs it. (`http://azurite1:{port}/[account_name]{container_name}/{data_prefix}`) or these parameters can be provided separately using storage_account_url, account_name & container. For specifying prefix, endpoint should be used. |
| `endpoint_contains_account_name` | This flag is used to specify if endpoint contains account_name as it is only needed for certain authentication methods. Default: `true` |
| `connection_string\|storage_account_url` | connection_string includes account name & key ([Create connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) or you could also provide the storage account url here and account name & account key as separate parameters (see parameters account_name & account_key) |
| `container_name` | Container name |
| `blobpath` | File path. Supports following wildcards in readonly mode: `*`, `**`, `?`, `{abc,def}` and `{N..M}` where `N`, `M` — numbers, `'abc'`, `'def'` — strings. |
| `account_name` | If storage_account_url is used, then account name can be specified here |
| `account_key` | If storage_account_url is used, then account key can be specified here |
| `format` | The [format](/interfaces/formats.md) of the file. |
| `compression` | Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. By default, it will autodetect compression by file extension (same as setting to `auto`). |
| `partition_strategy` | Options: `WILDCARD` or `HIVE`. `WILDCARD` requires a `{_partition_id}` in the path, which is replaced with the partition key. `HIVE` does not allow wildcards, assumes the path is the table root, and generates Hive-style partitioned directories with Snowflake IDs as filenames and the file format as the extension. Defaults to `WILDCARD` |
| `partition_columns_in_data_file` | Only used with `HIVE` partition strategy. Tells ClickHouse whether to expect partition columns to be written in the data file. Defaults to `false`. |
| `extra_credentials` | Use `client_id` and `tenant_id` for authentication. If extra_credentials are provided, they are given priority over `account_name` and `account_key`. |