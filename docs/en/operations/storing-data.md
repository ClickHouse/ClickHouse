---
description: 'Documentation for highlight-next-line'
sidebar_label: 'External disks for storing data'
sidebar_position: 68
slug: /operations/storing-data
title: 'External disks for storing data'
doc_type: 'guide'
---

Data processed in ClickHouse is usually stored in the local file system of the 
machine on which ClickHouse server is running. That requires large-capacity disks,
which can be expensive. To avoid storing data locally, various storage options are supported:
1. [Amazon S3](https://aws.amazon.com/s3/) object storage.
2. [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).
3. Unsupported: The Hadoop Distributed File System ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html))

<br/>

:::note 
ClickHouse also has support for external table engines, which are different from 
the external storage option described on this page, as they allow reading data 
stored in some general file format (like Parquet). On this page we are describing 
storage configuration for the ClickHouse `MergeTree` family or `Log` family tables.

1. to work with data stored on `Amazon S3` disks, use the [S3](/engines/table-engines/integrations/s3.md) table engine.
2. to work with data stored in Azure Blob Storage, use the [AzureBlobStorage](/engines/table-engines/integrations/azureBlobStorage.md) table engine.
3. to work with data in the Hadoop Distributed File System (unsupported), use the [HDFS](/engines/table-engines/integrations/hdfs.md) table engine.
:::

## Configure external storage {#configuring-external-storage}

[`MergeTree`](/engines/table-engines/mergetree-family/mergetree.md) and [`Log`](/engines/table-engines/log-family/log.md) 
family table engines can store data to `S3`, `AzureBlobStorage`, `HDFS` (unsupported) using a disk with types `s3`,
`azure_blob_storage`, `hdfs` (unsupported) respectively.

Disk configuration requires:

1. A `type` section, equal to one of `s3`, `azure_blob_storage`, `hdfs` (unsupported), `local_blob_storage`, `web`.
2. Configuration of a specific external storage type.

Starting from 24.1 clickhouse version, it is possible to use a new configuration option.
It requires specifying:

1. A `type` equal to `object_storage`
2. `object_storage_type`, equal to one of `s3`, `azure_blob_storage` (or just `azure` from `24.3`), `hdfs` (unsupported), `local_blob_storage` (or just `local` from `24.3`), `web`.

<br/>

Optionally, `metadata_type` can be specified (it is equal to `local` by default), but it can also be set to `plain`, `web` and, starting from `24.4`, `plain_rewritable`.
Usage of `plain` metadata type is described in [plain storage section](/operations/storing-data#plain-storage), `web` metadata type can be used only with `web` object storage type, `local` metadata type stores metadata files locally (each metadata files contains mapping to files in object storage and some additional meta information about them).

For example:

```xml
<s3>
    <type>s3</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

is equal to the following configuration (from version `24.1`):

```xml
<s3>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>local</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

The following configuration:

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

is equal to:

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

An example of full storage configuration will look like:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

Starting with version 24.1, it can also look like:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>local</metadata_type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

To make a specific kind of storage a default option for all `MergeTree` tables,
add the following section to the configuration file:

```xml
<clickhouse>
    <merge_tree>
        <storage_policy>s3</storage_policy>
    </merge_tree>
</clickhouse>
```

If you want to configure a specific storage policy for a specific table, 
you can define it in settings while creating the table:

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3';
```

You can also use `disk` instead of `storage_policy`. In this case it is not necessary
to have the `storage_policy` section in the configuration file, and a `disk` 
section is enough.

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = 's3';
```

## Dynamic Configuration {#dynamic-configuration}

There is also a possibility to specify storage configuration without a predefined
disk in configuration in a configuration file, but can be configured in the 
`CREATE`/`ATTACH` query settings.

The following example query builds on the above dynamic disk configuration and
shows how to use a local disk to cache data from a table stored at a URL.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=web,
    endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
  );
  -- highlight-end
```

The example below adds a cache to external storage.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
-- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
-- highlight-end
```

In the settings highlighted below notice that the disk of `type=web` is nested within
the disk of `type=cache`.

:::note
The example uses `type=web`, but any disk type can be configured as dynamic, 
including local disk. Local disks require a path argument to be inside the 
server config parameter `custom_local_disks_base_directory`, which has no 
default, so set that also when using local disk.
:::

A combination of config-based configuration and sql-defined configuration is 
also possible:

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  -- highlight-end
```

where `web` is from the server configuration file:

```xml
<storage_configuration>
    <disks>
        <web>
            <type>web</type>
            <endpoint>'https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'</endpoint>
        </web>
    </disks>
</storage_configuration>
```

### Using S3 Storage {#s3-storage}

#### Required parameters {#required-parameters-s3}

| Parameter           | Description                                                                                                                                                                            |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `endpoint`          | S3 endpoint URL in `path` or `virtual hosted` [styles](https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html). Should include the bucket and root path for data storage. |
| `access_key_id`     | S3 access key ID used for authentication.                                                                                                                                              |
| `secret_access_key` | S3 secret access key used for authentication.                                                                                                                                          |

#### Optional parameters {#optional-parameters-s3}

| Parameter                                       | Description                                                                                                                                                                                                                                   | Default Value                            |
|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|
| `region`                                        | S3 region name.                                                                                                                                                                                                                               | -                                        |
| `support_batch_delete`                          | Controls whether to check for batch delete support. Set to `false` when using Google Cloud Storage (GCS) as GCS doesn't support batch deletes.                                                                                                | `true`                                   |
| `use_environment_credentials`                   | Reads AWS credentials from environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` if they exist.                                                                                                        | `false`                                  |
| `use_insecure_imds_request`                     | If `true`, uses insecure IMDS request when obtaining credentials from Amazon EC2 metadata.                                                                                                                                                    | `false`                                  |
| `expiration_window_seconds`                     | Grace period (in seconds) for checking if expiration-based credentials have expired.                                                                                                                                                          | `120`                                    |
| `proxy`                                         | Proxy configuration for S3 endpoint. Each `uri` element inside `proxy` block should contain a proxy URL.                                                                                                                                      | -                                        |
| `connect_timeout_ms`                            | Socket connect timeout in milliseconds.                                                                                                                                                                                                       | `10000` (10 seconds)                     |
| `request_timeout_ms`                            | Request timeout in milliseconds.                                                                                                                                                                                                              | `5000` (5 seconds)                       |
| `retry_attempts`                                | Number of retry attempts for failed requests.                                                                                                                                                                                                 | `10`                                     |
| `single_read_retries`                           | Number of retry attempts for connection drops during read.                                                                                                                                                                                    | `4`                                      |
| `min_bytes_for_seek`                            | Minimum number of bytes to use seek operation instead of sequential read.                                                                                                                                                                     | `1 MB`                                   |
| `metadata_path`                                 | Local filesystem path to store S3 metadata files.                                                                                                                                                                                             | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`                             | If `true`, skips disk access checks during startup.                                                                                                                                                                                           | `false`                                  |
| `header`                                        | Adds specified HTTP header to requests. Can be specified multiple times.                                                                                                                                                                      | -                                        |
| `server_side_encryption_customer_key_base64`    | Required headers for accessing S3 objects with SSE-C encryption.                                                                                                                                                                              | -                                        |
| `server_side_encryption_kms_key_id`             | Required headers for accessing S3 objects with [SSE-KMS encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html). Empty string uses AWS managed S3 key.                                                     | -                                        |
| `server_side_encryption_kms_encryption_context` | Encryption context header for SSE-KMS (used with `server_side_encryption_kms_key_id`).                                                                                                                                                        | -                                        |
| `server_side_encryption_kms_bucket_key_enabled` | Enables S3 bucket keys for SSE-KMS (used with `server_side_encryption_kms_key_id`).                                                                                                                                                           | Matches bucket-level setting             |
| `s3_max_put_rps`                                | Maximum PUT requests per second before throttling.                                                                                                                                                                                            | `0` (unlimited)                          |
| `s3_max_put_burst`                              | Maximum concurrent PUT requests before hitting RPS limit.                                                                                                                                                                                     | Same as `s3_max_put_rps`                 |
| `s3_max_get_rps`                                | Maximum GET requests per second before throttling.                                                                                                                                                                                            | `0` (unlimited)                          |
| `s3_max_get_burst`                              | Maximum concurrent GET requests before hitting RPS limit.                                                                                                                                                                                     | Same as `s3_max_get_rps`                 |
| `read_resource`                                 | Resource name for [scheduling](/operations/workload-scheduling.md) read requests.                                                                                                                                                             | Empty string (disabled)                  |
| `write_resource`                                | Resource name for [scheduling](/operations/workload-scheduling.md) write requests.                                                                                                                                                            | Empty string (disabled)                  |
| `key_template`                                  | Defines object key generation format using [re2](https://github.com/google/re2/wiki/Syntax) syntax. Requires `storage_metadata_write_full_object_key` flag. Incompatible with `root path` in `endpoint`. Requires `key_compatibility_prefix`. | -                                        |
| `key_compatibility_prefix`                      | Required with `key_template`. Specifies the previous `root path` from `endpoint` for reading older metadata versions.                                                                                                                         | -                                        |
| `read_only`                                      | Only allowing reading from the disk.                                                                                                                                                                                                          | -                                        |
:::note
Google Cloud Storage (GCS) is also supported using the type `s3`. See [GCS backed MergeTree](/integrations/gcs).
:::

### Using Plain Storage {#plain-storage}

In `22.10` a new disk type `s3_plain` was introduced, which provides a write-once storage.
Configuration parameters for it are the same as for the `s3` disk type.
Unlike the `s3` disk type, it stores data as is. In other words, 
instead of having randomly generated blob names, it uses normal file names 
(the same way as ClickHouse stores files on local disk) and does not store any 
metadata locally. For example, it is derived from data on `s3`.

This disk type allows keeping a static version of the table, as it does not 
allow executing merges on the existing data and does not allow inserting of new
data. A use case for this disk type is to create backups on it, which can be done
via `BACKUP TABLE data TO Disk('plain_disk_name', 'backup_name')`. Afterward, 
you can do `RESTORE TABLE data AS data_restored FROM Disk('plain_disk_name', 'backup_name')` 
or use `ATTACH TABLE data (...) ENGINE = MergeTree() SETTINGS disk = 'plain_disk_name'`.

Configuration:

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

Starting from `24.1` it is possible configure any object storage disk (`s3`, `azure`, `hdfs` (unsupported), `local`) using
the `plain` metadata type.

Configuration:

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>azure</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

### Using S3 Plain Rewritable Storage {#s3-plain-rewritable-storage}

A new disk type `s3_plain_rewritable` was introduced in `24.4`.
Similar to the `s3_plain` disk type, it does not require additional storage for 
metadata files. Instead, metadata is stored in S3.
Unlike the `s3_plain` disk type, `s3_plain_rewritable` allows executing merges 
and supports `INSERT` operations.
[Mutations](/sql-reference/statements/alter#mutations) and replication of tables are not supported.

A use case for this disk type is for non-replicated `MergeTree` tables. Although 
the `s3` disk type is suitable for non-replicated `MergeTree` tables, you may opt
for the `s3_plain_rewritable` disk type if you do not require local metadata 
for the table and are willing to accept a limited set of operations. This could
be useful, for example, for system tables.

Configuration:

```xml
<s3_plain_rewritable>
    <type>s3_plain_rewritable</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

is equal to

```xml
<s3_plain_rewritable>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain_rewritable</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

Starting from `24.5` it is possible to configure any object storage disk 
(`s3`, `azure`, `local`) using the `plain_rewritable` metadata type.

### Using Azure Blob Storage {#azure-blob-storage}

`MergeTree` family table engines can store data to [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) 
using a disk with type `azure_blob_storage`.

Configuration markup:

```xml
<storage_configuration>
    ...
    <disks>
        <blob_storage_disk>
            <type>azure_blob_storage</type>
            <storage_account_url>http://account.blob.core.windows.net</storage_account_url>
            <container_name>container</container_name>
            <account_name>account</account_name>
            <account_key>pass123</account_key>
            <metadata_path>/var/lib/clickhouse/disks/blob_storage_disk/</metadata_path>
            <cache_path>/var/lib/clickhouse/disks/blob_storage_disk/cache/</cache_path>
            <skip_access_check>false</skip_access_check>
        </blob_storage_disk>
    </disks>
    ...
</storage_configuration>
```

#### Connection parameters {#azure-blob-storage-connection-parameters}

| Parameter                        | Description                                                                                                                                                                                      | Default Value       |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| `storage_account_url` (Required) | Azure Blob Storage account URL. Examples: `http://account.blob.core.windows.net` or `http://azurite1:10000/devstoreaccount1`.                                                                    | -                   |
| `container_name`                 | Target container name.                                                                                                                                                                           | `default-container` |
| `container_already_exists`       | Controls container creation behavior: <br/>- `false`: Creates a new container <br/>- `true`: Connects directly to existing container <br/>- Unset: Checks if container exists, creates if needed | -                   |

Authentication parameters (the disk will try all available methods **and** Managed Identity Credential):

| Parameter           | Description                                                     |
|---------------------|-----------------------------------------------------------------|
| `connection_string` | For authentication using a connection string.                   |
| `account_name`      | For authentication using Shared Key (used with `account_key`).  |
| `account_key`       | For authentication using Shared Key (used with `account_name`). |

#### Limit parameters {#azure-blob-storage-limit-parameters}

| Parameter                            | Description                                                                 |
|--------------------------------------|-----------------------------------------------------------------------------|
| `s3_max_single_part_upload_size`     | Maximum size of a single block upload to Blob Storage.                      |
| `min_bytes_for_seek`                 | Minimum size of a seekable region.                                          |
| `max_single_read_retries`            | Maximum number of attempts to read a chunk of data from Blob Storage.       |
| `max_single_download_retries`        | Maximum number of attempts to download a readable buffer from Blob Storage. |
| `thread_pool_size`                   | Maximum number of threads for `IDiskRemote` instantiation.                  |
| `s3_max_inflight_parts_for_one_file` | Maximum number of concurrent put requests for a single object.              |

#### Other parameters {#azure-blob-storage-other-parameters}

| Parameter                        | Description                                                                        | Default Value                            |
|----------------------------------|------------------------------------------------------------------------------------|------------------------------------------|
| `metadata_path`                  | Local filesystem path to store metadata files for Blob Storage.                    | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`              | If `true`, skips disk access checks during startup.                                | `false`                                  |
| `read_resource`                  | Resource name for [scheduling](/operations/workload-scheduling.md) read requests.  | Empty string (disabled)                  |
| `write_resource`                 | Resource name for [scheduling](/operations/workload-scheduling.md) write requests. | Empty string (disabled)                  |
| `metadata_keep_free_space_bytes` | Amount of free metadata disk space to reserve.                                     | -                                        |

Examples of working configurations can be found in integration tests directory (see e.g. [test_merge_tree_azure_blob_storage](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_merge_tree_azure_blob_storage/configs/config.d/storage_conf.xml) or [test_azure_blob_storage_zero_copy_replication](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_azure_blob_storage_zero_copy_replication/configs/config.d/storage_conf.xml)).

:::note Zero-copy replication is not ready for production
Zero-copy replication is disabled by default in ClickHouse version 22.8 and higher.  This feature is not recommended for production use.
:::

## Using HDFS storage (Unsupported) {#using-hdfs-storage-unsupported}

In this sample configuration:
- the disk is of type `hdfs` (unsupported)
- the data is hosted at `hdfs://hdfs1:9000/clickhouse/`

By the way, HDFS is unsupported and therefore there might be issues when using it. Feel free to make a pull request with the fix if any issue arises.

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <hdfs>
                <type>hdfs</type>
                <endpoint>hdfs://hdfs1:9000/clickhouse/</endpoint>
                <skip_access_check>true</skip_access_check>
            </hdfs>
            <hdd>
                <type>local</type>
                <path>/</path>
            </hdd>
        </disks>
        <policies>
            <hdfs>
                <volumes>
                    <main>
                        <disk>hdfs</disk>
                    </main>
                    <external>
                        <disk>hdd</disk>
                    </external>
                </volumes>
            </hdfs>
        </policies>
    </storage_configuration>
</clickhouse>
```

Keep in mind that HDFS may not work in corner cases.

### Using Data Encryption {#encrypted-virtual-file-system}

You can encrypt the data stored on [S3](/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3), or [HDFS](#using-hdfs-storage-unsupported) (unsupported) external disks, or on a local disk. To turn on the encryption mode, in the configuration file you must define a disk with the type `encrypted` and choose a disk on which the data will be saved. An `encrypted` disk ciphers all written files on the fly, and when you read files from an `encrypted` disk it deciphers them automatically. So you can work with an `encrypted` disk like with a normal one.

Example of disk configuration:

```xml
<disks>
  <disk1>
    <type>local</type>
    <path>/path1/</path>
  </disk1>
  <disk2>
    <type>encrypted</type>
    <disk>disk1</disk>
    <path>path2/</path>
    <key>_16_ascii_chars_</key>
  </disk2>
</disks>
```

For example, when ClickHouse writes data from some table to a file `store/all_1_1_0/data.bin` to `disk1`, then in fact this file will be written to the physical disk along the path `/path1/store/all_1_1_0/data.bin`.

When writing the same file to `disk2`, it will actually be written to the physical disk at the path `/path1/path2/store/all_1_1_0/data.bin` in encrypted mode.

### Required Parameters {#required-parameters-encrypted-disk}

| Parameter  | Type   | Description                                                                                                                                  |
|------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `type`     | String | Must be set to `encrypted` to create an encrypted disk.                                                                                      |
| `disk`     | String | Type of disk to use for underlying storage.                                                                                                  |
| `key`      | Uint64 | Key for encryption and decryption. Can be specified in hexadecimal using `key_hex`. Multiple keys can be specified using the `id` attribute. |

### Optional Parameters {#optional-parameters-encrypted-disk}

| Parameter        | Type   | Default        | Description                                                                                                                             |
|------------------|--------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `path`           | String | Root directory | Location on the disk where data will be saved.                                                                                          |
| `current_key_id` | String | -              | The key ID used for encryption. All specified keys can be used for decryption.                                                          |
| `algorithm`      | Enum   | `AES_128_CTR`  | Encryption algorithm. Options: <br/>- `AES_128_CTR` (16-byte key) <br/>- `AES_192_CTR` (24-byte key) <br/>- `AES_256_CTR` (32-byte key) |

Example of disk configuration:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <disk_s3>
                <type>s3</type>
                <endpoint>...
            </disk_s3>
            <disk_s3_encrypted>
                <type>encrypted</type>
                <disk>disk_s3</disk>
                <algorithm>AES_128_CTR</algorithm>
                <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
                <key_hex id="1">ffeeddccbbaa99887766554433221100</key_hex>
                <current_key_id>1</current_key_id>
            </disk_s3_encrypted>
        </disks>
    </storage_configuration>
</clickhouse>
```

### Using local cache {#using-local-cache}

It is possible to configure local cache over disks in storage configuration starting from version 22.3.
For versions 22.3 - 22.7 cache is supported only for `s3` disk type. For versions >= 22.8 cache is supported for any disk type: S3, Azure, Local, Encrypted, etc.
For versions >= 23.5 cache is supported only for remote disk types: S3, Azure, HDFS (unsupported).
Cache uses `LRU` cache policy.

Example of configuration for versions later or equal to 22.8:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
            </s3>
            <cache>
                <type>cache</type>
                <disk>s3</disk>
                <path>/s3_cache/</path>
                <max_size>10Gi</max_size>
            </cache>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>cache</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

Example of configuration for versions earlier than 22.8:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
                <data_cache_enabled>1</data_cache_enabled>
                <data_cache_max_size>10737418240</data_cache_max_size>
            </s3>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

File Cache **disk configuration settings**:

These settings should be defined in the disk configuration section.

| Parameter                             | Type    | Default    | Description                                                                                                                                                                                  |
|---------------------------------------|---------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `path`                                | String  | -          | **Required**. Path to the directory where cache will be stored.                                                                                                                              |
| `max_size`                            | Size    | -          | **Required**. Maximum cache size in bytes or readable format (e.g., `10Gi`). Files are evicted using LRU policy when the limit is reached. Supports `ki`, `Mi`, `Gi` formats (since v22.10). |
| `cache_on_write_operations`           | Boolean | `false`    | Enables write-through cache for `INSERT` queries and background merges. Can be overridden per query with `enable_filesystem_cache_on_write_operations`.                                      |
| `enable_filesystem_query_cache_limit` | Boolean | `false`    | Enables per-query cache size limits based on `max_query_cache_size`.                                                                                                                         |
| `enable_cache_hits_threshold`         | Boolean | `false`    | When enabled, data is cached only after being read multiple times.                                                                                                                           |
| `cache_hits_threshold`                | Integer | `0`        | Number of reads required before data is cached (requires `enable_cache_hits_threshold`).                                                                                                     |
| `enable_bypass_cache_with_threshold`  | Boolean | `false`    | Skips cache for large read ranges.                                                                                                                                                           |
| `bypass_cache_threshold`              | Size    | `256Mi`    | Read range size that triggers cache bypass (requires `enable_bypass_cache_with_threshold`).                                                                                                  |
| `max_file_segment_size`               | Size    | `8Mi`      | Maximum size of a single cache file in bytes or readable format.                                                                                                                             |
| `max_elements`                        | Integer | `10000000` | Maximum number of cache files.                                                                                                                                                               |
| `load_metadata_threads`               | Integer | `16`       | Number of threads for loading cache metadata at startup.                                                                                                                                     |

> **Note**: Size values support units like `ki`, `Mi`, `Gi`, etc. (e.g., `10Gi`).

## File Cache Query/Profile Settings {#file-cache-query-profile-settings}

| Setting                                                       | Type    | Default                 | Description                                                                                                                                                    |
|---------------------------------------------------------------|---------|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `enable_filesystem_cache`                                     | Boolean | `true`                  | Enables/disables cache usage per query, even when using a `cache` disk type.                                                                                   |
| `read_from_filesystem_cache_if_exists_otherwise_bypass_cache` | Boolean | `false`                 | When enabled, uses cache only if data exists; new data won't be cached.                                                                                        |
| `enable_filesystem_cache_on_write_operations`                 | Boolean | `false` (Cloud: `true`) | Enables write-through cache. Requires `cache_on_write_operations` in cache config.                                                                             |
| `enable_filesystem_cache_log`                                 | Boolean | `false`                 | Enables detailed cache usage logging to `system.filesystem_cache_log`.                                                                                         |
| `filesystem_cache_allow_background_download`                  | Boolean | `true`                  | Allows partially downloaded segments to be finished in the background. Disable to keep downloads in the foreground for the current query/session.             |
| `max_query_cache_size`                                        | Size    | `false`                 | Maximum cache size per query. Requires `enable_filesystem_query_cache_limit` in cache config.                                                                  |
| `filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit` | Boolean | `true`          | Controls behavior when `max_query_cache_size` is reached: <br/>- `true`: Stops downloading new data <br/>- `false`: Evicts old data to make space for new data |

:::warning
Cache configuration settings and cache query settings correspond to the latest ClickHouse version, 
for earlier versions something might not be supported.
:::

#### Cache system tables {#cache-system-tables-file-cache}

| Table Name                    | Description                                         | Requirements                                  |
|-------------------------------|-----------------------------------------------------|-----------------------------------------------|
| `system.filesystem_cache`     | Displays the current state of the filesystem cache. | None                                          |
| `system.filesystem_cache_log` | Provides detailed cache usage statistics per query. | Requires `enable_filesystem_cache_log = true` |

#### Cache commands {#cache-commands-file-cache}

##### `SYSTEM DROP FILESYSTEM CACHE (<cache_name>) (ON CLUSTER)` -- `ON CLUSTER` {#system-drop-filesystem-cache-on-cluster}

This command is only supported when no `<cache_name>` is provided

##### `SHOW FILESYSTEM CACHES` {#show-filesystem-caches}

Show a list of filesystem caches which were configured on the server. 
(For versions less than or equal to `22.8` the command is named `SHOW CACHES`)

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

##### `DESCRIBE FILESYSTEM CACHE '<cache_name>'` {#describe-filesystem-cache}

Show cache configuration and some general statistics for a specific cache. 
Cache name can be taken from `SHOW FILESYSTEM CACHES` command. (For versions less
than or equal to `22.8` the command is named `DESCRIBE CACHE`)

```sql title="Query"
DESCRIBE FILESYSTEM CACHE 's3_cache'
```

```text title="Response"
┌────max_size─┬─max_elements─┬─max_file_segment_size─┬─boundary_alignment─┬─cache_on_write_operations─┬─cache_hits_threshold─┬─current_size─┬─current_elements─┬─path───────┬─background_download_threads─┬─enable_bypass_cache_with_threshold─┐
│ 10000000000 │      1048576 │             104857600 │            4194304 │                         1 │                    0 │         3276 │               54 │ /s3_cache/ │                           2 │                                  0 │
└─────────────┴──────────────┴───────────────────────┴────────────────────┴───────────────────────────┴──────────────────────┴──────────────┴──────────────────┴────────────┴─────────────────────────────┴────────────────────────────────────┘
```

| Cache current metrics     | Cache asynchronous metrics | Cache profile events                                                                      |
|---------------------------|----------------------------|-------------------------------------------------------------------------------------------|
| `FilesystemCacheSize`     | `FilesystemCacheBytes`     | `CachedReadBufferReadFromSourceBytes`, `CachedReadBufferReadFromCacheBytes`               |
| `FilesystemCacheElements` | `FilesystemCacheFiles`     | `CachedReadBufferReadFromSourceMicroseconds`, `CachedReadBufferReadFromCacheMicroseconds` |
|                           |                            | `CachedReadBufferCacheWriteBytes`, `CachedReadBufferCacheWriteMicroseconds`               |
|                           |                            | `CachedWriteBufferCacheWriteBytes`, `CachedWriteBufferCacheWriteMicroseconds`             |

### Using static Web storage (read-only) {#web-storage}

This is a read-only disk. Its data is only read and never modified. A new table 
is loaded to this disk via `ATTACH TABLE` query (see example below). Local disk 
is not actually used, each `SELECT` query will result in a `http` request to 
fetch required data. All modification of the table data will result in an 
exception, i.e. the following types of queries are not allowed: [`CREATE TABLE`](/sql-reference/statements/create/table.md),
[`ALTER TABLE`](/sql-reference/statements/alter/index.md), [`RENAME TABLE`](/sql-reference/statements/rename#rename-table),
[`DETACH TABLE`](/sql-reference/statements/detach.md) and [`TRUNCATE TABLE`](/sql-reference/statements/truncate.md).
Web storage can be used for read-only purposes. An example use is for hosting 
sample data, or for migrating data. There is a tool `clickhouse-static-files-uploader`, 
which prepares a data directory for a given table (`SELECT data_paths FROM system.tables WHERE name = 'table_name'`). 
For each table you need, you get a directory of files. These files can be uploaded 
to, for example, a web server with static files. After this preparation, 
you can load this table into any ClickHouse server via `DiskWeb`.

In this sample configuration:
- the disk is of type `web`
- the data is hosted at `http://nginx:80/test1/`
- a cache on local storage is used

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>http://nginx:80/test1/</endpoint>
            </web>
            <cached_web>
                <type>cache</type>
                <disk>web</disk>
                <path>cached_web_cache/</path>
                <max_size>100000000</max_size>
            </cached_web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
            <cached_web>
                <volumes>
                    <main>
                        <disk>cached_web</disk>
                    </main>
                </volumes>
            </cached_web>
        </policies>
    </storage_configuration>
</clickhouse>
```

:::tip
Storage can also be configured temporarily within a query, if a web dataset is 
not expected to be used routinely, see [dynamic configuration](#dynamic-configuration) and skip 
editing the configuration file.

A [demo dataset](https://github.com/ClickHouse/web-tables-demo) is hosted in GitHub.  To prepare your own tables for web 
storage see the tool [clickhouse-static-files-uploader](/operations/utilities/static-files-disk-uploader)
:::

In this `ATTACH TABLE` query the `UUID` provided matches the directory name of the data, and the endpoint is the URL for the raw GitHub content.

```sql
-- highlight-next-line
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      );
  -- highlight-end
```

A ready test case. You need to add this configuration to config:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>https://clickhouse-datasets.s3.yandex.net/disk-with-static-files-tests/test-hits/</endpoint>
            </web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
        </policies>
    </storage_configuration>
</clickhouse>
```

And then execute this query:

```sql
ATTACH TABLE test_hits UUID '1ae36516-d62d-4218-9ae3-6516d62da218'
(
    WatchID UInt64,
    JavaEnable UInt8,
    Title String,
    GoodEvent Int16,
    EventTime DateTime,
    EventDate Date,
    CounterID UInt32,
    ClientIP UInt32,
    ClientIP6 FixedString(16),
    RegionID UInt32,
    UserID UInt64,
    CounterClass Int8,
    OS UInt8,
    UserAgent UInt8,
    URL String,
    Referer String,
    URLDomain String,
    RefererDomain String,
    Refresh UInt8,
    IsRobot UInt8,
    RefererCategories Array(UInt16),
    URLCategories Array(UInt16),
    URLRegions Array(UInt32),
    RefererRegions Array(UInt32),
    ResolutionWidth UInt16,
    ResolutionHeight UInt16,
    ResolutionDepth UInt8,
    FlashMajor UInt8,
    FlashMinor UInt8,
    FlashMinor2 String,
    NetMajor UInt8,
    NetMinor UInt8,
    UserAgentMajor UInt16,
    UserAgentMinor FixedString(2),
    CookieEnable UInt8,
    JavascriptEnable UInt8,
    IsMobile UInt8,
    MobilePhone UInt8,
    MobilePhoneModel String,
    Params String,
    IPNetworkID UInt32,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    SearchPhrase String,
    AdvEngineID UInt8,
    IsArtifical UInt8,
    WindowClientWidth UInt16,
    WindowClientHeight UInt16,
    ClientTimeZone Int16,
    ClientEventTime DateTime,
    SilverlightVersion1 UInt8,
    SilverlightVersion2 UInt8,
    SilverlightVersion3 UInt32,
    SilverlightVersion4 UInt16,
    PageCharset String,
    CodeVersion UInt32,
    IsLink UInt8,
    IsDownload UInt8,
    IsNotBounce UInt8,
    FUniqID UInt64,
    HID UInt32,
    IsOldCounter UInt8,
    IsEvent UInt8,
    IsParameter UInt8,
    DontCountHits UInt8,
    WithHash UInt8,
    HitColor FixedString(1),
    UTCEventTime DateTime,
    Age UInt8,
    Sex UInt8,
    Income UInt8,
    Interests UInt16,
    Robotness UInt8,
    GeneralInterests Array(UInt16),
    RemoteIP UInt32,
    RemoteIP6 FixedString(16),
    WindowName Int32,
    OpenerName Int32,
    HistoryLength Int16,
    BrowserLanguage FixedString(2),
    BrowserCountry FixedString(2),
    SocialNetwork String,
    SocialAction String,
    HTTPError UInt16,
    SendTiming Int32,
    DNSTiming Int32,
    ConnectTiming Int32,
    ResponseStartTiming Int32,
    ResponseEndTiming Int32,
    FetchTiming Int32,
    RedirectTiming Int32,
    DOMInteractiveTiming Int32,
    DOMContentLoadedTiming Int32,
    DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,
    LoadEventEndTiming Int32,
    NSToDOMContentLoadedTiming Int32,
    FirstPaintTiming Int32,
    RedirectCount Int8,
    SocialSourceNetworkID UInt8,
    SocialSourcePage String,
    ParamPrice Int64,
    ParamOrderID String,
    ParamCurrency FixedString(3),
    ParamCurrencyID UInt16,
    GoalsReached Array(UInt32),
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID UInt8,
    RefererHash UInt64,
    URLHash UInt64,
    CLID UInt32,
    YCLID UInt64,
    ShareService String,
    ShareURL String,
    ShareTitle String,
    ParsedParams Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    IslandID FixedString(16),
    RequestNum UInt32,
    RequestTry UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS storage_policy='web';
```

#### Required parameters {#static-web-storage-required-parameters}

| Parameter  | Description                                                                                                       |
|------------|-------------------------------------------------------------------------------------------------------------------|
| `type`     | `web`. Otherwise the disk is not created.                                                                         |
| `endpoint` | The endpoint URL in `path` format. Endpoint URL must contain a root path to store data, where they were uploaded. |

#### Optional parameters {#optional-parameters-web}

| Parameter                           | Description                                                                  | Default Value   |
|-------------------------------------|------------------------------------------------------------------------------|-----------------|
| `min_bytes_for_seek`                | The minimal number of bytes to use seek operation instead of sequential read | `1` MB          |
| `remote_fs_read_backoff_threashold` | The maximum wait time when trying to read data for remote disk               | `10000` seconds |
| `remote_fs_read_backoff_max_tries`  | The maximum number of attempts to read with backoff                          | `5`             |

If a query fails with an exception `DB:Exception Unreachable URL`, then you can try to adjust the settings: [http_connection_timeout](/operations/settings/settings.md/#http_connection_timeout), [http_receive_timeout](/operations/settings/settings.md/#http_receive_timeout), [keep_alive_timeout](/operations/server-configuration-parameters/settings#keep_alive_timeout).

To get files for upload run:
`clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>` (`--metadata-path` can be found in query `SELECT data_paths FROM system.tables WHERE name = 'table_name'`).

When loading files by `endpoint`, they must be loaded into `<endpoint>/store/` path, but config must contain only `endpoint`.

If URL is not reachable on disk load when the server is starting up tables, then all errors are caught. If in this case there were errors, tables can be reloaded (become visible) via `DETACH TABLE table_name` -> `ATTACH TABLE table_name`. If metadata was successfully loaded at server startup, then tables are available straight away.

Use [http_max_single_read_retries](/operations/storing-data#web-storage) setting to limit the maximum number of retries during a single HTTP read.

### Zero-copy Replication (not ready for production) {#zero-copy}

Zero-copy replication is possible, but not recommended, with  `S3` and `HDFS` (unsupported) disks. Zero-copy replication means that if the data is stored remotely on several machines and needs to be synchronized, then only the metadata is replicated (paths to the data parts), but not the data itself.

:::note Zero-copy replication is not ready for production
Zero-copy replication is disabled by default in ClickHouse version 22.8 and higher.  This feature is not recommended for production use.
:::
