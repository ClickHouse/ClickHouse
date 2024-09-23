---
slug: /en/operations/storing-data
sidebar_position: 68
sidebar_label: "External Disks for Storing Data"
title: "External Disks for Storing Data"
---

Data, processed in ClickHouse, is usually stored in the local file system — on the same machine with the ClickHouse server. That requires large-capacity disks, which can be expensive enough. To avoid that you can store the data remotely. Various storages are supported:
1. [Amazon S3](https://aws.amazon.com/s3/) object storage.
2. [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).
3. Unsupported: The Hadoop Distributed File System ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html))

:::note ClickHouse also has support for external table engines, which are different from external storage option described on this page as they allow to read data stored in some general file format (like Parquet), while on this page we are describing storage configuration for ClickHouse `MergeTree` family or `Log` family tables.
1. to work with data stored on `Amazon S3` disks, use [S3](/docs/en/engines/table-engines/integrations/s3.md) table engine.
2. to work with data stored in Azure Blob Storage use [AzureBlobStorage](/docs/en/engines/table-engines/integrations/azureBlobStorage.md) table engine.
3. Unsupported: to work with data in the Hadoop Distributed File System — [HDFS](/docs/en/engines/table-engines/integrations/hdfs.md) table engine.
:::

## Configuring external storage {#configuring-external-storage}

[MergeTree](/docs/en/engines/table-engines/mergetree-family/mergetree.md) and [Log](/docs/en/engines/table-engines/log-family/log.md) family table engines can store data to `S3`, `AzureBlobStorage`, `HDFS` (unsupported) using a disk with types `s3`, `azure_blob_storage`, `hdfs` (unsupported) accordingly.

Disk configuration requires:
1. `type` section, equal to one of `s3`, `azure_blob_storage`, `hdfs` (unsupported), `local_blob_storage`, `web`.
2. Configuration of a specific external storage type.

Starting from 24.1 clickhouse version, it is possible to use a new configuration option.
It requires to specify:
1. `type` equal to `object_storage`
2. `object_storage_type`, equal to one of `s3`, `azure_blob_storage` (or just `azure` from `24.3`), `hdfs` (unsupported), `local_blob_storage` (or just `local` from `24.3`), `web`.
Optionally, `metadata_type` can be specified (it is equal to `local` by default), but it can also be set to `plain`, `web` and, starting from `24.4`, `plain_rewritable`.
Usage of `plain` metadata type is described in [plain storage section](/docs/en/operations/storing-data.md/#storing-data-on-webserver), `web` metadata type can be used only with `web` object storage type, `local` metadata type stores metadata files locally (each metadata files contains mapping to files in object storage and some additional meta information about them).

E.g. configuration option
``` xml
<s3>
    <type>s3</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

is equal to configuration (from `24.1`):
``` xml
<s3>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>local</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

Configuration
``` xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

is equal to
``` xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

Example of full storage configuration will look like:
``` xml
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

Starting with 24.1 clickhouse version, it can also look like:
``` xml
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

In order to make a specific kind of storage a default option for all `MergeTree` tables add the following section to configuration file:
``` xml
<clickhouse>
    <merge_tree>
        <storage_policy>s3</storage_policy>
    </merge_tree>
</clickhouse>
```

If you want to configure a specific storage policy only to specific table, you can define it in settings while creating the table:

``` sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3';
```

You can also use `disk` instead of `storage_policy`. In this case it is not requires to have `storage_policy` section in configuration file, only `disk` section would be enough.

``` sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = 's3';
```

## Dynamic Configuration {#dynamic-configuration}

There is also a possibility to specify storage configuration without a predefined disk in configuration in a configuration file, but can be configured in the `CREATE`/`ATTACH` query settings.

The following example query builds on the above dynamic disk configuration and shows how to use a local disk to cache data from a table stored at a URL.

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
  # highlight-start
  SETTINGS disk = disk(
    type=web,
    endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
  );
  # highlight-end
```

The example below adds cache to external storage.

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
  # highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  # highlight-end
```

In the settings highlighted below notice that the disk of `type=web` is nested within
the disk of `type=cache`.

:::note
The example uses `type=web`, but any disk type can be configured as dynamic, even Local disk. Local disks require a path argument to be inside the server config parameter `custom_local_disks_base_directory`, which has no default, so set that also when using local disk.
:::

A combination of config-based configuration and sql-defined configuration is also possible:

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
  # highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  # highlight-end
```

where `web` is a from a server configuration file:

``` xml
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

Required parameters:

- `endpoint` — S3 endpoint URL in `path` or `virtual hosted` [styles](https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html). Endpoint URL should contain a bucket and root path to store data.
- `access_key_id` — S3 access key id.
- `secret_access_key` — S3 secret access key.

Optional parameters:

- `region` — S3 region name.
- `support_batch_delete` — This controls the check to see if batch deletes are supported. Set this to `false` when using Google Cloud Storage (GCS) as GCS does not support batch deletes and preventing the checks will prevent error messages in the logs.
- `use_environment_credentials` — Reads AWS credentials from the Environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN if they exist. Default value is `false`.
- `use_insecure_imds_request` — If set to `true`, S3 client will use insecure IMDS request while obtaining credentials from Amazon EC2 metadata. Default value is `false`.
- `expiration_window_seconds` — Grace period for checking if expiration-based credentials have expired. Optional, default value is `120`.
- `proxy` — Proxy configuration for S3 endpoint. Each `uri` element inside `proxy` block should contain a proxy URL.
- `connect_timeout_ms` — Socket connect timeout in milliseconds. Default value is `10 seconds`.
- `request_timeout_ms` — Request timeout in milliseconds. Default value is `5 seconds`.
- `retry_attempts` — Number of retry attempts in case of failed request. Default value is `10`.
- `single_read_retries` — Number of retry attempts in case of connection drop during read. Default value is `4`.
- `min_bytes_for_seek` — Minimal number of bytes to use seek operation instead of sequential read. Default value is `1 Mb`.
- `metadata_path` — Path on local FS to store metadata files for S3. Default value is `/var/lib/clickhouse/disks/<disk_name>/`.
- `skip_access_check` — If true, disk access checks will not be performed on disk start-up. Default value is `false`.
- `header` —  Adds specified HTTP header to a request to given endpoint. Optional, can be specified multiple times.
- `server_side_encryption_customer_key_base64` — If specified, required headers for accessing S3 objects with SSE-C encryption will be set.
- `server_side_encryption_kms_key_id` - If specified, required headers for accessing S3 objects with [SSE-KMS encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html) will be set. If an empty string is specified, the AWS managed S3 key will be used. Optional.
- `server_side_encryption_kms_encryption_context` - If specified alongside `server_side_encryption_kms_key_id`, the given encryption context header for SSE-KMS will be set. Optional.
- `server_side_encryption_kms_bucket_key_enabled` - If specified alongside `server_side_encryption_kms_key_id`, the header to enable S3 bucket keys for SSE-KMS will be set. Optional, can be `true` or `false`, defaults to nothing (matches the bucket-level setting).
- `s3_max_put_rps` — Maximum PUT requests per second rate before throttling. Default value is `0` (unlimited).
- `s3_max_put_burst` — Max number of requests that can be issued simultaneously before hitting request per second limit. By default (`0` value) equals to `s3_max_put_rps`.
- `s3_max_get_rps` — Maximum GET requests per second rate before throttling. Default value is `0` (unlimited).
- `s3_max_get_burst` — Max number of requests that can be issued simultaneously before hitting request per second limit. By default (`0` value) equals to `s3_max_get_rps`.
- `read_resource` — Resource name to be used for [scheduling](/docs/en/operations/workload-scheduling.md) of read requests to this disk. Default value is empty string (IO scheduling is not enabled for this disk).
- `write_resource` — Resource name to be used for [scheduling](/docs/en/operations/workload-scheduling.md) of write requests to this disk. Default value is empty string (IO scheduling is not enabled for this disk).
- `key_template` — Define the format with which the object keys are generated. By default, Clickhouse takes `root path` from `endpoint` option and adds random generated suffix. That suffix is a dir with 3 random symbols and a file name with 29 random symbols. With that option you have a full control how to the object keys are generated. Some usage scenarios require having random symbols in the prefix or in the middle of object key. For example: `[a-z]{3}-prefix-random/constant-part/random-middle-[a-z]{3}/random-suffix-[a-z]{29}`. The value is parsed with [`re2`](https://github.com/google/re2/wiki/Syntax). Only some subset of the syntax is supported. Check if your preferred format is supported before using that option. Disk isn't initialized if clickhouse is unable to generate a key by the value of `key_template`. It requires enabled feature flag [storage_metadata_write_full_object_key](/docs/en/operations/settings/settings#storage_metadata_write_full_object_key). It forbids declaring the `root path` in `endpoint` option. It requires definition of the option `key_compatibility_prefix`.
- `key_compatibility_prefix` — That option is required when option `key_template` is in use. In order to be able to read the objects keys which were stored in the metadata files with the metadata version lower that `VERSION_FULL_OBJECT_KEY`, the previous `root path` from the `endpoint` option should be set here.

:::note
Google Cloud Storage (GCS) is also supported using the type `s3`. See [GCS backed MergeTree](/docs/en/integrations/gcs).
:::

### Using Plain Storage {#plain-storage}

In `22.10` a new disk type `s3_plain` was introduced, which provides a write-once storage. Configuration parameters are the same as for `s3` disk type.
Unlike `s3` disk type, it stores data as is, e.g. instead of randomly-generated blob names, it uses normal file names (the same way as clickhouse stores files on local disk) and does not store any metadata locally, e.g. it is derived from data on `s3`.

This disk type allows to keep a static version of the table, as it does not allow executing merges on the existing data and does not allow inserting of new data.
A use case for this disk type is to create backups on it, which can be done via `BACKUP TABLE data TO Disk('plain_disk_name', 'backup_name')`. Afterwards you can do `RESTORE TABLE data AS data_restored FROM Disk('plain_disk_name', 'backup_name')` or using `ATTACH TABLE data (...) ENGINE = MergeTree() SETTINGS disk = 'plain_disk_name'`.

Configuration:
``` xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

Starting from `24.1` it is possible configure any object storage disk (`s3`, `azure`, `hdfs` (unsupported), `local`) using `plain` metadata type.

Configuration:
``` xml
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
Similar to the `s3_plain` disk type, it does not require additional storage for metadata files; instead, metadata is stored in S3.
Unlike `s3_plain` disk type, `s3_plain_rewritable` allows executing merges and supports INSERT operations.
[Mutations](/docs/en/sql-reference/statements/alter#mutations) and replication of tables are not supported.

A use case for this disk type are non-replicated `MergeTree` tables. Although the `s3` disk type is suitable for non-replicated
MergeTree tables, you may opt for the `s3_plain_rewritable` disk type if you do not require local metadata for the table and are
willing to accept a limited set of operations. This could be useful, for example, for system tables.

Configuration:
``` xml
<s3_plain_rewritable>
    <type>s3_plain_rewritable</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

is equal to
``` xml
<s3_plain_rewritable>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain_rewritable</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

Starting from `24.5` it is possible configure any object storage disk (`s3`, `azure`, `local`) using `plain_rewritable` metadata type.

### Using Azure Blob Storage {#azure-blob-storage}

`MergeTree` family table engines can store data to [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) using a disk with type `azure_blob_storage`.

As of February 2022, this feature is still a fresh addition, so expect that some Azure Blob Storage functionalities might be unimplemented.

Configuration markup:
``` xml
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

Connection parameters:
* `storage_account_url` - **Required**, Azure Blob Storage account URL, like `http://account.blob.core.windows.net` or `http://azurite1:10000/devstoreaccount1`.
* `container_name` - Target container name, defaults to `default-container`.
* `container_already_exists` - If set to `false`, a new container `container_name` is created in the storage account, if set to `true`, disk connects to the container directly, and if left unset, disk connects to the account, checks if the container `container_name` exists, and creates it if it doesn't exist yet.

Authentication parameters (the disk will try all available methods **and** Managed Identity Credential):
* `connection_string` - For authentication using a connection string.
* `account_name` and `account_key` - For authentication using Shared Key.

Limit parameters (mainly for internal usage):
* `s3_max_single_part_upload_size` - Limits the size of a single block upload to Blob Storage.
* `min_bytes_for_seek` - Limits the size of a seekable region.
* `max_single_read_retries` - Limits the number of attempts to read a chunk of data from Blob Storage.
* `max_single_download_retries` - Limits the number of attempts to download a readable buffer from Blob Storage.
* `thread_pool_size` - Limits the number of threads with which `IDiskRemote` is instantiated.
* `s3_max_inflight_parts_for_one_file` - Limits the number of put requests that can be run concurrently for one object.

Other parameters:
* `metadata_path` - Path on local FS to store metadata files for Blob Storage. Default value is `/var/lib/clickhouse/disks/<disk_name>/`.
* `skip_access_check` - If true, disk access checks will not be performed on disk start-up. Default value is `false`.
* `read_resource` — Resource name to be used for [scheduling](/docs/en/operations/workload-scheduling.md) of read requests to this disk. Default value is empty string (IO scheduling is not enabled for this disk).
* `write_resource` — Resource name to be used for [scheduling](/docs/en/operations/workload-scheduling.md) of write requests to this disk. Default value is empty string (IO scheduling is not enabled for this disk).
* `metadata_keep_free_space_bytes` - the amount of free metadata disk space to be reserved.

Examples of working configurations can be found in integration tests directory (see e.g. [test_merge_tree_azure_blob_storage](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_merge_tree_azure_blob_storage/configs/config.d/storage_conf.xml) or [test_azure_blob_storage_zero_copy_replication](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_azure_blob_storage_zero_copy_replication/configs/config.d/storage_conf.xml)).

:::note Zero-copy replication is not ready for production
Zero-copy replication is disabled by default in ClickHouse version 22.8 and higher.  This feature is not recommended for production use.
:::

## Using HDFS storage (Unsupported)

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

You can encrypt the data stored on [S3](/docs/en/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3), or [HDFS](#configuring-hdfs) (unsupported) external disks, or on a local disk. To turn on the encryption mode, in the configuration file you must define a disk with the type `encrypted` and choose a disk on which the data will be saved. An `encrypted` disk ciphers all written files on the fly, and when you read files from an `encrypted` disk it deciphers them automatically. So you can work with an `encrypted` disk like with a normal one.

Example of disk configuration:

``` xml
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

Required parameters:

- `type` — `encrypted`. Otherwise the encrypted disk is not created.
- `disk` — Type of disk for data storage.
- `key` — The key for encryption and decryption. Type: [Uint64](/docs/en/sql-reference/data-types/int-uint.md). You can use `key_hex` parameter to encode the key in hexadecimal form.
    You can specify multiple keys using the `id` attribute (see example below).

Optional parameters:

- `path` — Path to the location on the disk where the data will be saved. If not specified, the data will be saved in the root directory.
- `current_key_id` — The key used for encryption. All the specified keys can be used for decryption, and you can always switch to another key while maintaining access to previously encrypted data.
- `algorithm` — [Algorithm](/docs/en/sql-reference/statements/create/table.md/#create-query-encryption-codecs) for encryption. Possible values: `AES_128_CTR`, `AES_192_CTR` or `AES_256_CTR`. Default value: `AES_128_CTR`. The key length depends on the algorithm: `AES_128_CTR` — 16 bytes, `AES_192_CTR` — 24 bytes, `AES_256_CTR` — 32 bytes.

Example of disk configuration:

``` xml
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

``` xml
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

``` xml
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

- `path` - path to the directory with cache. Default: None, this setting is obligatory.

- `max_size` - maximum size of the cache in bytes or in readable format, e.g. `ki, Mi, Gi, etc`, example `10Gi` (such format works starting from `22.10` version). When the limit is reached, cache files are evicted according to the cache eviction policy. Default: None, this setting is obligatory.

- `cache_on_write_operations` - allow to turn on `write-through` cache (caching data on any write operations: `INSERT` queries, background merges). Default: `false`. The `write-through` cache can be disabled per query using setting `enable_filesystem_cache_on_write_operations` (data is cached only if both cache config settings and corresponding query setting are enabled).

- `enable_filesystem_query_cache_limit` - allow to limit the size of cache which is downloaded within each query (depends on user setting `max_query_cache_size`). Default: `false`.

- `enable_cache_hits_threshold` - number which defines how many times some data needs to be read before it will be cached. Default: `false`. This threshold can be defined by `cache_hits_threshold`. Default: `0`, e.g. the data is cached at the first attempt to read it.

- `enable_bypass_cache_with_threshold` - allows to skip cache completely in case the requested read range exceeds the threshold. Default: `false`. This threshold can be defined by `bypass_cache_threashold`. Default: `268435456` (`256Mi`).

- `max_file_segment_size` - a maximum size of a single cache file in bytes or in readable format (`ki, Mi, Gi, etc`, example `10Gi`). Default: `8388608` (`8Mi`).

- `max_elements` - a limit for a number of cache files. Default: `10000000`.

- `load_metadata_threads` - number of threads being used to load cache metadata on starting time. Default: `16`.

File Cache **query/profile settings**:

Some of these settings will disable cache features per query/profile that are enabled by default or in disk configuration settings. For example, you can enable cache in disk configuration and disable it per query/profile setting `enable_filesystem_cache` to `false`. Also setting `cache_on_write_operations` to `true` in disk configuration means that "write-though" cache is enabled. But if you need to disable this general setting per specific queries then setting `enable_filesystem_cache_on_write_operations` to `false` means that write operations cache will be disabled for a specific query/profile.

- `enable_filesystem_cache` - allows to disable cache per query even if storage policy was configured with `cache` disk type. Default: `true`.

- `read_from_filesystem_cache_if_exists_otherwise_bypass_cache` - allows to use cache in query only if it already exists, otherwise query data will not be written to local cache storage. Default: `false`.

- `enable_filesystem_cache_on_write_operations` - turn on `write-through` cache. This setting works only if setting `cache_on_write_operations` in cache configuration is turned on. Default: `false`. Cloud default value: `true`.

- `enable_filesystem_cache_log` - turn on logging to `system.filesystem_cache_log` table. Gives a detailed view of cache usage per query. It can be turn on for specific queries or enabled in a profile. Default: `false`.

- `max_query_cache_size` - a limit for the cache size, which can be written to local cache storage. Requires enabled `enable_filesystem_query_cache_limit` in cache configuration. Default: `false`.

- `skip_download_if_exceeds_query_cache` - allows to change the behaviour of setting `max_query_cache_size`. Default: `true`. If this setting is turned on and cache download limit during query was reached, no more cache will be downloaded to cache storage. If this setting is turned off and cache download limit during query was reached, cache will still be written by cost of evicting previously downloaded (within current query) data, e.g. second behaviour allows to preserve `last recently used` behaviour while keeping query cache limit.

**Warning**
Cache configuration settings and cache query settings correspond to the latest ClickHouse version, for earlier versions something might not be supported.

Cache **system tables**:

- `system.filesystem_cache` - system tables which shows current state of cache.

- `system.filesystem_cache_log` - system table which shows detailed cache usage per query. Requires `enable_filesystem_cache_log` setting to be `true`.

Cache **commands**:

- `SYSTEM DROP FILESYSTEM CACHE (<cache_name>) (ON CLUSTER)` -- `ON CLUSTER` is only supported when no `<cache_name>` is provided

- `SHOW FILESYSTEM CACHES` -- show list of filesystem caches which were configured on the server. (For versions <= `22.8` the command is named `SHOW CACHES`)

```sql
SHOW FILESYSTEM CACHES
```

Result:

``` text
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

- `DESCRIBE FILESYSTEM CACHE '<cache_name>'` - show cache configuration and some general statistics for a specific cache. Cache name can be taken from `SHOW FILESYSTEM CACHES` command. (For versions <= `22.8` the command is named `DESCRIBE CACHE`)

```sql
DESCRIBE FILESYSTEM CACHE 's3_cache'
```

``` text
┌────max_size─┬─max_elements─┬─max_file_segment_size─┬─boundary_alignment─┬─cache_on_write_operations─┬─cache_hits_threshold─┬─current_size─┬─current_elements─┬─path───────┬─background_download_threads─┬─enable_bypass_cache_with_threshold─┐
│ 10000000000 │      1048576 │             104857600 │            4194304 │                         1 │                    0 │         3276 │               54 │ /s3_cache/ │                           2 │                                  0 │
└─────────────┴──────────────┴───────────────────────┴────────────────────┴───────────────────────────┴──────────────────────┴──────────────┴──────────────────┴────────────┴─────────────────────────────┴────────────────────────────────────┘
```

Cache current metrics:

- `FilesystemCacheSize`

- `FilesystemCacheElements`

Cache asynchronous metrics:

- `FilesystemCacheBytes`

- `FilesystemCacheFiles`

Cache profile events:

- `CachedReadBufferReadFromSourceBytes`, `CachedReadBufferReadFromCacheBytes,`

- `CachedReadBufferReadFromSourceMicroseconds`, `CachedReadBufferReadFromCacheMicroseconds`

- `CachedReadBufferCacheWriteBytes`, `CachedReadBufferCacheWriteMicroseconds`

- `CachedWriteBufferCacheWriteBytes`, `CachedWriteBufferCacheWriteMicroseconds`

### Using static Web storage (read-only) {#web-storage}

This is a read-only disk. Its data is only read and never modified. A new table is loaded to this disk via `ATTACH TABLE` query (see example below). Local disk is not actually used, each `SELECT` query will result in a `http` request to fetch required data. All modification of the table data will result in an exception, i.e. the following types of queries are not allowed: [CREATE TABLE](/docs/en/sql-reference/statements/create/table.md), [ALTER TABLE](/docs/en/sql-reference/statements/alter/index.md), [RENAME TABLE](/docs/en/sql-reference/statements/rename.md/#misc_operations-rename_table), [DETACH TABLE](/docs/en/sql-reference/statements/detach.md) and [TRUNCATE TABLE](/docs/en/sql-reference/statements/truncate.md).
Web storage can be used for read-only purposes. An example use is for hosting sample data, or for migrating data.
There is a tool `clickhouse-static-files-uploader`, which prepares a data directory for a given table (`SELECT data_paths FROM system.tables WHERE name = 'table_name'`). For each table you need, you get a directory of files. These files can be uploaded to, for example, a web server with static files. After this preparation, you can load this table into any ClickHouse server via `DiskWeb`.

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
Storage can also be configured temporarily within a query, if a web dataset is not expected
to be used routinely, see [dynamic configuration](#dynamic-configuration) and skip editing the
configuration file.
:::

:::tip
A [demo dataset](https://github.com/ClickHouse/web-tables-demo) is hosted in GitHub.  To prepare your own tables for web storage see the tool [clickhouse-static-files-uploader](/docs/en/operations/storing-data.md/#storing-data-on-webserver)
:::

In this `ATTACH TABLE` query the `UUID` provided matches the directory name of the data, and the endpoint is the URL for the raw GitHub content.

```sql
# highlight-next-line
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
  # highlight-start
  SETTINGS disk = disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      );
  # highlight-end
```

A ready test case. You need to add this configuration to config:

``` xml
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

Required parameters:

- `type` — `web`. Otherwise the disk is not created.
- `endpoint` — The endpoint URL in `path` format. Endpoint URL must contain a root path to store data, where they were uploaded.

Optional parameters:

- `min_bytes_for_seek` — The minimal number of bytes to use seek operation instead of sequential read. Default value: `1` Mb.
- `remote_fs_read_backoff_threashold` — The maximum wait time when trying to read data for remote disk. Default value: `10000` seconds.
- `remote_fs_read_backoff_max_tries` — The maximum number of attempts to read with backoff. Default value: `5`.

If a query fails with an exception `DB:Exception Unreachable URL`, then you can try to adjust the settings: [http_connection_timeout](/docs/en/operations/settings/settings.md/#http_connection_timeout), [http_receive_timeout](/docs/en/operations/settings/settings.md/#http_receive_timeout), [keep_alive_timeout](/docs/en/operations/server-configuration-parameters/settings.md/#keep-alive-timeout).

To get files for upload run:
`clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>` (`--metadata-path` can be found in query `SELECT data_paths FROM system.tables WHERE name = 'table_name'`).

When loading files by `endpoint`, they must be loaded into `<endpoint>/store/` path, but config must contain only `endpoint`.

If URL is not reachable on disk load when the server is starting up tables, then all errors are caught. If in this case there were errors, tables can be reloaded (become visible) via `DETACH TABLE table_name` -> `ATTACH TABLE table_name`. If metadata was successfully loaded at server startup, then tables are available straight away.

Use [http_max_single_read_retries](/docs/en/operations/settings/settings.md/#http-max-single-read-retries) setting to limit the maximum number of retries during a single HTTP read.


### Zero-copy Replication (not ready for production) {#zero-copy}

Zero-copy replication is possible, but not recommended, with  `S3` and `HDFS` (unsupported) disks. Zero-copy replication means that if the data is stored remotely on several machines and needs to be synchronized, then only the metadata is replicated (paths to the data parts), but not the data itself.

:::note Zero-copy replication is not ready for production
Zero-copy replication is disabled by default in ClickHouse version 22.8 and higher.  This feature is not recommended for production use.
:::
