---
toc_priority: 68
toc_title: External Disks for Storing Data
---

# External Disks for Storing Data {#external-disks}

Data, processed in ClickHouse, is usually stored in the local file system — on the same machine with the ClickHouse server. That requires large-capacity disks, which can be expensive enough. To avoid that you can store the data remotely — on [Amazon S3](https://aws.amazon.com/s3/) disks or in the Hadoop Distributed File System ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)). 

To work with data stored on `Amazon S3` disks use [S3](../engines/table-engines/integrations/s3.md) table engine, and to work with data in the Hadoop Distributed File System — [HDFS](../engines/table-engines/integrations/hdfs.md) table engine. 

## Zero-copy Replication {#zero-copy}

ClickHouse supports zero-copy replication for `S3` and `HDFS` disks, which means that if the data is stored remotely on several machines and needs to be synchronized, then only the metadata is replicated (paths to the data parts), but not the data itself.

## Configuring HDFS {#configuring-hdfs}

[MergeTree](../engines/table-engines/mergetree-family/mergetree.md) and [Log](../engines/table-engines/log-family/log.md) family table engines can store data to HDFS using a disk with type `HDFS`.

Configuration markup:

``` xml
<yandex>
    <storage_configuration>
        <disks>
            <hdfs>
                <type>hdfs</type>
                <endpoint>hdfs://hdfs1:9000/clickhouse/</endpoint>
            </hdfs>
        </disks>
        <policies>
            <hdfs>
                <volumes>
                    <main>
                        <disk>hdfs</disk>
                    </main>
                </volumes>
            </hdfs>
        </policies>
    </storage_configuration>

    <merge_tree>
        <min_bytes_for_wide_part>0</min_bytes_for_wide_part>
    </merge_tree>
</yandex>
```

Required parameters:

-   `endpoint` — HDFS endpoint URL in `path` format. Endpoint URL should contain a root path to store data.

Optional parameters:

-   `min_bytes_for_seek` — The minimal number of bytes to use seek operation instead of sequential read. Default value: `1 Mb`.

## Using Virtual File System for Data Encryption {#encrypted-virtual-file-system}

You can encrypt the data stored on [S3](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-s3), or [HDFS](#configuring-hdfs) external disks, or on a local disk. To turn on the encryption mode, in the configuration file you must define a disk with the type `encrypted` and choose a disk on which the data will be saved. An `encrypted` disk ciphers all written files on the fly, and when you read files from an `encrypted` disk it deciphers them automatically. So you can work with an `encrypted` disk like with a normal one.

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

-   `type` — `encrypted`. Otherwise the encrypted disk is not created.
-   `disk` — Type of disk for data storage.
-   `key` — The key for encryption and decryption. Type: [Uint64](../sql-reference/data-types/int-uint.md). You can use `key_hex` parameter to encrypt in hexadecimal form.
    You can specify multiple keys using the `id` attribute (see example above).

Optional parameters:

-   `path` — Path to the location on the disk where the data will be saved. If not specified, the data will be saved in the root directory.
-   `current_key_id` — The key used for encryption. All the specified keys can be used for decryption, and you can always switch to another key while maintaining access to previously encrypted data.
-   `algorithm` — [Algorithm](../sql-reference/statements/create/table.md#create-query-encryption-codecs) for encryption. Possible values: `AES_128_CTR`, `AES_192_CTR` or `AES_256_CTR`. Default value: `AES_128_CTR`. The key length depends on the algorithm: `AES_128_CTR` — 16 bytes, `AES_192_CTR` — 24 bytes, `AES_256_CTR` — 32 bytes.

Example of disk configuration:

``` xml
<yandex>
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
</yandex>
```
