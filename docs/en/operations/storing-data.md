---
toc_priority: 68
toc_title: External Disks for Storing Data
---

# External Disks for Storing Data {#external-disks}

Data, processed in ClickHouse, is usually stored in the local file system — on the same machine with the ClickHouse server. That requires large-capacity disks, which can be expensive enough. To avoid that you can store the data remotely — on [Amazon s3](https://aws.amazon.com/s3/) disks or in the Hadoop Distributed File System ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)). 

To work with data stored on `Amazon s3` disks use [s3](../engines/table-engines/integrations/s3.md) table engine, and to work with data in the Hadoop Distributed File System — [HDFS](../engines/table-engines/integrations/hdfs.md) table engine. 

## Zero-copy Replication {#zero-copy}

ClickHouse supports zero-copy replication for `s3` and `HDFS` disks, which means that if the data is stored remotely on several machines and needs to be synchronized, then only the metadata is replicated (paths to the data parts), but not the data itself.

## Using HDFS for Data Storage {#table_engine-mergetree-hdfs}

[HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) is a distributed file system for remote data storage.

[MergeTree](../engines/table-engines/mergetree-family/mergetree.md) family table engines can store data to HDFS using a disk with type `HDFS`.

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

## Using Virtual File System for Encrypt Data {#encrypted-virtual-file-system}

You can encrypt the data and save it on [S3](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-s3) or [HDFS](#table_engine-mergetree-hdfs) external disks or a local disk. To do this, in the configuration file, you need to specify the disk with the type `encrypted` and the type of disk on which the data will be saved. An `encrypted` disk ciphers all written files on the fly, and when you read files from an `encrypted` disk it deciphers them automatically. So you can work with an `encrypted` disk like with a normal one.

Configuration markup:
``` xml
<yandex>
    <storage_configuration>
        <disks>
            <disk_s3>
                <type>s3</type>
                <endpoint>http://minio1:9001/root/data/</endpoint>
                <access_key_id>minio</access_key_id>
                <secret_access_key>minio123</secret_access_key>
            </disk_s3>
            <disk_memory>
                <type>memory</type>
            </disk_memory>
            <disk_local>
                <type>local</type>
                <path>/disk/</path>
            </disk_local>
            <disk_s3_encrypted>
                <type>encrypted</type>
                <disk>disk_s3</disk>
                <path>encrypted/</path>
                <key id="0">firstfirstfirstf</key>
                <key id="1">secondsecondseco</key>
                <current_key_id>1</current_key_id>
            </disk_s3_encrypted>
            <disk_local_encrypted>
                <type>encrypted</type>
                <disk>disk_local</disk>
                <path>encrypted/</path>
                <key>abcdefghijklmnop</key>
            </disk_local_encrypted>
        </disks>
    </storage_configuration>
</yandex>
```

Required parameters:

-   `type` — `encrypted`. Otherwise, the encrypted disk is not created.
-   `disk` — Type of disk for data storage.
-   `key` — The key for encryption and decryption. Type: [Uint64](../sql-reference/data-types/int-uint.md). You can use `key_hex` parameter to encrypt in hexadecimal form.
    You can specify multiple keys using the `id` attribute (see example).

Optional parameters:

-   `path` — Path to the location on the disk where the data will be saved. If not specified, the data will be saved to the root of the disk.
-   `current_key_id` — The key is used for encryption, and all the specified keys can be used for decryption. This way, you can switch to another key, while maintaining access to previously encrypted data.
-   `algorithm` — [Algorithm](../sql-reference/statements/create/table.md#create-query-encryption-codecs) for encryption. Can have one of the following values: `AES_128_CTR`, `AES_192_CTR` or `AES_256_CTR`. By default: `AES_128_CTR`. The key length depends on the algorithm: `AES_128_CTR` — 16 bytes, `AES_192_CTR` — 24 bytes, `AES_256_CTR` — 32 bytes.
