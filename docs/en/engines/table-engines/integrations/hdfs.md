---
sidebar_position: 6
sidebar_label: HDFS
---

# HDFS

This engine provides integration with the [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) ecosystem by allowing to manage data on [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) via ClickHouse. This engine is similar to the [File](../../../engines/table-engines/special/file.md#table_engines-file) and [URL](../../../engines/table-engines/special/url.md#table_engines-url) engines, but provides Hadoop-specific features.

## Usage {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

**Engine Parameters**

- `URI` - whole file URI in HDFS. The path part of `URI` may contain globs. In this case the table would be readonly.
-  `format` - specifies one of the available file formats. To perform
`SELECT` queries, the format must be supported for input, and to perform
`INSERT` queries – for output. The available formats are listed in the
[Formats](../../../interfaces/formats.md#formats) section.

**Example:**

**1.** Set up the `hdfs_engine_table` table:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** Fill file:

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** Query the data:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## Implementation Details {#implementation-details}

-   Reads and writes can be parallel.
-   [Zero-copy](../../../operations/storing-data.md#zero-copy) replication is supported.
-   Not supported:
    -   `ALTER` and `SELECT...SAMPLE` operations.
    -   Indexes.

**Globs in path**

Multiple path components can have globs. For being processed file should exists and matches to the whole path pattern. Listing of files determines during `SELECT` (not at `CREATE` moment).

-   `*` — Substitutes any number of any characters except `/` including empty string.
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

Constructions with `{}` are similar to the [remote](../../../sql-reference/table-functions/remote.md) table function.

**Example**

1.  Suppose we have several files in TSV format with the following URIs on HDFS:

    -  'hdfs://hdfs1:9000/some_dir/some_file_1'
    -  'hdfs://hdfs1:9000/some_dir/some_file_2'
    -  'hdfs://hdfs1:9000/some_dir/some_file_3'
    -  'hdfs://hdfs1:9000/another_dir/some_file_1'
    -  'hdfs://hdfs1:9000/another_dir/some_file_2'
    -  'hdfs://hdfs1:9000/another_dir/some_file_3'

1.  There are several ways to make a table consisting of all six files:

<!-- -->

``` sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

Another way:

``` sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

Table consists of all the files in both directories (all files should satisfy format and schema described in query):

``` sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

:::warning
If the listing of files contains number ranges with leading zeros, use the construction with braces for each digit separately or use `?`.
:::

**Example**

Create table with files named `file000`, `file001`, … , `file999`:

``` sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```
## Configuration {#configuration}

Similar to GraphiteMergeTree, the HDFS engine supports extended configuration using the ClickHouse config file. There are two configuration keys that you can use: global (`hdfs`) and user-level (`hdfs_*`). The global configuration is applied first, and then the user-level configuration is applied (if it exists).

``` xml
  <!-- Global configuration options for HDFS engine type -->
  <hdfs>
	<hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
	<hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
	<hadoop_security_authentication>kerberos</hadoop_security_authentication>
  </hdfs>

  <!-- Configuration specific for user "root" -->
  <hdfs_root>
	<hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  </hdfs_root>
```

### Configuration Options {#configuration-options}

#### Supported by libhdfs3 {#supported-by-libhdfs3}


| **parameter**                                         | **default value**       |
| -                                                     | -                       |
| rpc\_client\_connect\_tcpnodelay                      | true                    |
| dfs\_client\_read\_shortcircuit                       | true                    |
| output\_replace-datanode-on-failure                   | true                    |
| input\_notretry-another-node                          | false                   |
| input\_localread\_mappedfile                          | true                    |
| dfs\_client\_use\_legacy\_blockreader\_local          | false                   |
| rpc\_client\_ping\_interval                           | 10  * 1000              |
| rpc\_client\_connect\_timeout                         | 600 * 1000              |
| rpc\_client\_read\_timeout                            | 3600 * 1000             |
| rpc\_client\_write\_timeout                           | 3600 * 1000             |
| rpc\_client\_socekt\_linger\_timeout                  | -1                      |
| rpc\_client\_connect\_retry                           | 10                      |
| rpc\_client\_timeout                                  | 3600 * 1000             |
| dfs\_default\_replica                                 | 3                       |
| input\_connect\_timeout                               | 600 * 1000              |
| input\_read\_timeout                                  | 3600 * 1000             |
| input\_write\_timeout                                 | 3600 * 1000             |
| input\_localread\_default\_buffersize                 | 1 * 1024 * 1024         |
| dfs\_prefetchsize                                     | 10                      |
| input\_read\_getblockinfo\_retry                      | 3                       |
| input\_localread\_blockinfo\_cachesize                | 1000                    |
| input\_read\_max\_retry                               | 60                      |
| output\_default\_chunksize                            | 512                     |
| output\_default\_packetsize                           | 64 * 1024               |
| output\_default\_write\_retry                         | 10                      |
| output\_connect\_timeout                              | 600 * 1000              |
| output\_read\_timeout                                 | 3600 * 1000             |
| output\_write\_timeout                                | 3600 * 1000             |
| output\_close\_timeout                                | 3600 * 1000             |
| output\_packetpool\_size                              | 1024                    |
| output\_heeartbeat\_interval                          | 10 * 1000               |
| dfs\_client\_failover\_max\_attempts                  | 15                      |
| dfs\_client\_read\_shortcircuit\_streams\_cache\_size | 256                     |
| dfs\_client\_socketcache\_expiryMsec                  | 3000                    |
| dfs\_client\_socketcache\_capacity                    | 16                      |
| dfs\_default\_blocksize                               | 64 * 1024 * 1024        |
| dfs\_default\_uri                                     | "hdfs://localhost:9000" |
| hadoop\_security\_authentication                      | "simple"                |
| hadoop\_security\_kerberos\_ticket\_cache\_path       | ""                      |
| dfs\_client\_log\_severity                            | "INFO"                  |
| dfs\_domain\_socket\_path                             | ""                      |


[HDFS Configuration Reference](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) might explain some parameters.


#### ClickHouse extras {#clickhouse-extras}

| **parameter**                                         | **default value**       |
| -                                                     | -                       |
|hadoop\_kerberos\_keytab                               | ""                      |
|hadoop\_kerberos\_principal                            | ""                      |
|libhdfs3\_conf                                         | ""                      |

### Limitations {#limitations}
* `hadoop_security_kerberos_ticket_cache_path` and `libhdfs3_conf` can be global only, not user specific

## Kerberos support {#kerberos-support}

If the `hadoop_security_authentication` parameter has the value `kerberos`, ClickHouse authenticates via Kerberos.
Parameters are [here](#clickhouse-extras) and `hadoop_security_kerberos_ticket_cache_path` may be of help.
Note that due to libhdfs3 limitations only old-fashioned approach is supported,
datanode communications are not secured by SASL (`HADOOP_SECURE_DN_USER` is a reliable indicator of such
security approach). Use `tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh` for reference.

If `hadoop_kerberos_keytab`, `hadoop_kerberos_principal` or `hadoop_security_kerberos_ticket_cache_path` are specified, Kerberos authentication will be used. `hadoop_kerberos_keytab` and `hadoop_kerberos_principal` are mandatory in this case.
## HDFS Namenode HA support {#namenode-ha}

libhdfs3 support HDFS namenode HA.

- Copy `hdfs-site.xml` from an HDFS node to `/etc/clickhouse-server/`.
- Add following piece to ClickHouse config file:

``` xml
  <hdfs>
    <libhdfs3_conf>/etc/clickhouse-server/hdfs-site.xml</libhdfs3_conf>
  </hdfs>
```

- Then use `dfs.nameservices` tag value of `hdfs-site.xml` as the namenode address in the HDFS URI. For example, replace `hdfs://appadmin@192.168.101.11:8020/abc/` with `hdfs://appadmin@my_nameservice/abc/`.


## Virtual Columns {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**See Also**

-   [Virtual columns](../../../engines/table-engines/index.md#table_engines-virtual_columns)

[Original article](https://clickhouse.com/docs/en/engines/table-engines/integrations/hdfs/) <!--hide-->
