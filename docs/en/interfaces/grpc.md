---
toc_priority: 19
toc_title: gRPC Interface
---

# gRPC Interface {#grpc-interface}

## Introduction {#grpc-interface-introduction}

ClickHouse supports [gRPC](https://grpc.io/) interface. It is an open source remote procedure call system that uses HTTP/2 and [Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers). The implementation of gRPC in ClickHouse supports:

-   SSL; 
-   authentication; 
-   sessions; 
-   compression; 
-   parallel queries through the same channel; 
-   cancellation of queries; 
-   getting progress and logs; 
-   external tables.

The specification of the interface is described in [clickhouse_grpc.proto](https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/grpc_protos/clickhouse_grpc.proto).

## gRPC Configuration {#grpc-interface-configuration}

To use the gRPC interface set `grpc_port` in the main [server configuration](../operations/configuration-files.md). Other configuration options see in the following example:

```xml
<grpc_port>9100</grpc_port>
    <grpc>
        <enable_ssl>false</enable_ssl>

        <!-- The following two files are used only if SSL is enabled -->
        <ssl_cert_file>/path/to/ssl_cert_file</ssl_cert_file>
        <ssl_key_file>/path/to/ssl_key_file</ssl_key_file>

        <!-- Whether server requests client for a certificate -->
        <ssl_require_client_auth>false</ssl_require_client_auth>

        <!-- The following file is used only if ssl_require_client_auth=true -->
        <ssl_ca_cert_file>/path/to/ssl_ca_cert_file</ssl_ca_cert_file>

        <!-- Default compression algorithm (applied if client doesn't specify another algorithm, see result_compression in QueryInfo).
             Supported algorithms: none, deflate, gzip, stream_gzip -->
        <compression>deflate</compression>

        <!-- Default compression level (applied if client doesn't specify another level, see result_compression in QueryInfo).
             Supported levels: none, low, medium, high -->
        <compression_level>medium</compression_level>

        <!-- Send/receive message size limits in bytes. -1 means unlimited -->
        <max_send_message_size>-1</max_send_message_size>
        <max_receive_message_size>-1</max_receive_message_size>

        <!-- Enable if you want to get detailed logs -->
        <verbose_logs>false</verbose_logs>
    </grpc>
```

## Built-in Client {#grpc-client}

You can write a client in any of the programming languages supported by gRPC using the provided [specification](https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/grpc_protos/clickhouse_grpc.proto).
Or you can use a built-in Python client. It is placed in [utils/grpc-client/clickhouse-grpc-client.py](https://github.com/ClickHouse/ClickHouse/blob/master/utils/grpc-client/clickhouse-grpc-client.py) in the repository. The built-in client requires [grpcio and grpcio-tools](https://grpc.io/docs/languages/python/quickstart) Python modules. 

The client supports the following arguments:

-   `--help` – Shows a help message and exits.
-   `--host HOST, -h HOST` – A server name. Default value: `localhost`. You can use IPv4 or IPv6 addresses also.
-   `--port PORT` – A port to connect to. This port should be enabled in the ClickHouse server configuration (see `grpc_port`). Default value: `9100`.
-   `--user USER_NAME, -u USER_NAME` – A user name. Default value: `default`.
-   `--password PASSWORD` – A password. Default value: empty string.
-   `--query QUERY, -q QUERY` – A query to process when using non-interactive mode.
-   `--database DATABASE, -d DATABASE` – A default database. If not specified, the current database set in the server settings is used (`default` by default).
-   `--format OUTPUT_FORMAT, -f OUTPUT_FORMAT` – A result output [format](formats.md). Default value for interactive mode: `PrettyCompact`.
-   `--debug` – Enables showing debug information.

To run the client in an interactive mode call it without `--query` argument.

In a batch mode query data can be passed via `stdin`.

**Client Usage Example**

In the following example a table is created and loaded with data from a CSV file. Then the content of the table is queried.

``` bash
./clickhouse-grpc-client.py -q "CREATE TABLE grpc_example_table (id UInt32, text String) ENGINE = MergeTree() ORDER BY id;"
echo "0,Input data for" > a.txt ; echo "1,gRPC protocol example" >> a.txt
cat a.txt | ./clickhouse-grpc-client.py -q "INSERT INTO grpc_example_table FORMAT CSV"

./clickhouse-grpc-client.py --format PrettyCompact -q "SELECT * FROM grpc_example_table;"
```

Result:

``` text
┌─id─┬─text──────────────────┐
│  0 │ Input data for        │
│  1 │ gRPC protocol example │
└────┴───────────────────────┘
```
