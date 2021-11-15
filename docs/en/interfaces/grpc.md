---
toc_priority: 19
toc_title: gRPC Protocol
---

# gRPC Protocol {#grpc-protocol}

## Introduction {#grpc-protocol-introduction}

ClickHouse supports [gRPC](https://en.wikipedia.org/wiki/GRPC). It is an open source remote procedure call system that uses HTTP/2 and Protocol Buffers. The implementation of gRPC protocol supports:

-   SSL; 
-   authentication; 
-   sessions; 
-   compression; 
-   parallel queries through the same channel; 
-   cancellation of queries; 
-   getting progress and logs; 
-   external tables.

The protocol specification is described in [clickhouse_grpc.proto](https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/grpc_protos/clickhouse_grpc.proto).

## ClickHouse Configuration {#grpc-protocol-configuration}

To use the gRPC protocol set `grpc_port` in the main [server configuration](../../operations/configuration-files/). See the following configuration example:

```xml
<grpc_port>9100</grpc_port>
    <grpc>
        <enable_ssl>false</enable_ssl>

        <!-- The following two files are used only if enable_ssl=1 -->
        <ssl_cert_file>/path/to/ssl_cert_file</ssl_cert_file>
        <ssl_key_file>/path/to/ssl_key_file</ssl_key_file>

        <!-- Whether server will request client for a certificate -->
        <ssl_require_client_auth>false</ssl_require_client_auth>

        <!-- The following file is used only if ssl_require_client_auth=1 -->
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

        <!-- Enable if you want very detailed logs -->
        <verbose_logs>false</verbose_logs>
    </grpc>
```

## Built-in Client {#grpc-client}

You can either write a client in any of the programming languages supported by gRPC by using the provided specification or use the built-in Python client.

The built-in client is [utils/grpc-client/clickhouse-grpc-client.py](https://github.com/ClickHouse/ClickHouse/blob/master/utils/grpc-client/clickhouse-grpc-client.py). It requires [grpcio and grpcio-tools](https://grpc.io/docs/languages/python/quickstart) modules. To run the client in interactive mode call it without arguments.

Arguments:

-   `--help` – Show this help message and exit.
-   `--host HOST, -h HOST` – The server name, ‘localhost’ by default. You can use either the name or the IPv4 or IPv6 address.
-   `--port PORT` – The port to connect to. This port should be enabled on the ClickHouse server (see grpc_port in the config).
-   `--user USER_NAME, -u USER_NAME` – The username. Default value: ‘default’.
-   `--password PASSWORD` – The password. Default value: empty string.
-   `--query QUERY, -q QUERY` – The query to process when using non-interactive mode.
-   `--database DATABASE, -d DATABASE` – Select the current default database. Default value: the current database from the server settings (‘default’ by default).
-   `--format OUTPUT_FORMAT, -f OUTPUT_FORMAT` – Use the specified default format to output the result.
-   `--debug` – Enables showing the debug information.

**Client Usage Example**

In the following example a table is created and loaded with data from a CSV file.

``` text
./clickhouse-grpc-client.py -q "CREATE TABLE grpc_example_table (id UInt32, text String) ENGINE = MergeTree() ORDER BY id;"
echo "0,Input data for" > a.txt
echo "1,gRPC protocol example" >> a.txt
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
