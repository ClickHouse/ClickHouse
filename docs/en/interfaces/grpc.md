---
toc_priority: 18
toc_title: gRPC Protocol
---

# gRPC Protocol {#grpc-protocol}

For the specification of the protocol see [clickhouse_grpc.proto](https://github.com/vitlibar/ClickHouse/blob/grpc-protocol/src/Server/grpc_protos/clickhouse_grpc.proto)

To use the protocol first set grpc_port in the main configuration file, and then you can either write a client in any of the programming languages supported by gRPC by using the provided schema or use the built-in client utils/grpc-client/clickhouse-grpc-client.py. The built-in client is operated very likely to clickhouse-client, for example

``` text
utils/grpc-client/clickhouse-grpc-client.py -q "SELECT sum(number) FROM numbers(10)"

cat a.txt | utils/grpc-client/clickhouse-grpc-client.py -q "INSERT INTO temp FORMAT TSV"
```
and so on. Without parameters it runs the built-in client in the interactive mode.

The implementation of gRPC protocol also supports compression, SSL, getting progress and logs, authentication, parallel queries through the same channel, cancellation of queries, sessions, external tables.

/* This file describes gRPC protocol supported in ClickHouse.
 *
 * To use this protocol a client should send one or more messages of the QueryInfo type
 * and then receive one or more messages of the Result type.
 * According to that the service provides four methods for that:
 * ExecuteQuery(QueryInfo) returns (Result)
 * ExecuteQueryWithStreamInput(stream QueryInfo) returns (Result)
 * ExecuteQueryWithStreamOutput(QueryInfo) returns (stream Result)
 * ExecuteQueryWithStreamIO(stream QueryInfo) returns (stream Result)
 * It's up to the client to choose which method to use.
 * For example, ExecuteQueryWithStreamInput() allows the client to add data multiple times
 * while executing a query, which is suitable for inserting many rows.
 */
