---
description: 'Documentation for the HTTP interface in ClickHouse, which provides REST
  API access to ClickHouse from any platform and programming language'
sidebar_label: 'HTTP Interface'
sidebar_position: 15
slug: /interfaces/http
title: 'HTTP Interface'
doc_type: 'reference'
---

import PlayUI from '@site/static/images/play.png';
import Image from '@theme/IdealImage';

# HTTP Interface

## Prerequisites {#prerequisites}

For the examples in this article you will need:
- to have a running instance of ClickHouse server
- have `curl` installed. On Ubuntu or Debian, run `sudo apt install curl` or refer to this [documentation](https://curl.se/download.html) for installation instructions.

## Overview {#overview}

The HTTP interface lets you use ClickHouse on any platform from any programming language in the form of a REST API. The HTTP interface is more limited than the native interface, but it has better language support.

By default, `clickhouse-server` listens on the following ports:
- port 8123 for HTTP
- port 8443 for HTTPS can be enabled

If you make a `GET /` request without any parameters, a 200 response code is returned along with the string "Ok.":

```bash
$ curl 'http://localhost:8123/'
Ok.
```

"Ok." is the default value defined in [`http_server_default_response`](../operations/server-configuration-parameters/settings.md#http_server_default_response) and can be changed if desired.

Also see: [HTTP response codes caveats](#http_response_codes_caveats).

## Web user interface {#web-ui}

ClickHouse includes a web user interface, which can be accessed from the following address: 

```text
http://localhost:8123/play
```

The web UI supports displaying progress during query runtime, query cancellation, and result streaming.
It has a secret feature for displaying charts and graphs for query pipelines.

The web UI is designed for professionals like you.

<Image img={PlayUI} size="md" alt="ClickHouse Web UI screenshot" />

In health-check scripts use the `GET /ping` request. This handler always returns "Ok." (with a line feed at the end). Available from version 18.12.13. See also `/replicas_status` to check replica's delay.

```bash
$ curl 'http://localhost:8123/ping'
Ok.
$ curl 'http://localhost:8123/replicas_status'
Ok.
```

## Querying over HTTP/HTTPS {#querying}

To query over HTTP/HTTPS there are three options:
- send the request as a URL 'query' parameter
- using the POST method.
- Send the beginning of the query in the 'query' parameter, and the rest using POST

:::note
The size of the URL is limited to 1 MiB by default, this can be changed with the `http_max_uri_size` setting.
:::

If successful, you receive the 200 response code and the result in the response body.
If an error occurs, you receive the 500 response code and an error description text in the response body.

Requests using GET are 'readonly'. This means that for queries that modify data, you can only use the POST method. 
You can send the query itself either in the POST body or in the URL parameter. Let's look at some examples.

In the example below curl is used to send the query `SELECT 1`. Note the use of URL encoding for the space: `%20`.

```bash title="command"
curl 'http://localhost:8123/?query=SELECT%201'
```

```response title="Response"
1
```

In this example wget is used with the `-nv` (non-verbose) and `-O-` parameters to output the result to the terminal.
In this case it is not necessary to use URL encoding for the space:

```bash title="command"
wget -nv -O- 'http://localhost:8123/?query=SELECT 1'
```

```response
1
```

In this example we pipe a raw HTTP request into netcat:

```bash title="command"
echo -ne 'GET /?query=SELECT%201 HTTP/1.0\r\n\r\n' | nc localhost 8123
```

```response title="response"
HTTP/1.0 200 OK
X-ClickHouse-Summary: {"read_rows":"1","read_bytes":"1","written_rows":"0","written_bytes":"0","total_rows_to_read":"1","result_rows":"0","result_bytes":"0","elapsed_ns":"4505959","memory_usage":"1111711"}
Date: Tue, 11 Nov 2025 18:16:01 GMT
Connection: Close
Content-Type: text/tab-separated-values; charset=UTF-8
Access-Control-Expose-Headers: X-ClickHouse-Query-Id,X-ClickHouse-Summary,X-ClickHouse-Server-Display-Name,X-ClickHouse-Format,X-ClickHouse-Timezone,X-ClickHouse-Exception-Code,X-ClickHouse-Exception-Tag
X-ClickHouse-Server-Display-Name: MacBook-Pro.local
X-ClickHouse-Query-Id: ec0d8ec6-efc4-4e1d-a14f-b748e01f5294
X-ClickHouse-Format: TabSeparated
X-ClickHouse-Timezone: Europe/London
X-ClickHouse-Exception-Tag: dngjzjnxkvlwkeua

1
```

As you can see, the `curl` command is somewhat inconvenient in that spaces must be URL escaped.
Although `wget` escapes everything itself, we do not recommend using it because it does not work well over HTTP 1.1 when using keep-alive and Transfer-Encoding: chunked.

```bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

If part of the query is sent in the parameter, and part in the POST, a line feed is inserted between these two data parts.
For example, this won't work:

```bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

By default, data is returned in the [`TabSeparated`](/interfaces/formats/TabSeparated) format.

The `FORMAT` clause is used in the query to request any other format. For example:

```bash title="command"
wget -nv -O- 'http://localhost:8123/?query=SELECT 1, 2, 3 FORMAT JSON'
```

```response title="Response"
{
    "meta":
    [
        {
            "name": "1",
            "type": "UInt8"
        },
        {
            "name": "2",
            "type": "UInt8"
        },
        {
            "name": "3",
            "type": "UInt8"
        }
    ],

    "data":
    [
        {
            "1": 1,
            "2": 2,
            "3": 3
        }
    ],

    "rows": 1,

    "statistics":
    {
        "elapsed": 0.000515,
        "rows_read": 1,
        "bytes_read": 1
    }
}
```

You can use the `default_format` URL parameter or the `X-ClickHouse-Format` header to specify a default format other than `TabSeparated`.

```bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

You can use POST method with parameterized queries. The parameters are specified using curly braces with the parameter name and type, like `{name:Type}`. The parameter values are passed with the `param_name`:

```bash
$ curl -X POST -F 'query=select {p1:UInt8} + {p2:UInt8}' -F "param_p1=3" -F "param_p2=4" 'http://localhost:8123/'

7
```

## Insert queries over HTTP/HTTPS {#insert-queries}

The `POST` method of transmitting data is necessary for `INSERT` queries. In this case, you can write the beginning of the query in the URL parameter, and use POST to pass the data to insert. The data to insert could be, for example, a tab-separated dump from MySQL. In this way, the `INSERT` query replaces `LOAD DATA LOCAL INFILE` from MySQL.

### Examples {#examples}

To create a table:

```bash
$ echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

To use the familiar `INSERT` query for data insertion:

```bash
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

To send data separately from the query:

```bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

Any data format can be specified. For example, the 'Values' format, the same format as the one used when writing `INSERT INTO t VALUES`, can be specified:

```bash
$ echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

To insert data from a tab-separated dump, specify the corresponding format:

```bash
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

To read the table contents:

```bash
$ curl 'http://localhost:8123/?query=SELECT%20a%20FROM%20t'
7
8
9
10
11
12
1
2
3
4
5
6
```

:::note
Data is output in a random order due to parallel query processing
:::

To delete the table:

```bash
$ echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

For successful requests that do not return a data table, an empty response body is returned.

## Compression {#compression}

Compression can be used to reduce network traffic when transmitting a large amount of data, or for creating dumps that are immediately compressed.

You can use the internal ClickHouse compression format when transmitting data. The compressed data has a non-standard format, and you need the `clickhouse-compressor` program to work with it. It is installed by default with the `clickhouse-client` package. 

To increase the efficiency of data insertion, disable server-side checksum verification by using the [`http_native_compression_disable_checksumming_on_decompress`](../operations/settings/settings.md#http_native_compression_disable_checksumming_on_decompress) setting.

If you specify `compress=1` in the URL, the server will compress the data it sends to you. If you specify `decompress=1` in the URL, the server will decompress the data which you pass in the `POST` method.

You can also choose to use [HTTP compression](https://en.wikipedia.org/wiki/HTTP_compression). ClickHouse supports the following [compression methods](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens):

- `gzip`
- `br`
- `deflate`
- `xz`
- `zstd`
- `lz4`
- `bz2`
- `snappy`

To send a compressed `POST` request, append the request header `Content-Encoding: compression_method`.

In order for ClickHouse to compress the response, append the `Accept-Encoding: compression_method` header to the request. 

You can configure the data compression level using the [`http_zlib_compression_level`](../operations/settings/settings.md#http_zlib_compression_level) setting for all compression methods.

:::info
Some HTTP clients might decompress data from the server by default (with `gzip` and `deflate`) and you might get decompressed data even if you use the compression settings correctly.
:::

## Examples {#examples-compression}

To send compressed data to the server:

```bash
echo "SELECT 1" | gzip -c | \
curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

To receive the compressed data archive from the server:

```bash
curl -vsS "http://localhost:8123/?enable_http_compression=1" \
-H 'Accept-Encoding: gzip' --output result.gz -d 'SELECT number FROM system.numbers LIMIT 3'

zcat result.gz
0
1
2
```

To receive compressed data from the server, using gunzip to receive decompressed data:

```bash
curl -sS "http://localhost:8123/?enable_http_compression=1" \
-H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 3' | gunzip -
0
1
2
```

## Default database {#default-database}

You can use the `database` URL parameter or the `X-ClickHouse-Database` header to specify the default database.

```bash
echo 'SELECT number FROM numbers LIMIT 10' | curl 'http://localhost:8123/?database=system' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

By default, the database that is registered in the server settings is used as the default database. Out of the box, this is the database called `default`. Alternatively, you can always specify the database using a dot before the table name.

## Authentication {#authentication}

The username and password can be indicated in one of three ways:

1. Using HTTP Basic Authentication.

For example:

```bash
echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

2. In the `user` and `password` URL parameters

:::warning
We do not recommend using this method as the parameter might be logged by web proxy and cached in the browser
:::

For example:

```bash
echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

3. Using the 'X-ClickHouse-User' and 'X-ClickHouse-Key' headers

For example:

```bash
echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-
```

If the user name is not specified, then the `default` name is used. If the password is not specified, then an empty password is used.
You can also use the URL parameters to specify any settings for processing a single query or entire profiles of settings. 

For example:

```text
http://localhost:8123/?profile=web&max_rows_to_read=1000000000&query=SELECT+1
```

```bash
$ echo 'SELECT number FROM system.numbers LIMIT 10' | curl 'http://localhost:8123/?' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

For more information see:
- [Settings](/operations/settings/settings)
- [SET](/sql-reference/statements/set)

## Using ClickHouse sessions in the HTTP protocol {#using-clickhouse-sessions-in-the-http-protocol}

You can also use ClickHouse sessions in the HTTP protocol. To do this, you need to add the `session_id` `GET` parameter to the request. You can use any string as the session ID. 

By default, the session is terminated after 60 seconds of inactivity. To change this timeout (in seconds), modify the `default_session_timeout` setting in the server configuration, or add the `session_timeout` `GET` parameter to the request. 

To check the session status, use the `session_check=1` parameter. Only one query at a time can be executed within a single session.

You can receive information about the progress of a query in the `X-ClickHouse-Progress` response headers. To do this, enable [`send_progress_in_http_headers`](../operations/settings/settings.md#send_progress_in_http_headers). 

Below is an example of the header sequence:

```text
X-ClickHouse-Progress: {"read_rows":"261636","read_bytes":"2093088","total_rows_to_read":"1000000","elapsed_ns":"14050417","memory_usage":"22205975"}
X-ClickHouse-Progress: {"read_rows":"654090","read_bytes":"5232720","total_rows_to_read":"1000000","elapsed_ns":"27948667","memory_usage":"83400279"}
X-ClickHouse-Progress: {"read_rows":"1000000","read_bytes":"8000000","total_rows_to_read":"1000000","elapsed_ns":"38002417","memory_usage":"80715679"}
```

The possible header fields are:

| Header field         | Description                     |
|----------------------|---------------------------------|
| `read_rows`          | Number of rows read.            |
| `read_bytes`         | Volume of data read in bytes.   |
| `total_rows_to_read` | Total number of rows to be read.|
| `written_rows`       | Number of rows written.         |
| `written_bytes`      | Volume of data written in bytes.|
| `elapsed_ns`         | Query runtime in nanoseconds.|
| `memory_usage`       | Memory in bytes used by the query. |

Running requests do not stop automatically if the HTTP connection is lost. Parsing and data formatting are performed on the server-side, and using the network might be ineffective.

The following optional parameters exist:

| Parameters            | Description                               |
|-----------------------|-------------------------------------------|
| `query_id` (optional) | Can be passed as the query ID (any string). [`replace_running_query`](/operations/settings/settings#replace_running_query)|
| `quota_key` (optional)| Can be passed as the quota key (any string). ["Quotas"](/operations/quotas)   |

The HTTP interface allows passing external data (external temporary tables) for querying. For more information, see ["External data for query processing"](/engines/table-engines/special/external-data).

## Response buffering {#response-buffering}

Response buffering can be enabled on the server-side. The following URL parameters are provided for this purpose:
- `buffer_size`
-  `wait_end_of_query`

The following settings can be used:
- [`http_response_buffer_size`](/operations/settings/settings#http_response_buffer_size)
- [`http_wait_end_of_query`](/operations/settings/settings#http_wait_end_of_query)

`buffer_size` determines the number of bytes in the result to buffer in the server memory. If a result body is larger than this threshold, the buffer is written to the HTTP channel, and the remaining data is sent directly to the HTTP channel.

To ensure that the entire response is buffered, set `wait_end_of_query=1`. In this case, the data that is not stored in memory will be buffered in a temporary server file.

For example:

```bash
curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

:::tip
Use buffering to avoid situations where a query processing error occurred after the response code and HTTP headers were sent to the client. In this situation, an error message is written at the end of the response body, and on the client-side, the error can only be detected at the parsing stage.
:::

## Setting a role with query parameters {#setting-role-with-query-parameters}

This feature was added in ClickHouse 24.4.

In specific scenarios, setting the granted role first might be required before executing the statement itself.
However, it is not possible to send `SET ROLE` and the statement together, as multi-statements are not allowed:

```bash
curl -sS "http://localhost:8123" --data-binary "SET ROLE my_role;SELECT * FROM my_table;"
```

The command above results in an error:

```sql
Code: 62. DB::Exception: Syntax error (Multi-statements are not allowed)
```

To overcome this limitation, use the `role` query parameter instead:

```bash
curl -sS "http://localhost:8123?role=my_role" --data-binary "SELECT * FROM my_table;"
```

This is the equivalent of executing `SET ROLE my_role` before the statement.

Additionally, it is possible to specify multiple `role` query parameters:

```bash
curl -sS "http://localhost:8123?role=my_role&role=my_other_role" --data-binary "SELECT * FROM my_table;"
```

In this case, `?role=my_role&role=my_other_role` works similarly to executing `SET ROLE my_role, my_other_role` before the statement.

## HTTP response codes caveats {#http_response_codes_caveats}

Because of limitations of the HTTP protocol, a HTTP 200 response code does not guarantee that a query was successful.

Here is an example:

```bash
curl -v -Ss "http://localhost:8123/?max_block_size=1&query=select+sleepEachRow(0.001),throwIf(number=2)from+numbers(5)"
*   Trying 127.0.0.1:8123...
...
< HTTP/1.1 200 OK
...
Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: while executing 'FUNCTION throwIf(equals(number, 2) :: 1) -> throwIf(equals(number, 2))
```

The reason for this behavior is the nature of the HTTP protocol. The HTTP header is sent first with an HTTP code of 200, followed by the HTTP body, and then the error is injected into the body as plain text.

This behavior is independent of the format used, whether it's `Native`, `TSV`, or `JSON`; the error message will always be in the middle of the response stream.

You can mitigate this problem by enabling `wait_end_of_query=1` ([Response Buffering](#response-buffering)). In this case, sending of the HTTP header is delayed until the entire query is resolved. This however, does not completely solve the problem because the result must still fit within the [`http_response_buffer_size`](/operations/settings/settings#http_response_buffer_size), and other settings like [`send_progress_in_http_headers`](/operations/settings/settings#send_progress_in_http_headers) can interfere with the delay of the header.

:::tip
The only way to catch all errors is to analyze the HTTP body before parsing it using the required format.
:::

Such exceptions in ClickHouse have consistent exception format as below irrespective of which format used (eg. `Native`, `TSV`, `JSON`, etc) when `http_write_exception_in_output_format=0` (default) . Which makes it easy to parse and extract error messages on the client side.

```text
\r\n
__exception__\r\n
<TAG>\r\n
<error message>\r\n
<message_length> <TAG>\r\n
__exception__\r\n

```

Where `<TAG>` is a 16 byte random tag, which is the same tag sent in the `X-ClickHouse-Exception-Tag` response header.
The `<error message>` is the actual exception message (exact length can be found in `<message_length>`). The whole exception block described above can be up to 16 KiB.

Here is an example in `JSON` format

```bash
$ curl -v -Ss "http://localhost:8123/?max_block_size=1&query=select+sleepEachRow(0.001),throwIf(number=2)from+numbers(5)+FORMAT+JSON"
...
{
    "meta":
    [
        {
            "name": "sleepEachRow(0.001)",
            "type": "UInt8"
        },
        {
            "name": "throwIf(equals(number, 2))",
            "type": "UInt8"
        }
    ],

    "data":
    [
        {
            "sleepEachRow(0.001)": 0,
            "throwIf(equals(number, 2))": 0
        },
        {
            "sleepEachRow(0.001)": 0,
            "throwIf(equals(number, 2))": 0
        }
__exception__
dmrdfnujjqvszhav
Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: while executing 'FUNCTION throwIf(equals(__table1.number, 2_UInt8) :: 1) -> throwIf(equals(__table1.number, 2_UInt8)) UInt8 : 0'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 25.11.1.1)
262 dmrdfnujjqvszhav
__exception__
```

Here is similar example but in `CSV` format

```bash
$ curl -v -Ss "http://localhost:8123/?max_block_size=1&query=select+sleepEachRow(0.001),throwIf(number=2)from+numbers(5)+FORMAT+CSV"
...
<
0,0
0,0

__exception__
rumfyutuqkncbgau
Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: while executing 'FUNCTION throwIf(equals(__table1.number, 2_UInt8) :: 1) -> throwIf(equals(__table1.number, 2_UInt8)) UInt8 : 0'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 25.11.1.1)
262 rumfyutuqkncbgau
__exception__
```

## Queries with parameters {#cli-queries-with-parameters}

You can create a query with parameters and pass values for them from the corresponding HTTP request parameters. For more information, see [Queries with Parameters for CLI](../interfaces/cli.md#cli-queries-with-parameters).

### Example {#example-3}

```bash
$ curl -sS "<address>?param_id=2&param_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

### Tabs in URL Parameters {#tabs-in-url-parameters}

Query parameters are parsed from the "escaped" format. This has some benefits, such as the possibility to unambiguously parse nulls as `\N`. This means the tab character should be encoded as `\t` (or `\` and a tab). For example, the following contains an actual tab between `abc` and `123` and the input string is split into two values:

```bash
curl -sS "http://localhost:8123" -d "SELECT splitByChar('\t', 'abc      123')"
```

```response
['abc','123']
```

However, if you try to encode an actual tab using `%09` in a URL parameter, it won't get parsed properly:

```bash
curl -sS "http://localhost:8123?param_arg1=abc%09123" -d "SELECT splitByChar('\t', {arg1:String})"
Code: 457. DB::Exception: Value abc    123 cannot be parsed as String for query parameter 'arg1' because it isn't parsed completely: only 3 of 7 bytes was parsed: abc. (BAD_QUERY_PARAMETER) (version 23.4.1.869 (official build))
```

If you are using URL parameters, you will need to encode the `\t` as `%5C%09`. For example:

```bash
curl -sS "http://localhost:8123?param_arg1=abc%5C%09123" -d "SELECT splitByChar('\t', {arg1:String})"
```

```response
['abc','123']
```

## Predefined HTTP Interface {#predefined_http_interface}

ClickHouse supports specific queries through the HTTP interface. For example, you can write data to a table as follows:

```bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

ClickHouse also supports a Predefined HTTP Interface which can help you more easily integrate with third-party tools like [Prometheus exporter](https://github.com/ClickHouse/clickhouse_exporter). Let's look at an example.

First of all, add this section to your server configuration file.

`http_handlers` is configured to contain multiple `rule`. ClickHouse will match the HTTP requests received to the predefined type in `rule` and the first rule matched runs the handler. Then ClickHouse will execute the corresponding predefined query if the match is successful.

```yaml title="config.xml"
<http_handlers>
    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.metrics LIMIT 5 FORMAT Template SETTINGS format_template_resultset = 'prometheus_template_output_format_resultset', format_template_row = 'prometheus_template_output_format_row', format_template_rows_between_delimiter = '\n'</query>
        </handler>
    </rule>
    <rule>...</rule>
    <rule>...</rule>
</http_handlers>
```

You can now request the URL directly for data in the Prometheus format:

```bash
$ curl -v 'http://localhost:8123/predefined_query'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /predefined_query HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
>
< HTTP/1.1 200 OK
< Date: Tue, 28 Apr 2020 08:52:56 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< X-ClickHouse-Server-Display-Name: i-mloy5trc
< Transfer-Encoding: chunked
< X-ClickHouse-Query-Id: 96fe0052-01e6-43ce-b12a-6b7370de6e8a
< X-ClickHouse-Format: Template
< X-ClickHouse-Timezone: Asia/Shanghai
< Keep-Alive: timeout=10
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334","memory_usage":"8451671"}
<
# HELP "Query" "Number of executing queries"
# TYPE "Query" counter
"Query" 1

# HELP "Merge" "Number of executing background merges"
# TYPE "Merge" counter
"Merge" 0

# HELP "PartMutation" "Number of mutations (ALTER DELETE/UPDATE)"
# TYPE "PartMutation" counter
"PartMutation" 0

# HELP "ReplicatedFetch" "Number of data parts being fetched from replica"
# TYPE "ReplicatedFetch" counter
"ReplicatedFetch" 0

# HELP "ReplicatedSend" "Number of data parts being sent to replicas"
# TYPE "ReplicatedSend" counter
"ReplicatedSend" 0

* Connection #0 to host localhost left intact

* Connection #0 to host localhost left intact
```

Configuration options for `http_handlers` work as follows.

`rule` can configure the following parameters:
- `method`
- `headers`
- `url`
- `full_url`
- `handler`

Each of these are discussed below:

- `method` is responsible for matching the method part of the HTTP request. `method` fully conforms to the definition of [`method`]    
  (https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods) in the HTTP protocol. It is an optional configuration. If it is not defined in the   
  configuration file, it does not match the method portion of the HTTP request.

- `url` is responsible for matching the URL part (path and query string) of the HTTP request.
  If the `url` prefixed with `regex:` it expects [RE2](https://github.com/google/re2)'s regular expressions.
  It is an optional configuration. If it is not defined in the configuration file, it does not match the URL portion of the HTTP request.

- `full_url` same as `url`, but, includes complete URL, i.e. `schema://host:port/path?query_string`.
  Note, ClickHouse does not support "virtual hosts", so the `host` is an IP address (and not the value of `Host` header).

- `empty_query_string` - ensures that there is no query string (`?query_string`) in the request

- `headers` are responsible for matching the header part of the HTTP request. It is compatible with RE2's regular expressions. It is an optional 
  configuration. If it is not defined in the configuration file, it does not match the header portion of the HTTP request.

- `handler` contains the main processing part.

  It can have the following `type`:
  - [`predefined_query_handler`](#predefined_query_handler)
  - [`dynamic_query_handler`](#dynamic_query_handler)
  - [`static`](#static)
  - [`redirect`](#redirect)

  And the following parameters:
  - `query` — use with `predefined_query_handler` type, executes query when the handler is called.
  - `query_param_name` — use with `dynamic_query_handler` type, extracts and executes the value corresponding to the `query_param_name` value in 
       HTTP request parameters.
  - `status` — use with `static` type, response status code.
  - `content_type` — use with any type, response [content-type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type).
  - `http_response_headers` — use with any type, response headers map. Could be used to set content type as well.
  - `response_content` — use with `static` type, response content sent to client, when using the prefix 'file://' or 'config://', find the content 
    from the file or configuration sends to client.
  - `user` - user to execute the query from (default user is `default`).
    **Note**, you do not need to specify password for this user.

The configuration methods for different `type`s are discussed next.

### predefined_query_handler {#predefined_query_handler}

`predefined_query_handler` supports setting `Settings` and `query_params` values. You can configure `query` in the type of `predefined_query_handler`.

`query` value is a predefined query of `predefined_query_handler`, which is executed by ClickHouse when an HTTP request is matched and the result of the query is returned. It is a must configuration.

The following example defines the values of [`max_threads`](../operations/settings/settings.md#max_threads) and [`max_final_threads`](/operations/settings/settings#max_final_threads) settings, then queries the system table to check whether these settings were set successfully.

:::note
To keep the default `handlers` such as` query`, `play`,` ping`, add the `<defaults/>` rule.
:::

For example:

```yaml
<http_handlers>
    <rule>
        <url><![CDATA[regex:/query_param_with_url/(?P<name_1>[^/]+)]]></url>
        <methods>GET</methods>
        <headers>
            <XXX>TEST_HEADER_VALUE</XXX>
            <PARAMS_XXX><![CDATA[regex:(?P<name_2>[^/]+)]]></PARAMS_XXX>
        </headers>
        <handler>
            <type>predefined_query_handler</type>
            <query>
                SELECT name, value FROM system.settings
                WHERE name IN ({name_1:String}, {name_2:String})
            </query>
        </handler>
    </rule>
    <defaults/>
</http_handlers>
```

```bash
curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_final_threads' 'http://localhost:8123/query_param_with_url/max_threads?max_threads=1&max_final_threads=2'
max_final_threads    2
max_threads    1
```

:::note
In one `predefined_query_handler` only one `query` is supported.
:::

### dynamic_query_handler {#dynamic_query_handler}

In `dynamic_query_handler`, the query is written in the form of parameter of the HTTP request. The difference is that in `predefined_query_handler`, the query is written in the configuration file. `query_param_name` can be configured in `dynamic_query_handler`.

ClickHouse extracts and executes the value corresponding to the `query_param_name` value in the URL of the HTTP request. The default value of `query_param_name` is `/query` . It is an optional configuration. If there is no definition in the configuration file, the parameter is not passed in.

To experiment with this functionality, the following example defines the values of [`max_threads`](../operations/settings/settings.md#max_threads) and `max_final_threads` and `queries` whether the settings were set successfully.

Example:

```yaml
<http_handlers>
    <rule>
    <headers>
        <XXX>TEST_HEADER_VALUE_DYNAMIC</XXX>    </headers>
    <handler>
        <type>dynamic_query_handler</type>
        <query_param_name>query_param</query_param_name>
    </handler>
    </rule>
    <defaults/>
</http_handlers>
```

```bash
curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC'  'http://localhost:8123/own?max_threads=1&max_final_threads=2&param_name_1=max_threads&param_name_2=max_final_threads&query_param=SELECT%20name,value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D'
max_threads 1
max_final_threads   2
```

### static {#static}

`static` can return [`content_type`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type), [status](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status) and `response_content`. `response_content` can return the specified content.

For example, to return a message "Say Hi!":

```yaml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/hi</url>
            <handler>
                <type>static</type>
                <status>402</status>
                <content_type>text/html; charset=UTF-8</content_type>
                <http_response_headers>
                    <Content-Language>en</Content-Language>
                    <X-My-Custom-Header>43</X-My-Custom-Header>
                </http_response_headers>
                #highlight-next-line
                <response_content>Say Hi!</response_content>
            </handler>
        </rule>
        <defaults/>
</http_handlers>
```

`http_response_headers` could be used to set the content type instead of `content_type`.

```yaml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/hi</url>
            <handler>
                <type>static</type>
                <status>402</status>
                #begin-highlight
                <http_response_headers>
                    <Content-Type>text/html; charset=UTF-8</Content-Type>
                    <Content-Language>en</Content-Language>
                    <X-My-Custom-Header>43</X-My-Custom-Header>
                </http_response_headers>
                #end-highlight
                <response_content>Say Hi!</response_content>
            </handler>
        </rule>
        <defaults/>
</http_handlers>
```

```bash
curl -vv  -H 'XXX:xxx' 'http://localhost:8123/hi'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /hi HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 402 Payment Required
< Date: Wed, 29 Apr 2020 03:51:26 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=10
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334","memory_usage":"8451671"}
<
* Connection #0 to host localhost left intact
Say Hi!%
```

Find the content from the configuration send to client.

```yaml
<get_config_static_handler><![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]></get_config_static_handler>

<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_config_static_handler</url>
            <handler>
                <type>static</type>
                <response_content>config://get_config_static_handler</response_content>
            </handler>
        </rule>
</http_handlers>
```

```bash
$ curl -v  -H 'XXX:xxx' 'http://localhost:8123/get_config_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_config_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:01:24 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=10
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334","memory_usage":"8451671"}
<
* Connection #0 to host localhost left intact
<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>%
```

To find the content from the file send to client:

```yaml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_absolute_path_static_handler</url>
            <handler>
                <type>static</type>
                <content_type>text/html; charset=UTF-8</content_type>
                <http_response_headers>
                    <ETag>737060cd8c284d8af7ad3082f209582d</ETag>
                </http_response_headers>
                <response_content>file:///absolute_path_file.html</response_content>
            </handler>
        </rule>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_relative_path_static_handler</url>
            <handler>
                <type>static</type>
                <content_type>text/html; charset=UTF-8</content_type>
                <http_response_headers>
                    <ETag>737060cd8c284d8af7ad3082f209582d</ETag>
                </http_response_headers>
                <response_content>file://./relative_path_file.html</response_content>
            </handler>
        </rule>
</http_handlers>
```

```bash
$ user_files_path='/var/lib/clickhouse/user_files'
$ sudo echo "<html><body>Relative Path File</body></html>" > $user_files_path/relative_path_file.html
$ sudo echo "<html><body>Absolute Path File</body></html>" > $user_files_path/absolute_path_file.html
$ curl -vv -H 'XXX:xxx' 'http://localhost:8123/get_absolute_path_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_absolute_path_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:18:16 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=10
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334","memory_usage":"8451671"}
<
<html><body>Absolute Path File</body></html>
* Connection #0 to host localhost left intact
$ curl -vv -H 'XXX:xxx' 'http://localhost:8123/get_relative_path_static_handler'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /get_relative_path_static_handler HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
> XXX:xxx
>
< HTTP/1.1 200 OK
< Date: Wed, 29 Apr 2020 04:18:31 GMT
< Connection: Keep-Alive
< Content-Type: text/html; charset=UTF-8
< Transfer-Encoding: chunked
< Keep-Alive: timeout=10
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0","elapsed_ns":"662334","memory_usage":"8451671"}
<
<html><body>Relative Path File</body></html>
* Connection #0 to host localhost left intact
```

### redirect {#redirect}

`redirect` will do a `302` redirect to `location`

For instance this is how you can automatically add set user to `play` for ClickHouse play:

```xml
<clickhouse>
    <http_handlers>
        <rule>
            <methods>GET</methods>
            <url>/play</url>
            <handler>
                <type>redirect</type>
                <location>/play?user=play</location>
            </handler>
        </rule>
    </http_handlers>
</clickhouse>
```

## HTTP response headers {#http-response-headers}

ClickHouse allows you to configure custom HTTP response headers that can be applied to any kind of handler that can be configured. These headers can be set using the `http_response_headers` setting, which accepts key-value pairs representing header names and their values. This feature is particularly useful for implementing custom security headers, CORS policies, or any other HTTP header requirements across your ClickHouse HTTP interface.

For example, you can configure headers for:
- Regular query endpoints
- Web UI
- Health check.

It is also possible to specify `common_http_response_headers`. These will be applied to all http handlers defined in the configuration.

The headers will be included in the HTTP response for every configured handler.

In the example below, every server response will contain two custom headers: `X-My-Common-Header` and `X-My-Custom-Header`.

```xml
<clickhouse>
    <http_handlers>
        <common_http_response_headers>
            <X-My-Common-Header>Common header</X-My-Common-Header>
        </common_http_response_headers>
        <rule>
            <methods>GET</methods>
            <url>/ping</url>
            <handler>
                <type>ping</type>
                <http_response_headers>
                    <X-My-Custom-Header>Custom indeed</X-My-Custom-Header>
                </http_response_headers>
            </handler>
        </rule>
    </http_handlers>
</clickhouse>
```

## Valid JSON/XML response on exception during HTTP streaming {#valid-output-on-exception-http-streaming}

While query execution occurs over HTTP an exception can happen when part of the data has already been sent. Usually an exception is sent to the client in plain text.
Even if some specific data format was used to output data and the output may become invalid in terms of specified data format.
To prevent it, you can use setting [`http_write_exception_in_output_format`](/operations/settings/settings#http_write_exception_in_output_format) (disabled by default) that will tell ClickHouse to write an exception in specified format (currently supported for XML and JSON* formats).

Examples:

```bash
$ curl 'http://localhost:8123/?query=SELECT+number,+throwIf(number>3)+from+system.numbers+format+JSON+settings+max_block_size=1&http_write_exception_in_output_format=1'
{
    "meta":
    [
        {
            "name": "number",
            "type": "UInt64"
        },
        {
            "name": "throwIf(greater(number, 2))",
            "type": "UInt8"
        }
    ],

    "data":
    [
        {
            "number": "0",
            "throwIf(greater(number, 2))": 0
        },
        {
            "number": "1",
            "throwIf(greater(number, 2))": 0
        },
        {
            "number": "2",
            "throwIf(greater(number, 2))": 0
        }
    ],

    "rows": 3,

    "exception": "Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: while executing 'FUNCTION throwIf(greater(number, 2) :: 2) -> throwIf(greater(number, 2)) UInt8 : 1'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 23.8.1.1)"
}
```

```bash
$ curl 'http://localhost:8123/?query=SELECT+number,+throwIf(number>2)+from+system.numbers+format+XML+settings+max_block_size=1&http_write_exception_in_output_format=1'
<?xml version='1.0' encoding='UTF-8' ?>
<result>
    <meta>
        <columns>
            <column>
                <name>number</name>
                <type>UInt64</type>
            </column>
            <column>
                <name>throwIf(greater(number, 2))</name>
                <type>UInt8</type>
            </column>
        </columns>
    </meta>
    <data>
        <row>
            <number>0</number>
            <field>0</field>
        </row>
        <row>
            <number>1</number>
            <field>0</field>
        </row>
        <row>
            <number>2</number>
            <field>0</field>
        </row>
    </data>
    <rows>3</rows>
    <exception>Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: while executing 'FUNCTION throwIf(greater(number, 2) :: 2) -> throwIf(greater(number, 2)) UInt8 : 1'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 23.8.1.1)</exception>
</result>
```
