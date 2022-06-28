---
sidebar_position: 19
sidebar_label: HTTP Interface
---

# HTTP Interface {#http-interface}

The HTTP interface lets you use ClickHouse on any platform from any programming language in a form of REST API. The HTTP interface is more limited than the native interface, but it has better language support.

By default, `clickhouse-server` listens for HTTP on port 8123 (this can be changed in the config).
HTTPS can be enabled as well with port 8443 by default.

If you make a `GET /` request without parameters, it returns 200 response code and the string which defined in [http_server_default_response](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-http_server_default_response) default value “Ok.” (with a line feed at the end)

``` bash
$ curl 'http://localhost:8123/'
Ok.
```

Sometimes, `curl` command is not available on user operating systems. On Ubuntu or Debian, run `sudo apt install curl`. Please refer this [documentation](https://curl.se/download.html) to install it before running the examples.

Web UI can be accessed here: `http://localhost:8123/play`.

![Web UI](../images/play.png)

In health-check scripts use `GET /ping` request. This handler always returns “Ok.” (with a line feed at the end). Available from version 18.12.13. See also `/replicas_status` to check replica's delay.

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
$ curl 'http://localhost:8123/replicas_status'
Ok.
```

Send the request as a URL ‘query’ parameter, or as a POST. Or send the beginning of the query in the ‘query’ parameter, and the rest in the POST (we’ll explain later why this is necessary). The size of the URL is limited to 1 MiB by default, this can be changed with the `http_max_uri_size` setting.

If successful, you receive the 200 response code and the result in the response body.
If an error occurs, you receive the 500 response code and an error description text in the response body.

When using the GET method, ‘readonly’ is set. In other words, for queries that modify data, you can only use the POST method. You can send the query itself either in the POST body or in the URL parameter.

Examples:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%201'
1

$ wget -nv -O- 'http://localhost:8123/?query=SELECT 1'
1

$ echo -ne 'GET /?query=SELECT%201 HTTP/1.0\r\n\r\n' | nc localhost 8123
HTTP/1.0 200 OK
Date: Wed, 27 Nov 2019 10:30:18 GMT
Connection: Close
Content-Type: text/tab-separated-values; charset=UTF-8
X-ClickHouse-Server-Display-Name: clickhouse.ru-central1.internal
X-ClickHouse-Query-Id: 5abe861c-239c-467f-b955-8a201abb8b7f
X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}

1
```

As you can see, `curl` is somewhat inconvenient in that spaces must be URL escaped.
Although `wget` escapes everything itself, we do not recommend using it because it does not work well over HTTP 1.1 when using keep-alive and Transfer-Encoding: chunked.

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

If part of the query is sent in the parameter, and part in the POST, a line feed is inserted between these two data parts.
Example (this won’t work):

``` bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

By default, data is returned in [TabSeparated](formats.md#tabseparated) format.

You use the FORMAT clause of the query to request any other format.

Also, you can use the ‘default_format’ URL parameter or the ‘X-ClickHouse-Format’ header to specify a default format other than TabSeparated.

``` bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

The POST method of transmitting data is necessary for `INSERT` queries. In this case, you can write the beginning of the query in the URL parameter, and use POST to pass the data to insert. The data to insert could be, for example, a tab-separated dump from MySQL. In this way, the `INSERT` query replaces `LOAD DATA LOCAL INFILE` from MySQL.

**Examples**

Creating a table:

``` bash
$ echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

Using the familiar INSERT query for data insertion:

``` bash
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

Data can be sent separately from the query:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

You can specify any data format. The ‘Values’ format is the same as what is used when writing INSERT INTO t VALUES:

``` bash
$ echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

To insert data from a tab-separated dump, specify the corresponding format:

``` bash
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

Reading the table contents. Data is output in random order due to parallel query processing:

``` bash
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

Deleting the table.

``` bash
$ echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

For successful requests that do not return a data table, an empty response body is returned.


## Compression {#compression}

You can use compression to reduce network traffic when transmitting a large amount of data or for creating dumps that are immediately compressed.

You can use the internal ClickHouse compression format when transmitting data. The compressed data has a non-standard format, and you need `clickhouse-compressor` program to work with it. It is installed with the `clickhouse-client` package. To increase the efficiency of data insertion, you can disable server-side checksum verification by using the [http_native_compression_disable_checksumming_on_decompress](../operations/settings/settings.md#settings-http_native_compression_disable_checksumming_on_decompress) setting.

If you specify `compress=1` in the URL, the server will compress the data it sends to you. If you specify `decompress=1` in the URL, the server will decompress the data which you pass in the `POST` method.

You can also choose to use [HTTP compression](https://en.wikipedia.org/wiki/HTTP_compression). ClickHouse supports the following [compression methods](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens):

- `gzip`
- `br`
- `deflate`
- `xz`

To send a compressed `POST` request, append the request header `Content-Encoding: compression_method`.
In order for ClickHouse to compress the response, enable compression with [enable_http_compression](../operations/settings/settings.md#settings-enable_http_compression) setting and append `Accept-Encoding: compression_method` header to the request. You can configure the data compression level in the [http_zlib_compression_level](../operations/settings/settings.md#settings-http_zlib_compression_level) setting for all compression methods.

:::info    
Some HTTP clients might decompress data from the server by default (with `gzip` and `deflate`) and you might get decompressed data even if you use the compression settings correctly.
:::

**Examples**

``` bash
# Sending compressed data to the server
$ echo "SELECT 1" | gzip -c | \
  curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

``` bash
# Receiving compressed data archive from the server
$ curl -vsS "http://localhost:8123/?enable_http_compression=1" \
    -H 'Accept-Encoding: gzip' --output result.gz -d 'SELECT number FROM system.numbers LIMIT 3'
$ zcat result.gz
0
1
2
```

```bash
# Receiving compressed data from the server and using the gunzip to receive decompressed data
$ curl -sS "http://localhost:8123/?enable_http_compression=1" \
    -H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 3' | gunzip -
0
1
2
```

## Default Database {#default-database}

You can use the ‘database’ URL parameter or the ‘X-ClickHouse-Database’ header to specify the default database.

``` bash
$ echo 'SELECT number FROM numbers LIMIT 10' | curl 'http://localhost:8123/?database=system' --data-binary @-
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

By default, the database that is registered in the server settings is used as the default database. By default, this is the database called ‘default’. Alternatively, you can always specify the database using a dot before the table name.

The username and password can be indicated in one of three ways:

1.  Using HTTP Basic Authentication. Example:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

1.  In the ‘user’ and ‘password’ URL parameters. Example:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

1.  Using ‘X-ClickHouse-User’ and ‘X-ClickHouse-Key’ headers. Example:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-
```

If the user name is not specified, the `default` name is used. If the password is not specified, the empty password is used.
You can also use the URL parameters to specify any settings for processing a single query or entire profiles of settings. Example:http://localhost:8123/?profile=web&max_rows_to_read=1000000000&query=SELECT+1

For more information, see the [Settings](../operations/settings/index.md) section.

``` bash
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

For information about other parameters, see the section “SET”.

Similarly, you can use ClickHouse sessions in the HTTP protocol. To do this, you need to add the `session_id` GET parameter to the request. You can use any string as the session ID. By default, the session is terminated after 60 seconds of inactivity. To change this timeout, modify the `default_session_timeout` setting in the server configuration, or add the `session_timeout` GET parameter to the request. To check the session status, use the `session_check=1` parameter. Only one query at a time can be executed within a single session.

You can receive information about the progress of a query in `X-ClickHouse-Progress` response headers. To do this, enable [send_progress_in_http_headers](../operations/settings/settings.md#settings-send_progress_in_http_headers). Example of the header sequence:

``` text
X-ClickHouse-Progress: {"read_rows":"2752512","read_bytes":"240570816","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"5439488","read_bytes":"482285394","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"8783786","read_bytes":"819092887","total_rows_to_read":"8880128"}
```

Possible header fields:

-   `read_rows` — Number of rows read.
-   `read_bytes` — Volume of data read in bytes.
-   `total_rows_to_read` — Total number of rows to be read.
-   `written_rows` — Number of rows written.
-   `written_bytes` — Volume of data written in bytes.

Running requests do not stop automatically if the HTTP connection is lost. Parsing and data formatting are performed on the server-side, and using the network might be ineffective.
The optional ‘query_id’ parameter can be passed as the query ID (any string). For more information, see the section “Settings, replace_running_query”.

The optional ‘quota_key’ parameter can be passed as the quota key (any string). For more information, see the section “Quotas”.

The HTTP interface allows passing external data (external temporary tables) for querying. For more information, see the section “External data for query processing”.

## Response Buffering {#response-buffering}

You can enable response buffering on the server-side. The `buffer_size` and `wait_end_of_query` URL parameters are provided for this purpose.

`buffer_size` determines the number of bytes in the result to buffer in the server memory. If a result body is larger than this threshold, the buffer is written to the HTTP channel, and the remaining data is sent directly to the HTTP channel.

To ensure that the entire response is buffered, set `wait_end_of_query=1`. In this case, the data that is not stored in memory will be buffered in a temporary server file.

Example:

``` bash
$ curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

Use buffering to avoid situations where a query processing error occurred after the response code and HTTP headers were sent to the client. In this situation, an error message is written at the end of the response body, and on the client-side, the error can only be detected at the parsing stage.

### Queries with Parameters {#cli-queries-with-parameters}

You can create a query with parameters and pass values for them from the corresponding HTTP request parameters. For more information, see [Queries with Parameters for CLI](../interfaces/cli.md#cli-queries-with-parameters).

### Example {#example}

``` bash
$ curl -sS "<address>?param_id=2&param_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

## Predefined HTTP Interface {#predefined_http_interface}

ClickHouse supports specific queries through the HTTP interface. For example, you can write data to a table as follows:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

ClickHouse also supports Predefined HTTP Interface which can help you more easily integrate with third-party tools like [Prometheus exporter](https://github.com/percona-lab/clickhouse_exporter).

Example:

-   First of all, add this section to server configuration file:

<!-- -->

``` xml
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

-   You can now request the URL directly for data in the Prometheus format:

<!-- -->

``` bash
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
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
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

As you can see from the example if `http_handlers` is configured in the config.xml file and `http_handlers` can contain many `rules`. ClickHouse will match the HTTP requests received to the predefined type in `rule` and the first matched runs the handler. Then ClickHouse will execute the corresponding predefined query if the match is successful.

Now `rule` can configure `method`, `headers`, `url`, `handler`:
- `method` is responsible for matching the method part of the HTTP request. `method` fully conforms to the definition of [method](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods) in the HTTP protocol. It is an optional configuration. If it is not defined in the configuration file, it does not match the method portion of the HTTP request.

-   `url` is responsible for matching the URL part of the HTTP request. It is compatible with [RE2](https://github.com/google/re2)’s regular expressions. It is an optional configuration. If it is not defined in the configuration file, it does not match the URL portion of the HTTP request.

-   `headers` are responsible for matching the header part of the HTTP request. It is compatible with RE2’s regular expressions. It is an optional configuration. If it is not defined in the configuration file, it does not match the header portion of the HTTP request.

-   `handler` contains the main processing part. Now `handler` can configure `type`, `status`, `content_type`, `response_content`, `query`, `query_param_name`.
    `type` currently supports three types: [predefined_query_handler](#predefined_query_handler), [dynamic_query_handler](#dynamic_query_handler), [static](#static).

    -   `query` — use with `predefined_query_handler` type, executes query when the handler is called.

    -   `query_param_name` — use with `dynamic_query_handler` type, extracts and executes the value corresponding to the `query_param_name` value in HTTP request params.

    -   `status` — use with `static` type, response status code.

    -   `content_type` — use with any type, response [content-type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type).

    -   `response_content` — use with `static` type, response content sent to client, when using the prefix ‘file://’ or ‘config://’, find the content from the file or configuration sends to client.

Next are the configuration methods for different `type`.

### predefined_query_handler {#predefined_query_handler}

`predefined_query_handler` supports setting `Settings` and `query_params` values. You can configure `query` in the type of `predefined_query_handler`.

`query` value is a predefined query of `predefined_query_handler`, which is executed by ClickHouse when an HTTP request is matched and the result of the query is returned. It is a must configuration.

The following example defines the values of [max_threads](../operations/settings/settings.md#settings-max_threads) and `max_final_threads` settings, then queries the system table to check whether these settings were set successfully.

:::warning
To keep the default `handlers` such as` query`, `play`,` ping`, add the `<defaults/>` rule.
:::

Example:

``` xml
<http_handlers>
    <rule>
        <url><![CDATA[/query_param_with_url/\w+/(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></url>
        <methods>GET</methods>
        <headers>
            <XXX>TEST_HEADER_VALUE</XXX>
            <PARAMS_XXX><![CDATA[(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></PARAMS_XXX>
        </headers>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT value FROM system.settings WHERE name = {name_1:String}</query>
            <query>SELECT name, value FROM system.settings WHERE name = {name_2:String}</query>
        </handler>
    </rule>
    <defaults/>
</http_handlers>
```

``` bash
$ curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/query_param_with_url/1/max_threads/max_final_threads?max_threads=1&max_final_threads=2'
1
max_final_threads   2
```

:::warning
In one `predefined_query_handler` only supports one `query` of an insert type.
:::

### dynamic_query_handler {#dynamic_query_handler}

In `dynamic_query_handler`, the query is written in the form of param of the HTTP request. The difference is that in `predefined_query_handler`, the query is written in the configuration file. You can configure `query_param_name` in `dynamic_query_handler`.

ClickHouse extracts and executes the value corresponding to the `query_param_name` value in the URL of the HTTP request. The default value of `query_param_name` is `/query` . It is an optional configuration. If there is no definition in the configuration file, the param is not passed in.

To experiment with this functionality, the example defines the values of [max_threads](../operations/settings/settings.md#settings-max_threads) and `max_final_threads` and `queries` whether the settings were set successfully.

Example:

``` xml
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

``` bash
$ curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC'  'http://localhost:8123/own?max_threads=1&max_final_threads=2&param_name_1=max_threads&param_name_2=max_final_threads&query_param=SELECT%20name,value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D'
max_threads 1
max_final_threads   2
```

### static {#static}

`static` can return [content_type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type), [status](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status) and `response_content`. `response_content` can return the specified content.

Example:

Return a message.

``` xml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/hi</url>
            <handler>
                <type>static</type>
                <status>402</status>
                <content_type>text/html; charset=UTF-8</content_type>
                <response_content>Say Hi!</response_content>
            </handler>
        </rule>
        <defaults/>
</http_handlers>
```

``` bash
$ curl -vv  -H 'XXX:xxx' 'http://localhost:8123/hi'
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
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
* Connection #0 to host localhost left intact
Say Hi!%
```

Find the content from the configuration send to client.

``` xml
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

``` bash
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
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
* Connection #0 to host localhost left intact
<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>%
```

Find the content from the file send to client.

``` xml
<http_handlers>
        <rule>
            <methods>GET</methods>
            <headers><XXX>xxx</XXX></headers>
            <url>/get_absolute_path_static_handler</url>
            <handler>
                <type>static</type>
                <content_type>text/html; charset=UTF-8</content_type>
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
                <response_content>file://./relative_path_file.html</response_content>
            </handler>
        </rule>
</http_handlers>
```

``` bash
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
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
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
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
<html><body>Relative Path File</body></html>
* Connection #0 to host localhost left intact
```
