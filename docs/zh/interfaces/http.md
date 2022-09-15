---
sidebar_position: 19
sidebar_label: HTTP客户端
---

# HTTP客户端 {#http-interface}

HTTP接口允许您在任何编程语言的任何平台上使用ClickHouse。我们使用它在Java和Perl以及shell脚本中工作。在其他部门中，HTTP接口用于Perl、Python和Go。HTTP接口比原生接口受到更多的限制，但它具有更好的兼容性。

默认情况下，`clickhouse-server`会在`8123`端口上监控HTTP请求（这可以在配置中修改）。

如果你发送了一个未携带任何参数的`GET /`请求，它会返回一个字符串 «Ok.»（结尾有换行）。可以将它用在健康检查脚本中。

如果你发送了一个未携带任何参数的`GET /`请求，它返回响应码200和`OK`字符串定义，可在[Http服务响应配置](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-http_server_default_response)定义(在末尾添加换行)

``` bash
$ curl 'http://localhost:8123/'
Ok.
```

Web UI 可以通过这个地址访问: `http://localhost:8123/play`.
在运行状况检查脚本中，使用`GET /ping`请求。这个处理方法总是返回 "Ok"。(以换行结尾)。可从18.12.13版获得。请参见' /replicas_status '检查复制集的延迟。


``` bash
$ curl 'http://localhost:8123/ping'
Ok.
$ curl 'http://localhost:8123/replicas_status'
Ok.
```

通过URL中的 `query` 参数来发送请求，或者发送POST请求，或者将查询的开头部分放在URL的`query`参数中，其他部分放在POST中（我们会在后面解释为什么这样做是有必要的）。URL的大小会限制在16KB，所以发送大型查询时要时刻记住这点。

如果请求成功，将会收到200的响应状态码和响应主体中的结果。
如果发生了某个异常，将会收到500的响应状态码和响应主体中的异常描述信息。

当使用GET方法请求时，`readonly`会被设置。换句话说，若要作修改数据的查询，只能发送POST方法的请求。可以将查询通过POST主体发送，也可以通过URL参数发送。

示例:

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

可以看到，curl 命令由于空格需要 URL 转义，所以不是很方便。尽管 wget 命令对url做了 URL 转义，但我们并不推荐使用他，因为在 HTTP 1.1 协议下使用 keep-alive 和 Transfer-Encoding: chunked 头部设置它并不能很好的工作。

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

如您所见，curl有些不方便，因为空格必须进行URL转义。
尽管wget本身会对所有内容进行转义，但我们不推荐使用它，因为在使用keepalive和传输编码chunked时，它在HTTP 1.1上不能很好地工作。

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

如果部分查询是在参数中发送的，部分是在POST中发送的，则在这两个数据部分之间插入换行。

错误示例：

``` bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

默认情况下，返回的数据是`TabSeparated`格式的，更多信息，见[Formats](../interfaces/formats/)部分。

您可以使用查询的FORMAT子句来设置其他格式。

另外，还可以使用`default_format`URL参数或`X-ClickHouse-Format`头来指定TabSeparated之外的默认格式。

``` bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

INSERT必须通过POST方法来插入数据。在这种情况下，您可以在URL参数中编写查询的开始部分，并使用POST传递要插入的数据。例如，要插入的数据可以是来自MySQL的一个以tab分隔的存储。通过这种方式，INSERT查询替换了从MySQL查询的LOAD DATA LOCAL INFILE。

示例: 创建一个表:

``` bash
$ echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

使用类似INSERT的查询来插入数据：

``` bash
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

数据可以从查询中单独发送：

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

您可以指定任何数据格式。`Values`格式与将INSERT写入`t`值时使用的格式相同:

``` bash
$ echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

若要插入tab分割的数据，需要指定对应的格式：

``` bash
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

从表中读取内容。由于查询处理是并行的，数据以随机顺序输出。

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

删除表：

``` bash
$ echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

成功请求后并不会返回数据，返回一个空的响应体。

在传输数据时，可以使用ClickHouse内部压缩格式。压缩的数据具有非标准格式，您需要使用特殊的`clickhouse-compressor`程序来处理它(它是与`clickhouse-client`包一起安装的)。为了提高数据插入的效率，您可以通过使用[http_native_compression_disable_checksumming_on_decompress](../operations/settings/settings.md#settings-http_native_compression_disable_checksumming_on_decompress)设置禁用服务器端校验。

如果在URL中指定了`compress=1`，服务会返回压缩的数据。
如果在URL中指定了`decompress=1`，服务会解压通过POST方法发送的数据。

您也可以选择使用[HTTP compression](https://en.wikipedia.org/wiki/HTTP_compression)。发送一个压缩的POST请求，附加请求头`Content-Encoding: compression_method`。为了使ClickHouse响应，您必须附加`Accept-Encoding: compression_method`。ClickHouse支持`gzip`，`br`和`deflate` [compression methods](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens)。要启用HTTP压缩，必须使用ClickHouse[启用Http压缩](../operations/settings/settings.md#settings-enable_http_compression)配置。您可以在[Http zlib压缩级别](#settings-http_zlib_compression_level)设置中为所有压缩方法配置数据压缩级别。

您可以使用它在传输大量数据时减少网络流量，或者创建立即压缩的转储。

通过压缩发送数据的例子:

``` bash
#Sending data to the server:
$ curl -vsS "http://localhost:8123/?enable_http_compression=1" -d 'SELECT number FROM system.numbers LIMIT 10' -H 'Accept-Encoding: gzip'

#Sending data to the client:
$ echo "SELECT 1" | gzip -c | curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

!!! note "警告"
    一些HTTP客户端可能会在默认情况下从服务器解压数据(使用`gzip`和`deflate`)，即使您未正确地使用了压缩设置，您也可能会得到解压数据。

您可以使用`database`URL参数或`X-ClickHouse-Database`头来指定默认数据库。

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

默认情况下，在服务器设置中注册的数据库被用作默认数据库。默认情况下，它是名为`default`的数据库。或者，您可以始终在表名之前使用点来指定数据库。

用户名和密码可以通过以下三种方式指定：

1.  通过HTTP Basic Authentication。示例：

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

1.  通过URL参数中的`user`和`password`。示例：

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

1.  使用`X-ClickHouse-User`或`X-ClickHouse-Key`头指定，示例:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-
```

如果未指定用户名，则使用`default`。如果未指定密码，则使用空密码。
您还可以使用URL参数来指定处理单个查询或整个设置配置文件的任何设置。例子:`http://localhost:8123/?profile=web&max_rows_to_read=1000000000&query=SELECT+1`

更多信息，详见[设置](../operations/settings/index.md#settings)部分。

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

有关其他参数的信息，请参考`SET`一节。

类似地，您可以在HTTP协议中使用ClickHouse会话。为此，需要向请求添加`session_id`GET参数。您可以使用任何字符串作为会话ID。默认情况下，会话在60秒不活动后终止。要更改此超时配置，请修改服务器配置中的`default_session_timeout`设置，或向请求添加`session_timeout`GET参数。要检查会话状态，使用`session_check=1`参数。一次只能在单个会话中执行一个查询。

您可以在`X-ClickHouse-Progress`响应头中收到查询进度的信息。为此，启用[Http Header携带进度](../operations/settings/settings.md#settings-send_progress_in_http_headers)。示例：

``` text
X-ClickHouse-Progress: {"read_rows":"2752512","read_bytes":"240570816","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"5439488","read_bytes":"482285394","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"8783786","read_bytes":"819092887","total_rows_to_read":"8880128"}
```

显示字段信息:

-   `read_rows` — 读取的行数。
-   `read_bytes` — 读取的数据字节数。
-   `total_rows_to_read` — 读取的数据总行数。
-   `written_rows` — 写入数据行数。
-   `written_bytes` — 写入数据字节数。

如果HTTP连接丢失，运行的请求不会自动停止。解析和数据格式化是在服务器端执行的，使用Http连接可能无效。

可选的`query_id`参数可能当做query ID传入（或者任何字符串）。更多信息，详见[replace_running_query](../operations/settings/settings.md)部分。

可选的`quota_key`参数可能当做quota key传入（或者任何字符串）。更多信息，详见[Quotas](../operations/quotas.md#quotas)部分。

HTTP接口允许传入额外的数据（外部临时表）来查询。更多信息，详见[外部数据查询处理](../engines/table-engines/special/external-data.md)部分。

## 响应缓冲 {#response-buffering}

可以在服务器端启用响应缓冲。提供了`buffer_size`和`wait_end_of_query`两个URL参数来达此目的。

`buffer_size`决定了查询结果要在服务内存中缓冲多少个字节数据. 如果响应体比这个阈值大，缓冲区会写入到HTTP管道，剩下的数据也直接发到HTTP管道中。

为了确保整个响应体被缓冲，可以设置`wait_end_of_query=1`。这种情况下，存入内存的数据会被缓冲到服务端的一个临时文件中。

示例:

``` bash
$ curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

查询请求响应状态码和HTTP头被发送到客户端后，若发生查询处理出错，使用缓冲区可以避免这种情况的发生。在这种情况下，响应主体的结尾会写入一条错误消息，而在客户端，只能在解析阶段检测到该错误。

### 查询参数 {#cli-queries-with-parameters}

您可以使用参数创建查询，并通过相应的HTTP请求参数为它们传递值。有关更多信息，请参见[CLI查询参数](../interfaces/cli.md#cli-queries-with-parameters)。

### 示例 {#example}

``` bash
$ curl -sS "<address>?param_id=2&param_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

## 特定的HTTP接口 {#predefined_http_interface}

ClickHouse通过HTTP接口支持特定的查询。例如，您可以如下所示向表写入数据:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

ClickHouse还支持预定义的HTTP接口，可以帮助您更容易与第三方工具集成，如[Prometheus Exporter](https://github.com/percona-lab/clickhouse_exporter).

示例:

-   首先，将此部分添加到服务器配置文件中:

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

-   请求Prometheus格式的URL以获取数据:

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

正如您从示例中看到的，如果在`config.xml`文件中配置了`http_handlers`，并且`http_handlers`可以包含许多`规则`。ClickHouse将把接收到的HTTP请求与`rule`中的预定义类型进行匹配，第一个匹配的将运行处理程序。如果匹配成功，ClickHouse将执行相应的预定义查询。

现在`rule`可以配置`method`， `header`， `url`， `handler`:
- `method` 负责匹配HTTP请求的方法部分。 `method`完全符合HTTP协议中[method](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods)的定义。这是一个可选的配置。如果它没有在配置文件中定义，那么它与HTTP请求的方法部分不匹配。

-   `url` 负责匹配HTTP请求的URL部分。它匹配[RE2](https://github.com/google/re2)正则表达式。这是一个可选的配置。如果配置文件中没有定义它，则它与HTTP请求的URL部分不匹配。

-   `headers` 负责匹配HTTP请求的头部分。它与RE2的正则表达式兼容。这是一个可选的配置。如果它没有在配置文件中定义，那么它与HTTP请求的头部分不匹配。

-   `handler` 包含主要的处理部分。现在`handler`可以配置`type`, `status`, `content_type`, `response_content`, `query`, `query_param_name`。
    `type` 目前支持三种类型:[特定查询](#predefined_query_handler), [动态查询](#dynamic_query_handler), [static](#static).

    -   `query` — 使用`predefined_query_handler`类型，在调用处理程序时执行查询。

    -   `query_param_name` — 与`dynamic_query_handler`类型一起使用，提取并执行HTTP请求参数中与`query_param_name`值对应的值。

    -   `status` — 与`static`类型一起使用，响应状态代码。

    -   `content_type` — 与`static`类型一起使用，响应信息[content-type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type)。

    -   `response_content` — 与`static`类型一起使用，响应发送给客户端的内容，当使用前缀`file://`或`config://`时，从发送给客户端的文件或配置中查找内容。

接下来是不同`type`的配置方法。

### 特定查询 {#predefined_query_handler}

`predefined_query_handler` 支持设置`Settings`和`query_params`参数。您可以将`query`配置为`predefined_query_handler`类型。

`query` 是一个预定义的`predefined_query_handler`查询，它由ClickHouse在匹配HTTP请求并返回查询结果时执行。这是一个必须的配置。

以下是定义的[max_threads](../operations/settings/settings.md#settings-max_threads)和`max_final_threads`设置， 然后查询系统表以检查这些设置是否设置成功。

示例:

``` xml
<http_handlers>
    <rule>
        <url><![CDATA[/query_param_with_url/\w+/(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></url>
        <method>GET</method>
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
</http_handlers>
```

``` bash
$ curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/query_param_with_url/1/max_threads/max_final_threads?max_threads=1&max_final_threads=2'
1
max_final_threads   2
```

!!! note "警告"
    在一个`predefined_query_handler`中，只支持insert类型的一个`查询`。

### 动态查询 {#dynamic_query_handler}

`dynamic_query_handler`时，查询以HTTP请求参数的形式编写。区别在于，在`predefined_query_handler`中，查询是在配置文件中编写的。您可以在`dynamic_query_handler`中配置`query_param_name`。

ClickHouse提取并执行与HTTP请求URL中的`query_param_name`值对应的值。`query_param_name`的默认值是`/query`。这是一个可选的配置。如果配置文件中没有定义，则不会传入参数。

为了试验这个功能，示例定义了[max_threads](../operations/settings/settings.md#settings-max_threads)和`max_final_threads`，`queries`设置是否成功的值。

示例:

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
</http_handlers>
```

``` bash
$ curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC'  'http://localhost:8123/own?max_threads=1&max_final_threads=2&param_name_1=max_threads&param_name_2=max_final_threads&query_param=SELECT%20name,value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D'
max_threads 1
max_final_threads   2
```

### static {#static}

`static`可以返回[content_type](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type), [status](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status)和`response_content`。`response_content`可以返回指定的内容。

示例:

返回信息.

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

从配置中查找发送到客户端的内容。

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

从发送到客户端的文件中查找内容。

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

[来源文章](https://clickhouse.com/docs/zh/interfaces/http_interface/) <!--hide-->
