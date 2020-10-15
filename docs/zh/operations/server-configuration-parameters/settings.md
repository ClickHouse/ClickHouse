---
toc_priority: 57
toc_title: "\u670D\u52A1\u5668\u8BBE\u7F6E"
---

# 服务器配置 {#server-settings}

## builtin_dictionaries_reload_interval {#builtin-dictionaries-reload-interval}

重新加载内置字典的间隔时间（以秒为单位）。

ClickHouse每x秒重新加载内置字典。 这使得编辑字典 “on the fly”，而无需重新启动服务器。

默认值:3600.

**示例**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## 压缩 {#server-settings-compression}

数据压缩配置 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-引擎表。

!!! warning "警告"
    如果您刚开始使用ClickHouse，请不要使用它。

配置模板:

``` xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
    </case>
    ...
</compression>
```

`<case>` 参数:

-   `min_part_size` – The minimum size of a data part.
-   `min_part_size_ratio` – The ratio of the data part size to the table size.
-   `method` – Compression method. Acceptable values: `lz4` 或 `zstd`.

您可以配置多个 `<case>` 部分。

满足条件时的操作:

-   如果数据部分与条件集匹配，ClickHouse将使用指定的压缩方法。
-   如果数据部分匹配多个条件集，ClickHouse将使用第一个匹配的条件集。

如果没有满足数据部分的条件，ClickHouse使用 `lz4` 压缩。

**示例**

``` xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
    </case>
</compression>
```

## default_database {#default-database}

默认数据库。

要获取数据库列表，请使用 [SHOW DATABASES](../../sql-reference/statements/show.md#show-databases) 查询。

**示例**

``` xml
<default_database>default</default_database>
```

## default_profile {#default-profile}

默认配置文件。

配置文件位于`user_config`参数指定的文件中 .

**示例**

``` xml
<default_profile>default</default_profile>
```

## dictionaries_config {#server_configuration_parameters-dictionaries_config}

外部字典的配置文件的路径。

路径:

-   指定相对于服务器配置文件的绝对路径或路径。
-   路径可以包含通配符\*和?.

另请参阅 “[外部字典](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)”.

**示例**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## dictionaries_lazy_load {#server_configuration_parameters-dictionaries_lazy_load}

延迟加载字典。

如果 `true`，然后在第一次使用时创建每个字典。 如果字典创建失败，则使用该字典的函数将引发异常。

如果 `false`，服务器启动时创建所有字典，如果出现错误，服务器将关闭。

默认值为 `true`.

**示例**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format_schema_path {#server_configuration_parameters-format_schema_path}

包含输入数据方案的目录路径，例如输入数据的方案 [CapnProto](../../interfaces/formats.md#capnproto) 格式。

**示例**

``` xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## 石墨 {#server_configuration_parameters-graphite}

将数据发送到 [石墨](https://github.com/graphite-project).

设置:

-   host – The Graphite server.
-   port – The port on the Graphite server.
-   interval – The interval for sending, in seconds.
-   timeout – The timeout for sending data, in seconds.
-   root_path – Prefix for keys.
-   metrics – Sending data from the [系统。指标](../../operations/system-tables/metrics.md#system_tables-metrics) 桌子
-   events – Sending deltas data accumulated for the time period from the [系统。活动](../../operations/system-tables/events.md#system_tables-events) 桌子
-   events_cumulative – Sending cumulative data from the [系统。活动](../../operations/system-tables/events.md#system_tables-events) 桌子
-   asynchronous_metrics – Sending data from the [系统。asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) 桌子

您可以配置多个 `<graphite>` 条款 例如，您可以使用它以不同的时间间隔发送不同的数据。

**示例**

``` xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

## graphite_rollup {#server_configuration_parameters-graphite-rollup}

石墨细化数据的设置。

有关详细信息，请参阅 [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**示例**

``` xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

## http_port/https_port {#http-porthttps-port}

通过HTTP连接到服务器的端口。

如果 `https_port` 被指定, [openSSL](#server_configuration_parameters-openssl) 必须配置。

如果 `http_port` 指定时，即使设置了OpenSSL配置，也会忽略该配置。

**示例**

``` xml
<https_port>9999</https_port>
```

## http_server_default_response {#server_configuration_parameters-http_server_default_response}

访问ClickHouse HTTP(s)服务器时默认显示的页面。
默认值为 “Ok.” （最后有换行符)

**示例**

打开 `https://tabix.io/` 访问时 `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## 包括_从 {#server_configuration_parameters-include_from}

带替换的文件的路径。

有关详细信息，请参阅部分 “[配置文件](../configuration-files.md#configuration_files)”.

**示例**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## interserver_http_port {#interserver-http-port}

用于在ClickHouse服务器之间交换数据的端口。

**示例**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## interserver_http_host {#interserver-http-host}

其他服务器可用于访问此服务器的主机名。

如果省略，它以相同的方式作为定义 `hostname-f` 指挥部

用于脱离特定的网络接口。

**示例**

``` xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

## interserver_http_credentials {#server-settings-interserver-http-credentials}

用户名和密码用于在以下期间进行身份验证 [复制](../../engines/table-engines/mergetree-family/replication.md) 与复制\*引擎。 这些凭据仅用于副本之间的通信，与ClickHouse客户端的凭据无关。 服务器正在检查这些凭据以连接副本，并在连接到其他副本时使用相同的凭据。 因此，这些凭据应该为集群中的所有副本设置相同。
默认情况下，不使用身份验证。

本节包含以下参数:

-   `user` — username.
-   `password` — password.

**示例**

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
</interserver_http_credentials>
```

## keep_alive_timeout {#keep-alive-timeout}

ClickHouse在关闭连接之前等待传入请求的秒数。 默认为3秒。

**示例**

``` xml
<keep_alive_timeout>3</keep_alive_timeout>
```

## listen_host {#server_configuration_parameters-listen_host}

对请求可能来自的主机的限制。 如果您希望服务器回答所有这些问题，请指定 `::`.

例:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## 记录器 {#server_configuration_parameters-logger}

日志记录设置。

键:

-   level – Logging level. Acceptable values: `trace`, `debug`, `information`, `warning`, `error`.
-   log – The log file. Contains all the entries according to `level`.
-   errorlog – Error log file.
-   size – Size of the file. Applies to `log`和`errorlog`. 一旦文件到达 `size`，ClickHouse存档并重命名它，并在其位置创建一个新的日志文件。
-   count – The number of archived log files that ClickHouse stores.

**示例**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
</logger>
```

还支持写入系统日志。 配置示例:

``` xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

键:

-   use_syslog — Required setting if you want to write to the syslog.
-   address — The host\[:port\] of syslogd. If omitted, the local daemon is used.
-   hostname — Optional. The name of the host that logs are sent from.
-   facility — [系统日志工具关键字](https://en.wikipedia.org/wiki/Syslog#Facility) 在大写字母与 “LOG_” 前缀: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`，等等）。
    默认值: `LOG_USER` 如果 `address` 被指定, `LOG_DAEMON otherwise.`
-   format – Message format. Possible values: `bsd` 和 `syslog.`

## 宏 {#macros}

复制表的参数替换。

如果不使用复制的表，则可以省略。

有关详细信息，请参阅部分 “[创建复制的表](../../engines/table-engines/mergetree-family/replication.md)”.

**示例**

``` xml
<macros incl="macros" optional="true" />
```

## mark_cache_size {#server-mark-cache-size}

表引擎使用的标记缓存的近似大小（以字节为单位） [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 家人

缓存为服务器共享，并根据需要分配内存。 缓存大小必须至少为5368709120。

**示例**

``` xml
<mark_cache_size>5368709120</mark_cache_size>
```

## max_concurrent_queries {#max-concurrent-queries}

同时处理的请求的最大数量。

**示例**

``` xml
<max_concurrent_queries>100</max_concurrent_queries>
```

## max_connections {#max-connections}

入站连接的最大数量。

**示例**

``` xml
<max_connections>4096</max_connections>
```

## max_open_files {#max-open-files}

打开文件的最大数量。

默认情况下: `maximum`.

我们建议在Mac OS X中使用此选项，因为 `getrlimit()` 函数返回一个不正确的值。

**示例**

``` xml
<max_open_files>262144</max_open_files>
```

## max_table_size_to_drop {#max-table-size-to-drop}

限制删除表。

如果一个大小 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表超过 `max_table_size_to_drop` （以字节为单位），您无法使用删除查询将其删除。

如果仍然需要在不重新启动ClickHouse服务器的情况下删除表，请创建 `<clickhouse-path>/flags/force_drop_table` 文件并运行DROP查询。

默认值：50GB。

值0表示您可以删除所有表而不受任何限制。

**示例**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## merge_tree {#server_configuration_parameters-merge_tree}

微调中的表 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

有关详细信息，请参阅MergeTreeSettings。h头文件。

**示例**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## openSSL {#server_configuration_parameters-openssl}

SSL客户端/服务器配置。

对SSL的支持由 `libpoco` 图书馆. 该接口在文件中描述 [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

服务器/客户端设置的密钥:

-   privateKeyFile – The path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.
-   certificateFile – The path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` 包含证书。
-   caConfig – The path to the file or directory that contains trusted root certificates.
-   verificationMode – The method for checking the node’s certificates. Details are in the description of the [A.背景](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) 同学们 可能的值: `none`, `relaxed`, `strict`, `once`.
-   verificationDepth – The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.
-   loadDefaultCAFile – Indicates that built-in CA certificates for OpenSSL will be used. Acceptable values: `true`, `false`. \|
-   cipherList – Supported OpenSSL encryptions. For example: `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`.
-   cacheSessions – Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. 可接受的值: `true`, `false`.
-   sessionIdContext – A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. 始终建议使用此参数，因为如果服务器缓存会话，以及客户端请求缓存，它有助于避免出现问题。 默认值: `${application.name}`.
-   sessionCacheSize – The maximum number of sessions that the server caches. Default value: 1024\*20. 0 – Unlimited sessions.
-   sessionTimeout – Time for caching the session on the server.
-   extendedVerification – Automatically extended verification of certificates after the session ends. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1_1 – Require a TLSv1.1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1.2 connection. Acceptable values: `true`, `false`.
-   fips – Activates OpenSSL FIPS mode. Supported if the library’s OpenSSL version supports FIPS.
-   privateKeyPassphraseHandler – Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
-   invalidCertificateHandler – Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>` .
-   disableProtocols – Protocols that are not allowed to use.
-   preferServerCiphers – Preferred server ciphers on the client.

**设置示例:**

``` xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

## part_log {#server_configuration_parameters-part-log}

记录与之关联的事件 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). 例如，添加或合并数据。 您可以使用日志来模拟合并算法并比较它们的特征。 您可以可视化合并过程。

查询记录在 [系统。part_log](../../operations/system-tables/part_log.md#system_tables-part-log) 表，而不是在一个单独的文件。 您可以在以下命令中配置此表的名称 `table` 参数（见下文）。

使用以下参数配置日志记录:

-   `database` – Name of the database.
-   `table` – Name of the system table.
-   `partition_by` – Sets a [自定义分区键](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

**示例**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</part_log>
```

## 路径 {#server_configuration_parameters-path}

包含数据的目录的路径。

!!! note "注"
    尾部斜杠是强制性的。

**示例**

``` xml
<path>/var/lib/clickhouse/</path>
```

## query_log {#server_configuration_parameters-query-log}

用于记录接收到的查询的设置 [log_queries=1](../settings/settings.md) 设置。

查询记录在 [系统。query_log](../../operations/system-tables/query_log.md#system_tables-query_log) 表，而不是在一个单独的文件。 您可以更改表的名称 `table` 参数（见下文）。

使用以下参数配置日志记录:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [自定义分区键](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) 为了一张桌子
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

如果该表不存在，ClickHouse将创建它。 如果在ClickHouse服务器更新时查询日志的结构发生了更改，则会重命名具有旧结构的表，并自动创建新表。

**示例**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

## query_thread_log {#server_configuration_parameters-query-thread-log}

设置用于记录接收到的查询的线程 [log_query_threads=1](../settings/settings.md#settings-log-query-threads) 设置。

查询记录在 [系统。query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query-thread-log) 表，而不是在一个单独的文件。 您可以更改表的名称 `table` 参数（见下文）。

使用以下参数配置日志记录:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [自定义分区键](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) 对于一个系统表。
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

如果该表不存在，ClickHouse将创建它。 如果更新ClickHouse服务器时查询线程日志的结构发生了更改，则会重命名具有旧结构的表，并自动创建新表。

**示例**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_thread_log>
```

## trace_log {#server_configuration_parameters-trace_log}

设置为 [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) 系统表操作。

参数:

-   `database` — Database for storing a table.
-   `table` — Table name.
-   `partition_by` — [自定义分区键](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) 对于一个系统表。
-   `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.

默认服务器配置文件 `config.xml` 包含以下设置部分:

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</trace_log>
```

## query_masking_rules {#query-masking-rules}

基于正则表达式的规则，在将查询以及所有日志消息存储在服务器日志中之前，这些规则将应用于查询以及所有日志消息,
`system.query_log`, `system.text_log`, `system.processes` 表，并在日志中发送给客户端。 这允许防止
从SQL查询敏感数据泄漏（如姓名，电子邮件，个人
标识符或信用卡号码）记录。

**示例**

``` xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

配置字段:
- `name` -规则的名称（可选)
- `regexp` -RE2兼容正则表达式（强制性)
- `replace` -敏感数据的替换字符串（可选，默认情况下-六个星号)

屏蔽规则应用于整个查询（以防止敏感数据从格式错误/不可解析的查询泄漏）。

`system.events` 表有计数器 `QueryMaskingRulesMatch` 其中具有匹配的查询屏蔽规则的总数。

对于分布式查询，每个服务器必须单独配置，否则，子查询传递给其他
节点将被存储而不屏蔽。

## remote_servers {#server-settings-remote-servers}

所使用的集群的配置 [分布](../../engines/table-engines/special/distributed.md) 表引擎和由 `cluster` 表功能。

**示例**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

对于该值 `incl` 属性，请参阅部分 “[配置文件](../configuration-files.md#configuration_files)”.

**另请参阅**

-   [skip_unavailable_shards](../settings/settings.md#settings-skip_unavailable_shards)

## 时区 {#server_configuration_parameters-timezone}

服务器的时区。

指定为UTC时区或地理位置（例如，非洲/阿比让）的IANA标识符。

当DateTime字段输出为文本格式（打印在屏幕上或文件中）时，以及从字符串获取DateTime时，时区对于字符串和DateTime格式之间的转换是必需的。 此外，如果在输入参数中没有收到时区，则时区用于处理时间和日期的函数。

**示例**

``` xml
<timezone>Europe/Moscow</timezone>
```

## tcp_port {#server_configuration_parameters-tcp_port}

通过TCP协议与客户端通信的端口。

**示例**

``` xml
<tcp_port>9000</tcp_port>
```

## tcp_port_secure {#server_configuration_parameters-tcp_port_secure}

TCP端口，用于与客户端进行安全通信。 使用它与 [OpenSSL](#server_configuration_parameters-openssl) 设置。

**可能的值**

整数。

**默认值**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql_port {#server_configuration_parameters-mysql_port}

通过MySQL协议与客户端通信的端口。

**可能的值**

整数。

示例

``` xml
<mysql_port>9004</mysql_port>
```

## tmp_path {#server-settings-tmp_path}

用于处理大型查询的临时数据的路径。

!!! note "注"
    尾部斜杠是强制性的。

**示例**

``` xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## tmp_policy {#server-settings-tmp-policy}

从政策 [`storage_configuration`](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) 存储临时文件。
如果没有设置 [`tmp_path`](#server-settings-tmp_path) 被使用，否则被忽略。

!!! note "注"
    - `move_factor` 被忽略
- `keep_free_space_bytes` 被忽略
- `max_data_part_size_bytes` 被忽略
-您必须在该政策中只有一个卷

## uncompressed_cache_size {#server-settings-uncompressed_cache_size}

表引擎使用的未压缩数据的缓存大小（以字节为单位） [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

服务器有一个共享缓存。 内存按需分配。 如果选项使用缓存 [use_uncompressed_cache](../settings/settings.md#setting-use_uncompressed_cache) 被启用。

在个别情况下，未压缩的缓存对于非常短的查询是有利的。

**示例**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user_files_path {#server_configuration_parameters-user_files_path}

包含用户文件的目录。 在表函数中使用 [文件()](../../sql-reference/table-functions/file.md).

**示例**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## users_config {#users-config}

包含文件的路径:

-   用户配置。
-   访问权限。
-   设置配置文件。
-   配额设置。

**示例**

``` xml
<users_config>users.xml</users_config>
```

## zookeeper {#server-settings_zookeeper}

包含允许ClickHouse与 [zookpeer](http://zookeeper.apache.org/) 集群。

ClickHouse使用ZooKeeper存储复制表副本的元数据。 如果未使用复制的表，则可以省略此部分参数。

本节包含以下参数:

-   `node` — ZooKeeper endpoint. You can set multiple endpoints.

    例如:

<!-- -->

``` xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

      The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.

-   `session_timeout` — Maximum timeout for the client session in milliseconds.
-   `root` — The [znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) 隆隆隆隆路虏脢..陇.貌.垄拢卢虏禄.陇.貌路.隆拢脳枚脢虏.麓脢for脱 可选。
-   `identity` — User and password, that can be required by ZooKeeper to give access to requested znodes. Optional.

**配置示例**

``` xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
</zookeeper>
```

**另请参阅**

-   [复制](../../engines/table-engines/mergetree-family/replication.md)
-   [动物园管理员程序员指南](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

## use_minimalistic_part_header_in_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

ZooKeeper中数据部分头的存储方法。

此设置仅适用于 `MergeTree` 家人 它可以指定:

-   在全球范围内 [merge_tree](#server_configuration_parameters-merge_tree) 一节 `config.xml` 文件

    ClickHouse使用服务器上所有表的设置。 您可以随时更改设置。 当设置更改时，现有表会更改其行为。

-   对于每个表。

    创建表时，指定相应的 [发动机设置](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). 即使全局设置更改，具有此设置的现有表的行为也不会更改。

**可能的值**

-   0 — Functionality is turned off.
-   1 — Functionality is turned on.

如果 `use_minimalistic_part_header_in_zookeeper = 1`，然后 [复制](../../engines/table-engines/mergetree-family/replication.md) 表存储的数据部分的头紧凑使用一个单一的 `znode`. 如果表包含许多列，则此存储方法显着减少了Zookeeper中存储的数据量。

!!! attention "注意"
    申请后 `use_minimalistic_part_header_in_zookeeper = 1`，您不能将ClickHouse服务器降级到不支持此设置的版本。 在集群中的服务器上升级ClickHouse时要小心。 不要一次升级所有服务器。 在测试环境中或在集群的几台服务器上测试ClickHouse的新版本更安全。

      Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.

**默认值:** 0.

## disable_internal_dns_cache {#server-settings-disable-internal-dns-cache}

禁用内部DNS缓存。 推荐用于在系统中运行ClickHouse
随着频繁变化的基础设施，如Kubernetes。

**默认值:** 0.

## dns_cache_update_period {#server-settings-dns-cache-update-period}

更新存储在ClickHouse内部DNS缓存中的IP地址的周期（以秒为单位）。
更新是在一个单独的系统线程中异步执行的。

**默认值**: 15.

[原始文章](https://clickhouse.tech/docs/en/operations/server_configuration_parameters/settings/) <!--hide-->
