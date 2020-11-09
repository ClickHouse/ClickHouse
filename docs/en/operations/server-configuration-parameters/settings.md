---
toc_priority: 57
toc_title: Server Settings
---

# Server Settings {#server-settings}

## builtin\_dictionaries\_reload\_interval {#builtin-dictionaries-reload-interval}

The interval in seconds before reloading built-in dictionaries.

ClickHouse reloads built-in dictionaries every x seconds. This makes it possible to edit dictionaries “on the fly” without restarting the server.

Default value: 3600.

**Example**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## compression {#server-settings-compression}

Data compression settings for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine tables.

!!! warning "Warning"
    Don’t use it if you have just started using ClickHouse.

Configuration template:

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

`<case>` fields:

-   `min_part_size` – The minimum size of a data part.
-   `min_part_size_ratio` – The ratio of the data part size to the table size.
-   `method` – Compression method. Acceptable values: `lz4` or `zstd`.

You can configure multiple `<case>` sections.

Actions when conditions are met:

-   If a data part matches a condition set, ClickHouse uses the specified compression method.
-   If a data part matches multiple condition sets, ClickHouse uses the first matched condition set.

If no conditions met for a data part, ClickHouse uses the `lz4` compression.

**Example**

``` xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
    </case>
</compression>
```

## default\_database {#default-database}

The default database.

To get a list of databases, use the [SHOW DATABASES](../../sql-reference/statements/show.md#show-databases) query.

**Example**

``` xml
<default_database>default</default_database>
```

## default\_profile {#default-profile}

Default settings profile.

Settings profiles are located in the file specified in the parameter `user_config`.

**Example**

``` xml
<default_profile>default</default_profile>
```

## dictionaries\_config {#server_configuration_parameters-dictionaries_config}

The path to the config file for external dictionaries.

Path:

-   Specify the absolute path or the path relative to the server config file.
-   The path can contain wildcards \* and ?.

See also “[External dictionaries](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)”.

**Example**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## dictionaries\_lazy\_load {#server_configuration_parameters-dictionaries_lazy_load}

Lazy loading of dictionaries.

If `true`, then each dictionary is created on first use. If dictionary creation failed, the function that was using the dictionary throws an exception.

If `false`, all dictionaries are created when the server starts, and if there is an error, the server shuts down.

The default is `true`.

**Example**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format\_schema\_path {#server_configuration_parameters-format_schema_path}

The path to the directory with the schemes for the input data, such as schemas for the [CapnProto](../../interfaces/formats.md#capnproto) format.

**Example**

``` xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## graphite {#server_configuration_parameters-graphite}

Sending data to [Graphite](https://github.com/graphite-project).

Settings:

-   host – The Graphite server.
-   port – The port on the Graphite server.
-   interval – The interval for sending, in seconds.
-   timeout – The timeout for sending data, in seconds.
-   root\_path – Prefix for keys.
-   metrics – Sending data from the [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics) table.
-   events – Sending deltas data accumulated for the time period from the [system.events](../../operations/system-tables/events.md#system_tables-events) table.
-   events\_cumulative – Sending cumulative data from the [system.events](../../operations/system-tables/events.md#system_tables-events) table.
-   asynchronous\_metrics – Sending data from the [system.asynchronous\_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) table.

You can configure multiple `<graphite>` clauses. For instance, you can use this for sending different data at different intervals.

**Example**

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

## graphite\_rollup {#server_configuration_parameters-graphite-rollup}

Settings for thinning data for Graphite.

For more details, see [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Example**

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

## http\_port/https\_port {#http-porthttps-port}

The port for connecting to the server over HTTP(s).

If `https_port` is specified, [openSSL](#server_configuration_parameters-openssl) must be configured.

If `http_port` is specified, the OpenSSL configuration is ignored even if it is set.

**Example**

``` xml
<https_port>9999</https_port>
```

## http\_server\_default\_response {#server_configuration_parameters-http_server_default_response}

The page that is shown by default when you access the ClickHouse HTTP(s) server.
The default value is “Ok.” (with a line feed at the end)

**Example**

Opens `https://tabix.io/` when accessing `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## include\_from {#server_configuration_parameters-include_from}

The path to the file with substitutions.

For more information, see the section “[Configuration files](../../operations/configuration-files.md#configuration_files)”.

**Example**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## interserver\_http\_port {#interserver-http-port}

Port for exchanging data between ClickHouse servers.

**Example**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## interserver\_http\_host {#interserver-http-host}

The hostname that can be used by other servers to access this server.

If omitted, it is defined in the same way as the `hostname-f` command.

Useful for breaking away from a specific network interface.

**Example**

``` xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

## interserver\_http\_credentials {#server-settings-interserver-http-credentials}

The username and password used to authenticate during [replication](../../engines/table-engines/mergetree-family/replication.md) with the Replicated\* engines. These credentials are used only for communication between replicas and are unrelated to credentials for ClickHouse clients. The server is checking these credentials for connecting replicas and use the same credentials when connecting to other replicas. So, these credentials should be set the same for all replicas in a cluster.
By default, the authentication is not used.

This section contains the following parameters:

-   `user` — username.
-   `password` — password.

**Example**

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
</interserver_http_credentials>
```

## keep\_alive\_timeout {#keep-alive-timeout}

The number of seconds that ClickHouse waits for incoming requests before closing the connection. Defaults to 3 seconds.

**Example**

``` xml
<keep_alive_timeout>3</keep_alive_timeout>
```

## listen\_host {#server_configuration_parameters-listen_host}

Restriction on hosts that requests can come from. If you want the server to answer all of them, specify `::`.

Examples:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## logger {#server_configuration_parameters-logger}

Logging settings.

Keys:

-   `level` – Logging level. Acceptable values: `trace`, `debug`, `information`, `warning`, `error`.
-   `log` – The log file. Contains all the entries according to `level`.
-   `errorlog` – Error log file.
-   `size` – Size of the file. Applies to `log`and`errorlog`. Once the file reaches `size`, ClickHouse archives and renames it, and creates a new log file in its place.
-   `count` – The number of archived log files that ClickHouse stores.

**Example**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
</logger>
```

Writing to the syslog is also supported. Config example:

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

Keys:

-   use\_syslog — Required setting if you want to write to the syslog.
-   address — The host\[:port\] of syslogd. If omitted, the local daemon is used.
-   hostname — Optional. The name of the host that logs are sent from.
-   facility — [The syslog facility keyword](https://en.wikipedia.org/wiki/Syslog#Facility) in uppercase letters with the “LOG\_” prefix: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`, and so on).
    Default value: `LOG_USER` if `address` is specified, `LOG_DAEMON otherwise.`
-   format – Message format. Possible values: `bsd` and `syslog.`

## send\_crash\_reports {#server_configuration_parameters-logger}

Settings for opt-in sending crash reports to the ClickHouse core developers team via [Sentry](https://sentry.io).
Enabling it, especially in pre-production environments, is greatly appreciated.

The server will need an access to public Internet via IPv4 (at the time of writing IPv6 is not supported by Sentry) for this feature to be functioning properly.

Keys:

-   `enabled` – Boolean flag to enable the feature. Set to `true` to allow sending crash reports.
-   `endpoint` – Overrides the Sentry endpoint.
-   `anonymize` - Avoid attaching the server hostname to crash report.
-   `http_proxy` - Configure HTTP proxy for sending crash reports.
-   `debug` - Sets the Sentry client into debug mode.
-   `tmp_path` - Filesystem path for temporary crash report state.

**Recommended way to use**

``` xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

## macros {#macros}

Parameter substitutions for replicated tables.

Can be omitted if replicated tables are not used.

For more information, see the section “[Creating replicated tables](../../engines/table-engines/mergetree-family/replication.md)”.

**Example**

``` xml
<macros incl="macros" optional="true" />
```

## mark\_cache\_size {#server-mark-cache-size}

Approximate size (in bytes) of the cache of marks used by table engines of the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) family.

The cache is shared for the server and memory is allocated as needed. The cache size must be at least 5368709120.

**Example**

``` xml
<mark_cache_size>5368709120</mark_cache_size>
```
## max\_server\_memory\_usage {#max_server_memory_usage}

Limits total RAM usage by the ClickHouse server. You can specify it only for the default profile.

Possible values:

-   Positive integer.
-   0 — Unlimited.

Default value: `0`.

**Additional Info**

The default `max_server_memory_usage` value is calculated as `memory_amount * max_server_memory_usage_to_ram_ratio`.

**See also**

-   [max\_memory\_usage](../../operations/settings/query-complexity.md#settings_max_memory_usage)
-   [max_server_memory_usage_to_ram_ratio](#max_server_memory_usage_to_ram_ratio)

## max_server_memory_usage_to_ram_ratio {#max_server_memory_usage_to_ram_ratio}

Defines the fraction of total physical RAM amount, available to the Clickhouse server. If the server tries to utilize more, the memory is cut down to the appropriate amount. 

Possible values:

-   Positive double.
-   0 — The Clickhouse server can use all available RAM.

Default value: `0`.

**Usage**

On hosts with low RAM and swap, you possibly need setting `max_server_memory_usage_to_ram_ratio` larger than 1.

**Example**

``` xml
<max_server_memory_usage_to_ram_ratio>0.9</max_server_memory_usage_to_ram_ratio>
```

**See Also**

-   [max_server_memory_usage](#max_server_memory_usage)

## max\_concurrent\_queries {#max-concurrent-queries}

The maximum number of simultaneously processed requests.

**Example**

``` xml
<max_concurrent_queries>100</max_concurrent_queries>
```

## max\_connections {#max-connections}

The maximum number of inbound connections.

**Example**

``` xml
<max_connections>4096</max_connections>
```

## max\_open\_files {#max-open-files}

The maximum number of open files.

By default: `maximum`.

We recommend using this option in Mac OS X since the `getrlimit()` function returns an incorrect value.

**Example**

``` xml
<max_open_files>262144</max_open_files>
```

## max\_table\_size\_to\_drop {#max-table-size-to-drop}

Restriction on deleting tables.

If the size of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `max_table_size_to_drop` (in bytes), you can’t delete it using a DROP query.

If you still need to delete the table without restarting the ClickHouse server, create the `<clickhouse-path>/flags/force_drop_table` file and run the DROP query.

Default value: 50 GB.

The value 0 means that you can delete all tables without any restrictions.

**Example**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## max\_thread\_pool\_size {#max-thread-pool-size}

The maximum number of threads in the Global Thread pool.

Default value: 10000.

**Example**

``` xml
<max_thread_pool_size>12000</max_thread_pool_size>
```

## merge\_tree {#server_configuration_parameters-merge_tree}

Fine tuning for tables in the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

For more information, see the MergeTreeSettings.h header file.

**Example**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## openSSL {#server_configuration_parameters-openssl}

SSL client/server configuration.

Support for SSL is provided by the `libpoco` library. The interface is described in the file [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

Keys for server/client settings:

-   privateKeyFile – The path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.
-   certificateFile – The path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` contains the certificate.
-   caConfig – The path to the file or directory that contains trusted root certificates.
-   verificationMode – The method for checking the node’s certificates. Details are in the description of the [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) class. Possible values: `none`, `relaxed`, `strict`, `once`.
-   verificationDepth – The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.
-   loadDefaultCAFile – Indicates that built-in CA certificates for OpenSSL will be used. Acceptable values: `true`, `false`. \|
-   cipherList – Supported OpenSSL encryptions. For example: `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`.
-   cacheSessions – Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. Acceptable values: `true`, `false`.
-   sessionIdContext – A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. This parameter is always recommended since it helps avoid problems both if the server caches the session and if the client requested caching. Default value: `${application.name}`.
-   sessionCacheSize – The maximum number of sessions that the server caches. Default value: 1024\*20. 0 – Unlimited sessions.
-   sessionTimeout – Time for caching the session on the server.
-   extendedVerification – Automatically extended verification of certificates after the session ends. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1\_1 – Require a TLSv1.1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1.2 connection. Acceptable values: `true`, `false`.
-   fips – Activates OpenSSL FIPS mode. Supported if the library’s OpenSSL version supports FIPS.
-   privateKeyPassphraseHandler – Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
-   invalidCertificateHandler – Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>` .
-   disableProtocols – Protocols that are not allowed to use.
-   preferServerCiphers – Preferred server ciphers on the client.

**Example of settings:**

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

## part\_log {#server_configuration_parameters-part-log}

Logging events that are associated with [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). For instance, adding or merging data. You can use the log to simulate merge algorithms and compare their characteristics. You can visualize the merge process.

Queries are logged in the [system.part\_log](../../operations/system-tables/part_log.md#system_tables-part-log) table, not in a separate file. You can configure the name of this table in the `table` parameter (see below).

Use the following parameters to configure logging:

-   `database` – Name of the database.
-   `table` – Name of the system table.
-   `partition_by` – Sets a [custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

**Example**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</part_log>
```

## path {#server_configuration_parameters-path}

The path to the directory containing data.

!!! note "Note"
    The trailing slash is mandatory.

**Example**

``` xml
<path>/var/lib/clickhouse/</path>
```

## prometheus {#server_configuration_parameters-prometheus}

Exposing metrics data for scraping from [Prometheus](https://prometheus.io).

Settings:

-   `endpoint` – HTTP endpoint for scraping metrics by prometheus server. Start from ‘/’.
-   `port` – Port for `endpoint`.
-   `metrics` – Flag that sets to expose metrics from the [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics) table.
-   `events` – Flag that sets to expose metrics from the [system.events](../../operations/system-tables/events.md#system_tables-events) table.
-   `asynchronous_metrics` – Flag that sets to expose current metrics values from the [system.asynchronous\_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) table.

**Example**

``` xml
 <prometheus>
        <endpoint>/metrics</endpoint>
        <port>8001</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
    </prometheus>
```

## query\_log {#server_configuration_parameters-query-log}

Setting for logging queries received with the [log\_queries=1](../../operations/settings/settings.md) setting.

Queries are logged in the [system.query\_log](../../operations/system-tables/query_log.md#system_tables-query_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

Use the following parameters to configure logging:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a table.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

If the table doesn’t exist, ClickHouse will create it. If the structure of the query log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

## query\_thread\_log {#server_configuration_parameters-query_thread_log}

Setting for logging threads of queries received with the [log\_query\_threads=1](../../operations/settings/settings.md#settings-log-query-threads) setting.

Queries are logged in the [system.query\_thread\_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

Use the following parameters to configure logging:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

If the table doesn’t exist, ClickHouse will create it. If the structure of the query thread log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_thread_log>
```

## trace\_log {#server_configuration_parameters-trace_log}

Settings for the [trace\_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) system table operation.

Parameters:

-   `database` — Database for storing a table.
-   `table` — Table name.
-   `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table.
-   `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.

The default server configuration file `config.xml` contains the following settings section:

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</trace_log>
```

## query\_masking\_rules {#query-masking-rules}

Regexp-based rules, which will be applied to queries as well as all log messages before storing them in server logs,
`system.query_log`, `system.text_log`, `system.processes` table, and in logs sent to the client. That allows preventing
sensitive data leakage from SQL queries (like names, emails, personal
identifiers or credit card numbers) to logs.

**Example**

``` xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

Config fields:
- `name` - name for the rule (optional)
- `regexp` - RE2 compatible regular expression (mandatory)
- `replace` - substitution string for sensitive data (optional, by default - six asterisks)

The masking rules are applied to the whole query (to prevent leaks of sensitive data from malformed / non-parsable queries).

`system.events` table have counter `QueryMaskingRulesMatch` which have an overall number of query masking rules matches.

For distributed queries each server have to be configured separately, otherwise, subqueries passed to other
nodes will be stored without masking.

## remote\_servers {#server-settings-remote-servers}

Configuration of clusters used by the [Distributed](../../engines/table-engines/special/distributed.md) table engine and by the `cluster` table function.

**Example**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

For the value of the `incl` attribute, see the section “[Configuration files](../../operations/configuration-files.md#configuration_files)”.

**See Also**

-   [skip\_unavailable\_shards](../../operations/settings/settings.md#settings-skip_unavailable_shards)

## timezone {#server_configuration_parameters-timezone}

The server’s time zone.

Specified as an IANA identifier for the UTC timezone or geographic location (for example, Africa/Abidjan).

The time zone is necessary for conversions between String and DateTime formats when DateTime fields are output to text format (printed on the screen or in a file), and when getting DateTime from a string. Besides, the time zone is used in functions that work with the time and date if they didn’t receive the time zone in the input parameters.

**Example**

``` xml
<timezone>Europe/Moscow</timezone>
```

## tcp\_port {#server_configuration_parameters-tcp_port}

Port for communicating with clients over the TCP protocol.

**Example**

``` xml
<tcp_port>9000</tcp_port>
```

## tcp\_port\_secure {#server_configuration_parameters-tcp_port_secure}

TCP port for secure communication with clients. Use it with [OpenSSL](#server_configuration_parameters-openssl) settings.

**Possible values**

Positive integer.

**Default value**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql\_port {#server_configuration_parameters-mysql_port}

Port for communicating with clients over MySQL protocol.

**Possible values**

Positive integer.

Example

``` xml
<mysql_port>9004</mysql_port>
```

## tmp\_path {#tmp-path}

Path to temporary data for processing large queries.

!!! note "Note"
    The trailing slash is mandatory.

**Example**

``` xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## tmp\_policy {#tmp-policy}

Policy from [storage\_configuration](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) to store temporary files.

If not set, [tmp\_path](#tmp-path) is used, otherwise it is ignored.

!!! note "Note"
    - `move_factor` is ignored.
- `keep_free_space_bytes` is ignored.
- `max_data_part_size_bytes` is ignored.
- Уou must have exactly one volume in that policy.

## uncompressed\_cache\_size {#server-settings-uncompressed_cache_size}

Cache size (in bytes) for uncompressed data used by table engines from the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

There is one shared cache for the server. Memory is allocated on demand. The cache is used if the option [use\_uncompressed\_cache](../../operations/settings/settings.md#setting-use_uncompressed_cache) is enabled.

The uncompressed cache is advantageous for very short queries in individual cases.

**Example**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user\_files\_path {#server_configuration_parameters-user_files_path}

The directory with user files. Used in the table function [file()](../../sql-reference/table-functions/file.md).

**Example**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## users\_config {#users-config}

Path to the file that contains:

-   User configurations.
-   Access rights.
-   Settings profiles.
-   Quota settings.

**Example**

``` xml
<users_config>users.xml</users_config>
```

## zookeeper {#server-settings_zookeeper}

Contains settings that allow ClickHouse to interact with a [ZooKeeper](http://zookeeper.apache.org/) cluster.

ClickHouse uses ZooKeeper for storing metadata of replicas when using replicated tables. If replicated tables are not used, this section of parameters can be omitted.

This section contains the following parameters:

-   `node` — ZooKeeper endpoint. You can set multiple endpoints.

    For example:

<!-- -->

``` xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

      The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.

-   `session_timeout` — Maximum timeout for the client session in milliseconds.
-   `root` — The [znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) that is used as the root for znodes used by the ClickHouse server. Optional.
-   `identity` — User and password, that can be required by ZooKeeper to give access to requested znodes. Optional.

**Example configuration**

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

**See Also**

-   [Replication](../../engines/table-engines/mergetree-family/replication.md)
-   [ZooKeeper Programmer’s Guide](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

## use\_minimalistic\_part\_header\_in\_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

Storage method for data part headers in ZooKeeper.

This setting only applies to the `MergeTree` family. It can be specified:

-   Globally in the [merge\_tree](#server_configuration_parameters-merge_tree) section of the `config.xml` file.

    ClickHouse uses the setting for all the tables on the server. You can change the setting at any time. Existing tables change their behaviour when the setting changes.

-   For each table.

    When creating a table, specify the corresponding [engine setting](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). The behaviour of an existing table with this setting does not change, even if the global setting changes.

**Possible values**

-   0 — Functionality is turned off.
-   1 — Functionality is turned on.

If `use_minimalistic_part_header_in_zookeeper = 1`, then [replicated](../../engines/table-engines/mergetree-family/replication.md) tables store the headers of the data parts compactly using a single `znode`. If the table contains many columns, this storage method significantly reduces the volume of the data stored in Zookeeper.

!!! attention "Attention"
    After applying `use_minimalistic_part_header_in_zookeeper = 1`, you can’t downgrade the ClickHouse server to a version that doesn’t support this setting. Be careful when upgrading ClickHouse on servers in a cluster. Don’t upgrade all the servers at once. It is safer to test new versions of ClickHouse in a test environment, or on just a few servers of a cluster.

      Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.

**Default value:** 0.

## disable\_internal\_dns\_cache {#server-settings-disable-internal-dns-cache}

Disables the internal DNS cache. Recommended for operating ClickHouse in systems
with frequently changing infrastructure such as Kubernetes.

**Default value:** 0.

## dns\_cache\_update\_period {#server-settings-dns-cache-update-period}

The period of updating IP addresses stored in the ClickHouse internal DNS cache (in seconds).
The update is performed asynchronously, in a separate system thread.

**Default value**: 15.

**See also**

-   [background\_schedule\_pool\_size](../../operations/settings/settings.md#background_schedule_pool_size)

## access\_control\_path {#access_control_path}

Path to a folder where a ClickHouse server stores user and role configurations created by SQL commands.

Default value: `/var/lib/clickhouse/access/`.

**See also**

-   [Access Control and Account Management](../../operations/access-rights.md#access-control)

[Original article](https://clickhouse.tech/docs/en/operations/server_configuration_parameters/settings/) <!--hide-->
