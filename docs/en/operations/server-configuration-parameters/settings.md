---
sidebar_position: 57
sidebar_label: Server Settings
---

# Server Settings

## builtin_dictionaries_reload_interval {#builtin-dictionaries-reload-interval}

The interval in seconds before reloading built-in dictionaries.

ClickHouse reloads built-in dictionaries every x seconds. This makes it possible to edit dictionaries “on the fly” without restarting the server.

Default value: 3600.

**Example**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## compression {#server-settings-compression}

Data compression settings for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine tables.

:::warning
Don’t use it if you have just started using ClickHouse.
:::

Configuration template:

``` xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
      <level>...</level>
    </case>
    ...
</compression>
```

`<case>` fields:

-   `min_part_size` – The minimum size of a data part.
-   `min_part_size_ratio` – The ratio of the data part size to the table size.
-   `method` – Compression method. Acceptable values: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
-   `level` – Compression level. See [Codecs](../../sql-reference/statements/create/table.md#create-query-general-purpose-codecs).

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
        <level>1</level>
    </case>
</compression>
```

## encryption {#server-settings-encryption}

Configures a command to obtain a key to be used by [encryption codecs](../../sql-reference/statements/create/table.md#create-query-encryption-codecs). Key (or keys) should be written in environment variables or set in the configuration file.

Keys can be hex or string with a length equal to 16 bytes.

**Example**

Loading from config:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
Storing keys in the configuration file is not recommended. It isn't secure. You can move the keys into a separate config file on a secure disk and put a symlink to that config file to `config.d/` folder.
:::

Loading from config, when the key is in hex:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Loading key from the environment variable:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Here `current_key_id` sets the current key for encryption, and all specified keys can be used for decryption.

Each of these methods can be applied for multiple keys:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Here `current_key_id` shows current key for encryption.

Also, users can add nonce that must be 12 bytes long (by default encryption and decryption processes use nonce that consists of zero bytes):

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Or it can be set in hex:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Everything mentioned above can be applied for `aes_256_gcm_siv` (but the key must be 32 bytes long).


## custom_settings_prefixes {#custom_settings_prefixes}

List of prefixes for [custom settings](../../operations/settings/index.md#custom_settings). The prefixes must be separated with commas.

**Example**

```xml
<custom_settings_prefixes>custom_</custom_settings_prefixes>
```

**See Also**

-   [Custom settings](../../operations/settings/index.md#custom_settings)

## core_dump {#server_configuration_parameters-core_dump}

Configures soft limit for core dump file size.

Possible values:

-   Positive integer.

Default value: `1073741824` (1 GB).

:::note
Hard limit is configured via system tools
:::

**Example**

```xml
<core_dump>
    <size_limit>1073741824</size_limit>
</core_dump>
```

## database_atomic_delay_before_drop_table_sec {#database_atomic_delay_before_drop_table_sec}

Sets the delay before remove table data in seconds. If the query has `SYNC` modifier, this setting is ignored.

Default value: `480` (8 minute).

## database_catalog_unused_dir_hide_timeout_sec {#database_catalog_unused_dir_hide_timeout_sec}

Parameter of a task that cleans up garbage from `store/` directory.
If some subdirectory is not used by clickhouse-server and this directory was not modified for last
`database_catalog_unused_dir_hide_timeout_sec` seconds, the task will "hide" this directory by
removing all access rights. It also works for directories that clickhouse-server does not
expect to see inside `store/`. Zero means "immediately".

Default value: `3600` (1 hour).

## database_catalog_unused_dir_rm_timeout_sec {#database_catalog_unused_dir_rm_timeout_sec}

Parameter of a task that cleans up garbage from `store/` directory.
If some subdirectory is not used by clickhouse-server and it was previousely "hidden"
(see [database_catalog_unused_dir_hide_timeout_sec](../../operations/server-configuration-parameters/settings.md#database_catalog_unused_dir_hide_timeout_sec))
and this directory was not modified for last
`database_catalog_unused_dir_rm_timeout_sec` seconds, the task will remove this directory.
It also works for directories that clickhouse-server does not
expect to see inside `store/`. Zero means "never".

Default value: `2592000` (30 days).

## database_catalog_unused_dir_cleanup_period_sec {#database_catalog_unused_dir_cleanup_period_sec}

Parameter of a task that cleans up garbage from `store/` directory.
Sets scheduling period of the task. Zero means "never".

Default value: `86400` (1 day).

## default_database {#default-database}

The default database.

To get a list of databases, use the [SHOW DATABASES](../../sql-reference/statements/show.md#show-databases) query.

**Example**

``` xml
<default_database>default</default_database>
```

## default_profile {#default-profile}

Default settings profile.

Settings profiles are located in the file specified in the parameter `user_config`.

**Example**

``` xml
<default_profile>default</default_profile>
```

## default_replica_path {#default_replica_path}

The path to the table in ZooKeeper.

**Example**

``` xml
<default_replica_path>/clickhouse/tables/{uuid}/{shard}</default_replica_path>
```
## default_replica_name {#default_replica_name}

 The replica name in ZooKeeper.

**Example**

``` xml
<default_replica_name>{replica}</default_replica_name>
```

## dictionaries_config {#server_configuration_parameters-dictionaries_config}

The path to the config file for external dictionaries.

Path:

-   Specify the absolute path or the path relative to the server config file.
-   The path can contain wildcards \* and ?.

See also “[External dictionaries](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)”.

**Example**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## user_defined_executable_functions_config {#server_configuration_parameters-user_defined_executable_functions_config}

The path to the config file for executable user defined functions.

Path:

-   Specify the absolute path or the path relative to the server config file.
-   The path can contain wildcards \* and ?.

See also “[Executable User Defined Functions](../../sql-reference/functions/index.md#executable-user-defined-functions).”.

**Example**

``` xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

## dictionaries_lazy_load {#server_configuration_parameters-dictionaries_lazy_load}

Lazy loading of dictionaries.

If `true`, then each dictionary is created on first use. If dictionary creation failed, the function that was using the dictionary throws an exception.

If `false`, all dictionaries are created when the server starts, if the dictionary or dictionaries are created too long or are created with errors, then the server boots without of these dictionaries and continues to try to create these dictionaries.

The default is `true`.

**Example**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format_schema_path {#server_configuration_parameters-format_schema_path}

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
-   root_path – Prefix for keys.
-   metrics – Sending data from the [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics) table.
-   events – Sending deltas data accumulated for the time period from the [system.events](../../operations/system-tables/events.md#system_tables-events) table.
-   events_cumulative – Sending cumulative data from the [system.events](../../operations/system-tables/events.md#system_tables-events) table.
-   asynchronous_metrics – Sending data from the [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) table.

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

## graphite_rollup {#server_configuration_parameters-graphite-rollup}

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

## http_port/https_port {#http-porthttps-port}

The port for connecting to the server over HTTP(s).

If `https_port` is specified, [openSSL](#server_configuration_parameters-openssl) must be configured.

If `http_port` is specified, the OpenSSL configuration is ignored even if it is set.

**Example**

``` xml
<https_port>9999</https_port>
```

## http_server_default_response {#server_configuration_parameters-http_server_default_response}

The page that is shown by default when you access the ClickHouse HTTP(s) server.
The default value is “Ok.” (with a line feed at the end)

**Example**

Opens `https://tabix.io/` when accessing `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```
## hsts_max_age  {#hsts-max-age}

Expired time for HSTS in seconds. The default value is 0 means clickhouse disabled HSTS. If you set a positive number, the HSTS will be enabled and the max-age is the number you set.

**Example**

```xml
<hsts_max_age>600000</hsts_max_age>
```

## include_from {#server_configuration_parameters-include_from}

The path to the file with substitutions.

For more information, see the section “[Configuration files](../../operations/configuration-files.md#configuration_files)”.

**Example**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## interserver_listen_host {#interserver-listen-host}

Restriction on hosts that can exchange data between ClickHouse servers.
The default value equals to `listen_host` setting.

Examples:

``` xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

## interserver_http_port {#interserver-http-port}

Port for exchanging data between ClickHouse servers.

**Example**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## interserver_http_host {#interserver-http-host}

The hostname that can be used by other servers to access this server.

If omitted, it is defined in the same way as the `hostname-f` command.

Useful for breaking away from a specific network interface.

**Example**

``` xml
<interserver_http_host>example.clickhouse.com</interserver_http_host>
```

## interserver_https_port {#interserver-https-port}

Port for exchanging data between ClickHouse servers over `HTTPS`.

**Example**

``` xml
<interserver_https_port>9010</interserver_https_port>
```

## interserver_https_host {#interserver-https-host}

Similar to `interserver_http_host`, except that this hostname can be used by other servers to access this server over `HTTPS`.

**Example**

``` xml
<interserver_https_host>example.clickhouse.com</interserver_https_host>
```

## interserver_http_credentials {#server-settings-interserver-http-credentials}

A username and a password used to connect to other servers during [replication](../../engines/table-engines/mergetree-family/replication.md). Also the server authenticates other replicas using these credentials. So, `interserver_http_credentials` must be the same for all replicas in a cluster.

By default, if `interserver_http_credentials` section is omitted, authentication is not used during replication.

:::note
`interserver_http_credentials` settings do not relate to a ClickHouse client credentials [configuration](../../interfaces/cli.md#configuration_files).
:::

:::note
These credentials are common for replication via `HTTP` and `HTTPS`.
:::

The section contains the following parameters:

-   `user` — Username.
-   `password` — Password.
-   `allow_empty` — If `true`, then other replicas are allowed to connect without authentication even if credentials are set. If `false`, then connections without authentication are refused. Default value: `false`.
-   `old` — Contains old `user` and `password` used during credential rotation. Several `old` sections can be specified.

**Credentials Rotation**

ClickHouse supports dynamic interserver credentials rotation without stopping all replicas at the same time to update their configuration. Credentials can be changed in several steps.

To enable authentication, set `interserver_http_credentials.allow_empty` to `true` and add credentials. This allows connections with authentication and without it.

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

After configuring all replicas set `allow_empty` to `false` or remove this setting. It makes authentication with new credentials mandatory.

To change existing credentials, move the username and the password to `interserver_http_credentials.old` section and update `user` and `password` with new values. At this point the server uses new credentials to connect to other replicas and accepts connections with either new or old credentials.

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
    <old>
        <user>admin</user>
        <password>111</password>
    </old>
    <old>
        <user>temp</user>
        <password>000</password>
    </old>
</interserver_http_credentials>
```

When new credentials are applied to all replicas, old credentials may be removed.

## keep_alive_timeout {#keep-alive-timeout}

The number of seconds that ClickHouse waits for incoming requests before closing the connection. Defaults to 10 seconds.

**Example**

``` xml
<keep_alive_timeout>10</keep_alive_timeout>
```

## listen_host {#server_configuration_parameters-listen_host}

Restriction on hosts that requests can come from. If you want the server to answer all of them, specify `::`.

Examples:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## listen_backlog {#server_configuration_parameters-listen_backlog}

Backlog (queue size of pending connections) of the listen socket.

Default value: `4096` (as in linux [5.4+](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=19f92a030ca6d772ab44b22ee6a01378a8cb32d4)).

Usually this value does not need to be changed, since:
-  default value is large enough,
-  and for accepting client's connections server has separate thread.

So even if you have `TcpExtListenOverflows` (from `nstat`) non zero and this counter grows for ClickHouse server it does not mean that this value need to be increased, since:
-  usually if 4096 is not enough it shows some internal ClickHouse scaling issue, so it is better to report an issue.
-  and it does not mean that the server can handle more connections later (and even if it could, by that moment clients may be gone or disconnected).

Examples:

``` xml
<listen_backlog>4096</listen_backlog>
```

## logger {#server_configuration_parameters-logger}

Logging settings.

Keys:

-   `level` – Logging level. Acceptable values: `trace`, `debug`, `information`, `warning`, `error`.
-   `log` – The log file. Contains all the entries according to `level`.
-   `errorlog` – Error log file.
-   `size` – Size of the file. Applies to `log` and `errorlog`. Once the file reaches `size`, ClickHouse archives and renames it, and creates a new log file in its place.
-   `count` – The number of archived log files that ClickHouse stores.
-   `console` – Send `log` and `errorlog` to the console instead of file. To enable, set to `1` or `true`.

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

Writing to the console can be configured. Config example:

``` xml
<logger>
    <level>information</level>
    <console>1</console>
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

Keys for syslog:

-   use_syslog — Required setting if you want to write to the syslog.
-   address — The host\[:port\] of syslogd. If omitted, the local daemon is used.
-   hostname — Optional. The name of the host that logs are sent from.
-   facility — [The syslog facility keyword](https://en.wikipedia.org/wiki/Syslog#Facility) in uppercase letters with the “LOG_” prefix: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`, and so on).
    Default value: `LOG_USER` if `address` is specified, `LOG_DAEMON` otherwise.
-   format – Message format. Possible values: `bsd` and `syslog.`

## send_crash_reports {#server_configuration_parameters-send_crash_reports}

Settings for opt-in sending crash reports to the ClickHouse core developers team via [Sentry](https://sentry.io).
Enabling it, especially in pre-production environments, is highly appreciated.

The server will need access to the public Internet via IPv4 (at the time of writing IPv6 is not supported by Sentry) for this feature to be functioning properly.

Keys:

-   `enabled` – Boolean flag to enable the feature, `false` by default. Set to `true` to allow sending crash reports.
-   `endpoint` – You can override the Sentry endpoint URL for sending crash reports. It can be either a separate Sentry account or your self-hosted Sentry instance. Use the [Sentry DSN](https://docs.sentry.io/error-reporting/quickstart/?platform=native#configure-the-sdk) syntax.
-   `anonymize` - Avoid attaching the server hostname to the crash report.
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

For more information, see the section [Creating replicated tables](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables).

**Example**

``` xml
<macros incl="macros" optional="true" />
```

## mark_cache_size {#server-mark-cache-size}

Approximate size (in bytes) of the cache of marks used by table engines of the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) family.

The cache is shared for the server and memory is allocated as needed.

**Example**

``` xml
<mark_cache_size>5368709120</mark_cache_size>
```

## max_server_memory_usage {#max_server_memory_usage}

Limits total RAM usage by the ClickHouse server.

Possible values:

-   Positive integer.
-   0 — Auto.

Default value: `0`.

**Additional Info**

The default `max_server_memory_usage` value is calculated as `memory_amount * max_server_memory_usage_to_ram_ratio`.

**See also**

-   [max_memory_usage](../../operations/settings/query-complexity.md#settings_max_memory_usage)
-   [max_server_memory_usage_to_ram_ratio](#max_server_memory_usage_to_ram_ratio)

## max_server_memory_usage_to_ram_ratio {#max_server_memory_usage_to_ram_ratio}

Defines the fraction of total physical RAM amount, available to the ClickHouse server. If the server tries to utilize more, the memory is cut down to the appropriate amount.

Possible values:

-   Positive double.
-   0 — The ClickHouse server can use all available RAM.

Default value: `0.9`.

**Usage**

On hosts with low RAM and swap, you possibly need setting `max_server_memory_usage_to_ram_ratio` larger than 1.

**Example**

``` xml
<max_server_memory_usage_to_ram_ratio>0.9</max_server_memory_usage_to_ram_ratio>
```

**See Also**

-   [max_server_memory_usage](#max_server_memory_usage)

## concurrent_threads_soft_limit {#concurrent_threads_soft_limit}
The maximum number of query processing threads, excluding threads for retrieving data from remote servers, allowed to run all queries. This is not a hard limit. In case if the limit is reached the query will still get one thread to run.

Possible values:
-   Positive integer.
-   0 — No limit.
-   -1 — The parameter is initialized by number of logical cores multiplies by 3. Which is a good heuristic for CPU-bound tasks.

Default value: `0`.

## max_concurrent_queries {#max-concurrent-queries}

The maximum number of simultaneously processed queries.
Note that other limits also apply: [max_concurrent_insert_queries](#max-concurrent-insert-queries), [max_concurrent_select_queries](#max-concurrent-select-queries), [max_concurrent_queries_for_user](#max-concurrent-queries-for-user), [max_concurrent_queries_for_all_users](#max-concurrent-queries-for-all-users).

:::note
These settings can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::

Possible values:

-   Positive integer.
-   0 — No limit.

Default value: `100`.

**Example**

``` xml
<max_concurrent_queries>200</max_concurrent_queries>
```

## max_concurrent_insert_queries {#max-concurrent-insert-queries}

The maximum number of simultaneously processed `INSERT` queries.

:::note
These settings can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::

Possible values:

-   Positive integer.
-   0 — No limit.

Default value: `0`.

**Example**

``` xml
<max_concurrent_insert_queries>100</max_concurrent_insert_queries>
```

## max_concurrent_select_queries {#max-concurrent-select-queries}

The maximum number of simultaneously processed `SELECT` queries.

:::note
These settings can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::

Possible values:

-   Positive integer.
-   0 — No limit.

Default value: `0`.

**Example**

``` xml
<max_concurrent_select_queries>100</max_concurrent_select_queries>
```

## max_concurrent_queries_for_user {#max-concurrent-queries-for-user}

The maximum number of simultaneously processed queries related to MergeTree table per user.

Possible values:

-   Positive integer.
-   0 — No limit.

Default value: `0`.

**Example**

``` xml
<max_concurrent_queries_for_user>5</max_concurrent_queries_for_user>
```

## max_concurrent_queries_for_all_users {#max-concurrent-queries-for-all-users}

Throw exception if the value of this setting is less or equal than the current number of simultaneously processed queries.

Example: `max_concurrent_queries_for_all_users` can be set to 99 for all users and database administrator can set it to 100 for itself to run queries for investigation even when the server is overloaded.

Modifying the setting for one query or user does not affect other queries.

Possible values:

-   Positive integer.
-   0 — No limit.

Default value: `0`.

**Example**

``` xml
<max_concurrent_queries_for_all_users>99</max_concurrent_queries_for_all_users>
```

**See Also**

-   [max_concurrent_queries](#max-concurrent-queries)

## max_connections {#max-connections}

The maximum number of inbound connections.

**Example**

``` xml
<max_connections>4096</max_connections>
```

## max_open_files {#max-open-files}

The maximum number of open files.

By default: `maximum`.

We recommend using this option in Mac OS X since the `getrlimit()` function returns an incorrect value.

**Example**

``` xml
<max_open_files>262144</max_open_files>
```

## max_table_size_to_drop {#max-table-size-to-drop}

Restriction on deleting tables.

If the size of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `max_table_size_to_drop` (in bytes), you can’t delete it using a DROP query.

If you still need to delete the table without restarting the ClickHouse server, create the `<clickhouse-path>/flags/force_drop_table` file and run the DROP query.

Default value: 50 GB.

The value 0 means that you can delete all tables without any restrictions.

**Example**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## max_thread_pool_size {#max-thread-pool-size}

ClickHouse uses threads from the Global Thread pool to process queries. If there is no idle thread to process a query, then a new thread is created in the pool. `max_thread_pool_size` limits the maximum number of threads in the pool.

Possible values:

-   Positive integer.

Default value: `10000`.

**Example**

``` xml
<max_thread_pool_size>12000</max_thread_pool_size>
```

## max_thread_pool_free_size {#max-thread-pool-free-size}

If the number of **idle** threads in the Global Thread pool is greater than `max_thread_pool_free_size`, then ClickHouse releases resources occupied by some threads and the pool size is decreased. Threads can be created again if necessary.

Possible values:

-   Positive integer.

Default value: `1000`.

**Example**

``` xml
<max_thread_pool_free_size>1200</max_thread_pool_free_size>
```

## thread_pool_queue_size {#thread-pool-queue-size}

The maximum number of jobs that can be scheduled on the Global Thread pool. Increasing queue size leads to larger memory usage. It is recommended to keep this value equal to [max_thread_pool_size](#max-thread-pool-size).

Possible values:

-   Positive integer.

Default value: `10000`.

**Example**

``` xml
<thread_pool_queue_size>12000</thread_pool_queue_size>
```

## background_pool_size {#background_pool_size}

Sets the number of threads performing background merges and mutations for tables with MergeTree engines. This setting is also could be applied  at server startup from the `default` profile configuration for backward compatibility at the ClickHouse server start. You can only increase the number of threads at runtime. To lower the number of threads you have to restart the server. By adjusting this setting, you manage CPU and disk load. Smaller pool size utilizes less CPU and disk resources, but background processes advance slower which might eventually impact query performance.

Before changing it, please also take a look at related MergeTree settings, such as `number_of_free_entries_in_pool_to_lower_max_size_of_merge` and `number_of_free_entries_in_pool_to_execute_mutation`.

Possible values:

-   Any positive integer.

Default value: 16.

**Example**

```xml
<background_pool_size>16</background_pool_size>
```

## background_merges_mutations_concurrency_ratio {#background_merges_mutations_concurrency_ratio}

Sets a ratio between the number of threads and the number of background merges and mutations that can be executed concurrently. For example if the ratio equals to 2 and
`background_pool_size` is set to 16 then ClickHouse can execute 32 background merges concurrently. This is possible, because background operation could be suspended and postponed. This is needed to give small merges more execution priority. You can only increase this ratio at runtime. To lower it you have to restart the server.
The same as for `background_pool_size` setting `background_merges_mutations_concurrency_ratio` could be applied from the `default` profile for backward compatibility.

Possible values:

-   Any positive integer.

Default value: 2.

**Example**

```xml
<background_merges_mutations_concurrency_ratio>3</background_pbackground_merges_mutations_concurrency_ratio>
```

## background_move_pool_size {#background_move_pool_size}

Sets the number of threads performing background moves for tables with MergeTree engines. Could be increased at runtime and could be applied at server startup from the `default` profile for backward compatibility.

Possible values:

-   Any positive integer.

Default value: 8.

**Example**

```xml
<background_move_pool_size>36</background_move_pool_size>
```

## background_fetches_pool_size {#background_fetches_pool_size}

Sets the number of threads performing background fetches for tables with ReplicatedMergeTree engines. Could be increased at runtime and could be applied at server startup from the `default` profile for backward compatibility.

Possible values:

-   Any positive integer.

Default value: 8.

**Example**

```xml
<background_fetches_pool_size>36</background_fetches_pool_size>
```

## background_common_pool_size {#background_common_pool_size}

Sets the number of threads performing background non-specialized operations like cleaning the filesystem etc. for tables with MergeTree engines. Could be increased at runtime and could be applied at server startup from the `default` profile for backward compatibility.

Possible values:

-   Any positive integer.

Default value: 8.

**Example**

```xml
<background_common_pool_size>36</background_common_pool_size>
```



## merge_tree {#server_configuration_parameters-merge_tree}

Fine tuning for tables in the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

For more information, see the MergeTreeSettings.h header file.

**Example**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## metric_log {#metric_log}

It is enabled by default. If it`s not, you can do this manually.

**Enabling**

To manually turn on metrics history collection [`system.metric_log`](../../operations/system-tables/metric_log.md), create `/etc/clickhouse-server/config.d/metric_log.xml` with the following content:

``` xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
    </metric_log>
</clickhouse>
```

**Disabling**

To disable `metric_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_metric_log.xml` with the following content:

``` xml
<clickhouse>
<metric_log remove="1" />
</clickhouse>
```

## replicated_merge_tree {#server_configuration_parameters-replicated_merge_tree}

Fine tuning for tables in the [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

This setting has a higher priority.

For more information, see the MergeTreeSettings.h header file.

**Example**

``` xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

## openSSL {#server_configuration_parameters-openssl}

SSL client/server configuration.

Support for SSL is provided by the `libpoco` library. The available configuration options are explained in [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h). Default values can be found in [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp).

Keys for server/client settings:

-   privateKeyFile – The path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.
-   certificateFile – The path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` contains the certificate.
-   caConfig (default: none) – The path to the file or directory that contains trusted CA certificates. If this points to a file, it must be in PEM format and can contain several CA certificates. If this points to a directory, it must contain one .pem file per CA certificate. The filenames are looked up by the CA subject name hash value. Details can be found in the man page of [SSL_CTX_load_verify_locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html).
-   verificationMode (default: relaxed) – The method for checking the node’s certificates. Details are in the description of the [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) class. Possible values: `none`, `relaxed`, `strict`, `once`.
-   verificationDepth (default: 9) – The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.
-   loadDefaultCAFile (default: true) – Wether built-in CA certificates for OpenSSL will be used. ClickHouse assumes that builtin CA certificates are in the file `/etc/ssl/cert.pem` (resp. the directory `/etc/ssl/certs`) or in file (resp. directory) specified by the environment variable `SSL_CERT_FILE` (resp. `SSL_CERT_DIR`).
-   cipherList (default: `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`) - Supported OpenSSL encryptions.
-   cacheSessions (default: false) – Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. Acceptable values: `true`, `false`.
-   sessionIdContext (default: `${application.name}`) – A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. This parameter is always recommended since it helps avoid problems both if the server caches the session and if the client requested caching. Default value: `${application.name}`.
-   sessionCacheSize (default: [1024\*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978)) – The maximum number of sessions that the server caches. A value of 0 means unlimited sessions.
-   sessionTimeout (default: [2h](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1926)) – Time for caching the session on the server.
-   extendedVerification (default: false) – If enabled, verify that the certificate CN or SAN matches the peer hostname.
-   requireTLSv1 (default: false) – Require a TLSv1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1_1 (default: false) – Require a TLSv1.1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1_2 (default: false) – Require a TLSv1.2 connection. Acceptable values: `true`, `false`.
-   fips (default: false) – Activates OpenSSL FIPS mode. Supported if the library’s OpenSSL version supports FIPS.
-   privateKeyPassphraseHandler (default: `KeyConsoleHandler`)– Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
-   invalidCertificateHandler (default: `ConsoleCertificateHandler`) – Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>` .
-   disableProtocols (default: "") – Protocols that are not allowed to use.
-   preferServerCiphers (default: false) – Preferred server ciphers on the client.

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

## part_log {#server_configuration_parameters-part-log}

Logging events that are associated with [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). For instance, adding or merging data. You can use the log to simulate merge algorithms and compare their characteristics. You can visualize the merge process.

Queries are logged in the [system.part_log](../../operations/system-tables/part_log.md#system_tables-part-log) table, not in a separate file. You can configure the name of this table in the `table` parameter (see below).

Use the following parameters to configure logging:

-   `database` – Name of the database.
-   `table` – Name of the system table.
-   `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
-   `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` defined.
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

:::note
The trailing slash is mandatory.
:::

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
-   `asynchronous_metrics` – Flag that sets to expose current metrics values from the [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) table.

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

## query_log {#server_configuration_parameters-query-log}

Setting for logging queries received with the [log_queries=1](../../operations/settings/settings.md) setting.

Queries are logged in the [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

Use the following parameters to configure logging:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
-   `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` defined.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

If the table does not exist, ClickHouse will create it. If the structure of the query log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

## query_thread_log {#server_configuration_parameters-query_thread_log}

Setting for logging threads of queries received with the [log_query_threads=1](../../operations/settings/settings.md#settings-log-query-threads) setting.

Queries are logged in the [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

Use the following parameters to configure logging:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
-   `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` defined.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

If the table does not exist, ClickHouse will create it. If the structure of the query thread log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_thread_log>
```

## query_views_log {#server_configuration_parameters-query_views_log}

Setting for logging views (live, materialized etc) dependant of queries received with the [log_query_views=1](../../operations/settings/settings.md#settings-log-query-views) setting.

Queries are logged in the [system.query_views_log](../../operations/system-tables/query_views_log.md#system_tables-query_views_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

Use the following parameters to configure logging:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
-   `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` defined.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

If the table does not exist, ClickHouse will create it. If the structure of the query views log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

``` xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_views_log>
```

## text_log {#server_configuration_parameters-text_log}

Settings for the [text_log](../../operations/system-tables/text_log.md#system_tables-text_log) system table for logging text messages.

Parameters:

-   `level` — Maximum Message Level (by default `Trace`) which will be stored in a table.
-   `database` — Database name.
-   `table` — Table name.
-   `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
-   `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` defined.
-   `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.

**Example**
```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```


## trace_log {#server_configuration_parameters-trace_log}

Settings for the [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) system table operation.

Parameters:

-   `database` — Database for storing a table.
-   `table` — Table name.
-   `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
-   `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/index.md) for a system table. Can't be used if `partition_by` defined.
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

## query_masking_rules {#query-masking-rules}

Regexp-based rules, which will be applied to queries as well as all log messages before storing them in server logs,
`system.query_log`, `system.text_log`, `system.processes` tables, and in logs sent to the client. That allows preventing
sensitive data leakage from SQL queries (like names, emails, personal identifiers or credit card numbers) to logs.

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

## remote_servers {#server-settings-remote-servers}

Configuration of clusters used by the [Distributed](../../engines/table-engines/special/distributed.md) table engine and by the `cluster` table function.

**Example**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

For the value of the `incl` attribute, see the section “[Configuration files](../../operations/configuration-files.md#configuration_files)”.

**See Also**

-   [skip_unavailable_shards](../../operations/settings/settings.md#settings-skip_unavailable_shards)

## timezone {#server_configuration_parameters-timezone}

The server’s time zone.

Specified as an IANA identifier for the UTC timezone or geographic location (for example, Africa/Abidjan).

The time zone is necessary for conversions between String and DateTime formats when DateTime fields are output to text format (printed on the screen or in a file), and when getting DateTime from a string. Besides, the time zone is used in functions that work with the time and date if they didn’t receive the time zone in the input parameters.

**Example**

``` xml
<timezone>Asia/Istanbul</timezone>
```

## tcp_port {#server_configuration_parameters-tcp_port}

Port for communicating with clients over the TCP protocol.

**Example**

``` xml
<tcp_port>9000</tcp_port>
```

## tcp_port_secure {#server_configuration_parameters-tcp_port_secure}

TCP port for secure communication with clients. Use it with [OpenSSL](#server_configuration_parameters-openssl) settings.

**Possible values**

Positive integer.

**Default value**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql_port {#server_configuration_parameters-mysql_port}

Port for communicating with clients over MySQL protocol.

**Possible values**

Positive integer.

Example

``` xml
<mysql_port>9004</mysql_port>
```

## postgresql_port {#server_configuration_parameters-postgresql_port}

Port for communicating with clients over PostgreSQL protocol.

**Possible values**

Positive integer.

Example

``` xml
<postgresql_port>9005</postgresql_port>
```

## tmp_path {#tmp-path}

Path to temporary data for processing large queries.

:::note
The trailing slash is mandatory.
:::

**Example**

``` xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## tmp_policy {#tmp-policy}

Policy from [storage_configuration](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) to store temporary files.

If not set, [tmp_path](#tmp-path) is used, otherwise it is ignored.

:::note
- `move_factor` is ignored.
- `keep_free_space_bytes` is ignored.
- `max_data_part_size_bytes` is ignored.
- Уou must have exactly one volume in that policy.
:::

## uncompressed_cache_size {#server-settings-uncompressed_cache_size}

Cache size (in bytes) for uncompressed data used by table engines from the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

There is one shared cache for the server. Memory is allocated on demand. The cache is used if the option [use_uncompressed_cache](../../operations/settings/settings.md#setting-use_uncompressed_cache) is enabled.

The uncompressed cache is advantageous for very short queries in individual cases.

**Example**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## user_files_path {#server_configuration_parameters-user_files_path}

The directory with user files. Used in the table function [file()](../../sql-reference/table-functions/file.md).

**Example**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## user_scripts_path {#server_configuration_parameters-user_scripts_path}

The directory with user scripts files. Used for Executable user defined functions [Executable User Defined Functions](../../sql-reference/functions/index.md#executable-user-defined-functions).

**Example**

``` xml
<user_scripts_path>/var/lib/clickhouse/user_scripts/</user_scripts_path>
```

## user_defined_path {#server_configuration_parameters-user_defined_path}

The directory with user defined files. Used for SQL user defined functions [SQL User Defined Functions](../../sql-reference/functions/index.md#user-defined-functions).

**Example**

``` xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

## users_config {#users-config}

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

- `session_timeout_ms` — Maximum timeout for the client session in milliseconds.
- `operation_timeout_ms` — Maximum timeout for one operation in milliseconds.
- `root` — The [znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) that is used as the root for znodes used by the ClickHouse server. Optional.
- `identity` — User and password, that can be required by ZooKeeper to give access to requested znodes. Optional.

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
-   [Optional secured communication between ClickHouse and Zookeeper](../ssl-zookeeper.md#secured-communication-with-zookeeper)

## use_minimalistic_part_header_in_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

Storage method for data part headers in ZooKeeper.

This setting only applies to the `MergeTree` family. It can be specified:

-   Globally in the [merge_tree](#server_configuration_parameters-merge_tree) section of the `config.xml` file.

    ClickHouse uses the setting for all the tables on the server. You can change the setting at any time. Existing tables change their behaviour when the setting changes.

-   For each table.

    When creating a table, specify the corresponding [engine setting](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). The behaviour of an existing table with this setting does not change, even if the global setting changes.

**Possible values**

-   0 — Functionality is turned off.
-   1 — Functionality is turned on.

If `use_minimalistic_part_header_in_zookeeper = 1`, then [replicated](../../engines/table-engines/mergetree-family/replication.md) tables store the headers of the data parts compactly using a single `znode`. If the table contains many columns, this storage method significantly reduces the volume of the data stored in Zookeeper.

:::note
After applying `use_minimalistic_part_header_in_zookeeper = 1`, you can’t downgrade the ClickHouse server to a version that does not support this setting. Be careful when upgrading ClickHouse on servers in a cluster. Don’t upgrade all the servers at once. It is safer to test new versions of ClickHouse in a test environment, or on just a few servers of a cluster.

Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.
:::

**Default value:** 0.

## disable_internal_dns_cache {#server-settings-disable-internal-dns-cache}

Disables the internal DNS cache. Recommended for operating ClickHouse in systems
with frequently changing infrastructure such as Kubernetes.

**Default value:** 0.

## dns_cache_update_period {#server-settings-dns-cache-update-period}

The period of updating IP addresses stored in the ClickHouse internal DNS cache (in seconds).
The update is performed asynchronously, in a separate system thread.

**Default value**: 15.

**See also**

-   [background_schedule_pool_size](../../operations/settings/settings.md#background_schedule_pool_size)

## distributed_ddl {#server-settings-distributed_ddl}

Manage executing [distributed ddl queries](../../sql-reference/distributed-ddl.md)  (CREATE, DROP, ALTER, RENAME) on cluster.
Works only if [ZooKeeper](#server-settings_zookeeper) is enabled.

**Example**

```xml
<distributed_ddl>
    <!-- Path in ZooKeeper to queue with DDL queries -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- Settings from this profile will be used to execute DDL queries -->
    <profile>default</profile>

    <!-- Controls how much ON CLUSTER queries can be run simultaneously. -->
    <pool_size>1</pool_size>

    <!--
         Cleanup settings (active tasks will not be removed)
    -->

    <!-- Controls task TTL (default 1 week) -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- Controls how often cleanup should be performed (in seconds) -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- Controls how many tasks could be in the queue -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

## access_control_path {#access_control_path}

Path to a folder where a ClickHouse server stores user and role configurations created by SQL commands.

Default value: `/var/lib/clickhouse/access/`.

**See also**

- [Access Control and Account Management](../../operations/access-rights.md#access-control)

## user_directories {#user_directories}

Section of the configuration file that contains settings:
-   Path to configuration file with predefined users.
-   Path to folder where users created by SQL commands are stored.
-   ZooKeeper node path where users created by SQL commands are stored and replicated (experimental).

If this section is specified, the path from [users_config](../../operations/server-configuration-parameters/settings.md#users-config) and [access_control_path](../../operations/server-configuration-parameters/settings.md#access_control_path) won't be used.

The `user_directories` section can contain any number of items, the order of the items means their precedence (the higher the item the higher the precedence).

**Examples**

``` xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

Users, roles, row policies, quotas, and profiles can be also stored in ZooKeeper:

``` xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

You can also define sections `memory` — means storing information only in memory, without writing to disk, and `ldap` — means storing information on an LDAP server.

To add an LDAP server as a remote user directory of users that are not defined locally, define a single `ldap` section with a following parameters:
-   `server` — one of LDAP server names defined in `ldap_servers` config section. This parameter is mandatory and cannot be empty.
-   `roles` — section with a list of locally defined roles that will be assigned to each user retrieved from the LDAP server. If no roles are specified, user will not be able to perform any actions after authentication. If any of the listed roles is not defined locally at the time of authentication, the authentication attempt will fail as if the provided password was incorrect.

**Example**

``` xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

## total_memory_profiler_step {#total-memory-profiler-step}

Sets the memory size (in bytes) for a stack trace at every peak allocation step. The data is stored in the [system.trace_log](../../operations/system-tables/trace_log.md) system table with `query_id` equal to an empty string.

Possible values:

-   Positive integer.

Default value: `4194304`.

## total_memory_tracker_sample_probability {#total-memory-tracker-sample-probability}

Allows to collect random allocations and deallocations and writes them in the [system.trace_log](../../operations/system-tables/trace_log.md) system table with `trace_type` equal to a `MemorySample` with the specified probability. The probability is for every allocation or deallocations, regardless of the size of the allocation. Note that sampling happens only when the amount of untracked memory exceeds the untracked memory limit (default value is `4` MiB). It can be lowered if [total_memory_profiler_step](#total-memory-profiler-step) is lowered. You can set `total_memory_profiler_step` equal to `1` for extra fine-grained sampling.

Possible values:

-   Positive integer.
-   0 — Writing of random allocations and deallocations in the `system.trace_log` system table is disabled.

Default value: `0`.

## mmap_cache_size {#mmap-cache-size}

Sets the cache size (in bytes) for mapped files. This setting allows to avoid frequent open/[mmap/munmap](https://en.wikipedia.org/wiki/Mmap)/close calls (which are very expensive due to consequent page faults) and to reuse mappings from several threads and queries. The setting value is the number of mapped regions (usually equal to the number of mapped files). The amount of data in mapped files can be monitored in [system.metrics](../../operations/system-tables/metrics.md), [system.metric_log](../../operations/system-tables/metric_log.md) system tables by the `MMappedFiles` and `MMappedFileBytes` metrics, in [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md), [system.asynchronous_metrics_log](../../operations/system-tables/asynchronous_metric_log.md) by the `MMapCacheCells` metric, and also in [system.events](../../operations/system-tables/events.md), [system.processes](../../operations/system-tables/processes.md), [system.query_log](../../operations/system-tables/query_log.md), [system.query_thread_log](../../operations/system-tables/query_thread_log.md), [system.query_views_log](../../operations/system-tables/query_views_log.md) by the `CreatedReadBufferMMap`, `CreatedReadBufferMMapFailed`, `MMappedFileCacheHits`, `MMappedFileCacheMisses` events. Note that the amount of data in mapped files does not consume memory directly and is not accounted in query or server memory usage — because this memory can be discarded similar to OS page cache. The cache is dropped (the files are closed) automatically on the removal of old parts in tables of the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) family, also it can be dropped manually by the `SYSTEM DROP MMAP CACHE` query.

Possible values:

-   Positive integer.

Default value: `1000`.

## compiled_expression_cache_size {#compiled-expression-cache-size}

Sets the cache size (in bytes) for [compiled expressions](../../operations/caches.md).

Possible values:

-   Positive integer.

Default value: `134217728`.

## compiled_expression_cache_elements_size {#compiled_expression_cache_elements_size}

Sets the cache size (in elements) for [compiled expressions](../../operations/caches.md).

Possible values:

-   Positive integer.

Default value: `10000`.
