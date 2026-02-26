## asynchronous_metric_log {#asynchronous_metric_log}

Enabled by default on ClickHouse Cloud deployments.

If the setting is not enabled by default on your environment, depending on how ClickHouse was installed, you can follow the instruction below to enable or disable it.

**Enabling**

To manually turn on asynchronous metric logs history collection [`system.asynchronous_metric_log`](../../operations/system-tables/asynchronous_metric_log.md), create `/etc/clickhouse-server/config.d/asynchronous_metric_log.xml` with the following content:

```xml
<clickhouse>
     <asynchronous_metric_log>
        <database>system</database>
        <table>asynchronous_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </asynchronous_metric_log>
</clickhouse>
```

**Disabling**

To disable `asynchronous_metric_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_asynchronous_metric_log.xml` with the following content:

```xml
<clickhouse><asynchronous_metric_log remove="1" /></clickhouse>
```

<SystemLogParameters/>

## auth_use_forwarded_address {#auth_use_forwarded_address}

Use originating address for authentication for clients connected through proxy.

:::note
This setting should be used with extra caution since forwarded addresses can be easily spoofed - servers accepting such authentication should not be accessed directly but rather exclusively through a trusted proxy.
:::

## backups {#backups}

Settings for backups, used when executing the [`BACKUP` and `RESTORE`](/operations/backup/overview) statements.

The following settings can be configured by sub-tags:

<!-- SQL
WITH settings AS (
  SELECT arrayJoin([
    ('allow_concurrent_backups', 'Bool','Determines whether multiple backup operations can run concurrently on the same host.', 'true'),
    ('allow_concurrent_restores', 'Bool', 'Determines whether multiple restore operations can run concurrently on the same host.', 'true'),
    ('allowed_disk', 'String', 'Disk to backup to when using `File()`. This setting must be set in order to use `File`.', ''),
    ('allowed_path', 'String', 'Path to backup to when using `File()`. This setting must be set in order to use `File`.', ''),
    ('attempts_to_collect_metadata_before_sleep', 'UInt', 'Number of attempts to collect metadata before sleeping in case of inconsistency after comparing collected metadata.', '2'),
    ('collect_metadata_timeout', 'UInt64', 'Timeout in milliseconds for collecting metadata during backup.', '600000'),
    ('compare_collected_metadata', 'Bool', 'If true, compares the collected metadata with the existing metadata to ensure they are not changed during backup .', 'true'),
    ('create_table_timeout', 'UInt64', 'Timeout in milliseconds for creating tables during restore.', '300000'),
    ('max_attempts_after_bad_version', 'UInt64', 'Maximum number of attempts to retry after encountering a bad version error during coordinated backup/restore.', '3'),
    ('max_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Maximum sleep time in milliseconds before the next attempt to collect metadata.', '100'),
    ('min_sleep_before_next_attempt_to_collect_metadata', 'UInt64', 'Minimum sleep time in milliseconds before the next attempt to collect metadata.', '5000'),
    ('remove_backup_files_after_failure', 'Bool', 'If the `BACKUP` command fails, ClickHouse will try to remove the files already copied to the backup before the failure,  otherwise it will leave the copied files as they are.', 'true'),
    ('sync_period_ms', 'UInt64', 'Synchronization period in milliseconds for coordinated backup/restore.', '5000'),
    ('test_inject_sleep', 'Bool', 'Testing related sleep', 'false'),
    ('test_randomize_order', 'Bool', 'If true, randomizes the order of certain operations for testing purposes.', 'false'),
    ('zookeeper_path', 'String', 'Path in ZooKeeper where backup and restore metadata is stored when using `ON CLUSTER` clause.', '/clickhouse/backups')
  ]) AS t )
SELECT concat('`', t.1, '`') AS Setting, t.2 AS Type, t.3 AS Description, concat('`', t.4, '`') AS Default FROM settings FORMAT Markdown
-->
| Setting | Type | Description | Default |
|:-|:-|:-|:-|
| `allow_concurrent_backups` | Bool | Determines whether multiple backup operations can run concurrently on the same host. | `true` |
| `allow_concurrent_restores` | Bool | Determines whether multiple restore operations can run concurrently on the same host. | `true` |
| `allowed_disk` | String | Disk to backup to when using `File()`. This setting must be set in order to use `File`. | `` |
| `allowed_path` | String | Path to backup to when using `File()`. This setting must be set in order to use `File`. | `` |
| `attempts_to_collect_metadata_before_sleep` | UInt | Number of attempts to collect metadata before sleeping in case of inconsistency after comparing collected metadata. | `2` |
| `collect_metadata_timeout` | UInt64 | Timeout in milliseconds for collecting metadata during backup. | `600000` |
| `compare_collected_metadata` | Bool | If true, compares the collected metadata with the existing metadata to ensure they are not changed during backup . | `true` |
| `create_table_timeout` | UInt64 | Timeout in milliseconds for creating tables during restore. | `300000` |
| `max_attempts_after_bad_version` | UInt64 | Maximum number of attempts to retry after encountering a bad version error during coordinated backup/restore. | `3` |
| `max_sleep_before_next_attempt_to_collect_metadata` | UInt64 | Maximum sleep time in milliseconds before the next attempt to collect metadata. | `100` |
| `min_sleep_before_next_attempt_to_collect_metadata` | UInt64 | Minimum sleep time in milliseconds before the next attempt to collect metadata. | `5000` |
| `remove_backup_files_after_failure` | Bool | If the `BACKUP` command fails, ClickHouse will try to remove the files already copied to the backup before the failure,  otherwise it will leave the copied files as they are. | `true` |
| `sync_period_ms` | UInt64 | Synchronization period in milliseconds for coordinated backup/restore. | `5000` |
| `test_inject_sleep` | Bool | Testing related sleep | `false` |
| `test_randomize_order` | Bool | If true, randomizes the order of certain operations for testing purposes. | `false` |
| `zookeeper_path` | String | Path in ZooKeeper where backup and restore metadata is stored when using `ON CLUSTER` clause. | `/clickhouse/backups` |

This setting is configured by default as:

```xml
<backups>
    ....
</backups>
```

## background_schedule_pool_log {#background_schedule_pool_log}

Contains information about all background tasks that are executed via various background pools.

```xml
<background_schedule_pool_log>
    <database>system</database>
    <table>background_schedule_pool_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <!-- Only tasks longer than duration_threshold_milliseconds will be logged. Zero means log everything -->
    <duration_threshold_milliseconds>0</duration_threshold_milliseconds>
</background_schedule_pool_log>
```

## bcrypt_workfactor {#bcrypt_workfactor}

Work factor for the `bcrypt_password` authentication type which uses the [Bcrypt algorithm](https://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/).
The work factor defines the amount of computations and time needed to compute the hash and verify the password.

```xml
<bcrypt_workfactor>12</bcrypt_workfactor>
```

:::warning
For applications with high-frequency authentication,
consider alternative authentication methods due to
bcrypt's computational overhead at higher work factors.
:::

## table_engines_require_grant {#table_engines_require_grant}

If set to true, users require a grant to create a table with a specific engine e.g. `GRANT TABLE ENGINE ON TinyLog to user`.

:::note
By default, for backward compatibility creating table with a specific table engine ignores grant, however you can change this behaviour by setting this to true.
:::

## builtin_dictionaries_reload_interval {#builtin_dictionaries_reload_interval}

The interval in seconds before reloading built-in dictionaries.

ClickHouse reloads built-in dictionaries every x seconds. This makes it possible to edit dictionaries "on the fly" without restarting the server.

**Example**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## compression {#compression}

Data compression settings for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine tables.

:::note
We recommend not changing this if you have just started using ClickHouse.
:::

**Configuration template**:

```xml
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

**`<case>` fields**:

- `min_part_size` – The minimum size of a data part.
- `min_part_size_ratio` – The ratio of the data part size to the table size.
- `method` – Compression method. Acceptable values: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
- `level` – Compression level. See [Codecs](/sql-reference/statements/create/table#general-purpose-codecs).

:::note
You can configure multiple `<case>` sections.
:::

**Actions when conditions are met**:

- If a data part matches a condition set, ClickHouse uses the specified compression method.
- If a data part matches multiple condition sets, ClickHouse uses the first matched condition set.

:::note
If no conditions are met for a data part, ClickHouse uses the `lz4` compression.
:::

**Example**

```xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

## encryption {#encryption}

Configures a command to obtain a key to be used by [encryption codecs](/sql-reference/statements/create/table#encryption-codecs). Key (or keys) should be written in environment variables or set in the configuration file.

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
:::note
Everything mentioned above can be applied for `aes_256_gcm_siv` (but the key must be 32 bytes long).
:::

## error_log {#error_log}

It is disabled by default.

**Enabling**

To manually turn on error history collection [`system.error_log`](../../operations/system-tables/error_log.md), create `/etc/clickhouse-server/config.d/error_log.xml` with the following content:

```xml
<clickhouse>
    <error_log>
        <database>system</database>
        <table>error_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </error_log>
</clickhouse>
```

**Disabling**

To disable `error_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_error_log.xml` with the following content:

```xml
<clickhouse>
    <error_log remove="1" />
</clickhouse>
```

<SystemLogParameters/>

## custom_settings_prefixes {#custom_settings_prefixes}

List of prefixes for [custom settings](/operations/settings/query-level#custom_settings). The prefixes must be separated with commas.

**Example**

```xml
<custom_settings_prefixes>custom_</custom_settings_prefixes>
```

**See Also**

- [Custom settings](/operations/settings/query-level#custom_settings)

## core_dump {#core_dump}

Configures soft limit for core dump file size.

:::note
Hard limit is configured via system tools
:::

**Example**

```xml
<core_dump>
     <size_limit>1073741824</size_limit>
</core_dump>
```

## default_profile {#default_profile}

Default settings profile. Settings profiles are located in the file specified in the setting `user_config`.

**Example**

```xml
<default_profile>default</default_profile>
```

## dictionaries_config {#dictionaries_config}

The path to the config file for dictionaries.

Path:

- Specify the absolute path or the path relative to the server config file.
- The path can contain wildcards \* and ?.

See also:
- "[Dictionaries](../../sql-reference/statements/create/dictionary/overview.md)".

**Example**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## user_defined_executable_functions_config {#user_defined_executable_functions_config}

The path to the config file for executable user defined functions.

Path:

- Specify the absolute path or the path relative to the server config file.
- The path can contain wildcards \* and ?.

See also:
- "[Executable User Defined Functions](/sql-reference/functions/udf#executable-user-defined-functions).".

**Example**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

## graphite {#graphite}

Sending data to [Graphite](https://github.com/graphite-project).

Settings:

- `host` – The Graphite server.
- `port` – The port on the Graphite server.
- `interval` – The interval for sending, in seconds.
- `timeout` – The timeout for sending data, in seconds.
- `root_path` – Prefix for keys.
- `metrics` – Sending data from the [system.metrics](/operations/system-tables/metrics) table.
- `events` – Sending deltas data accumulated for the time period from the [system.events](/operations/system-tables/events) table.
- `events_cumulative` – Sending cumulative data from the [system.events](/operations/system-tables/events) table.
- `asynchronous_metrics` – Sending data from the [system.asynchronous_metrics](/operations/system-tables/asynchronous_metrics) table.

You can configure multiple `<graphite>` clauses. For instance, you can use this for sending different data at different intervals.

**Example**

```xml
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

## graphite_rollup {#graphite_rollup}

Settings for thinning data for Graphite.

For more details, see [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Example**

```xml
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

## http_handlers {#http_handlers}

Allows using custom HTTP handlers.
To add a new http handler simply add a new `<rule>`.
Rules are checked from top to bottom as defined,
and the first match will run the handler.

The following settings can be configured by sub-tags:

| Sub-tags             | Definition                                                                                                                                        |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`                | To match the request URL, you can use the 'regex:' prefix to use regex match (optional)                                                           |
| `methods`            | To match request methods, you can use commas to separate multiple method matches (optional)                                                       |
| `headers`            | To match request headers, match each child element (child element name is header name), you can use 'regex:' prefix to use regex match (optional) |
| `handler`            | The request handler                                                                                                                               |
| `empty_query_string` | Check that there is no query string in the URL                                                                                                    |

`handler` contains the following settings, which can be configured by sub-tags:

| Sub-tags           | Definition                                                                                                                                                            |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`              | A location for redirect                                                                                                                                               |
| `type`             | Supported types: static, dynamic_query_handler, predefined_query_handler, redirect                                                                                    |
| `status`           | Use with static type, response status code                                                                                                                            |
| `query_param_name` | Use with dynamic_query_handler type, extracts and executes the value corresponding to the `<query_param_name>` value in HTTP request params                             |
| `query`            | Use with predefined_query_handler type, executes query when the handler is called                                                                                     |
| `content_type`     | Use with static type, response content-type                                                                                                                           |
| `response_content` | Use with static type, Response content sent to client, when using the prefix 'file://' or 'config://', find the content from the file or configuration send to client |

Along with a list of rules, you can specify `<defaults/>` which specifies to enable all the default handlers.

Example:

```xml
<http_handlers>
    <rule>
        <url>/</url>
        <methods>POST,GET</methods>
        <headers><pragma>no-cache</pragma></headers>
        <handler>
            <type>dynamic_query_handler</type>
            <query_param_name>query</query_param_name>
        </handler>
    </rule>

    <rule>
        <url>/predefined_query</url>
        <methods>POST,GET</methods>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT * FROM system.settings</query>
        </handler>
    </rule>

    <rule>
        <handler>
            <type>static</type>
            <status>200</status>
            <content_type>text/plain; charset=UTF-8</content_type>
            <response_content>config://http_server_default_response</response_content>
        </handler>
    </rule>
</http_handlers>
```

## http_server_default_response {#http_server_default_response}

The page that is shown by default when you access the ClickHouse HTTP(s) server.
The default value is "Ok." (with a line feed at the end)

**Example**

Opens `https://tabix.io/` when accessing `http://localhost: http_port`.

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## http_options_response {#http_options_response}

Used to add headers to the response in an `OPTIONS` HTTP request.
The `OPTIONS` method is used when making CORS preflight requests.

For more information, see [OPTIONS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS).

Example:

```xml
<http_options_response>
     <header>
            <name>Access-Control-Allow-Origin</name>
            <value>*</value>
     </header>
     <header>
          <name>Access-Control-Allow-Headers</name>
          <value>origin, x-requested-with, x-clickhouse-format, x-clickhouse-user, x-clickhouse-key, Authorization</value>
     </header>
     <header>
          <name>Access-Control-Allow-Methods</name>
          <value>POST, GET, OPTIONS</value>
     </header>
     <header>
          <name>Access-Control-Max-Age</name>
          <value>86400</value>
     </header>
</http_options_response>
```

## hsts_max_age {#hsts_max_age}

Expired time for HSTS in seconds.

:::note
A value of `0` means ClickHouse disables HSTS. If you set a positive number, the HSTS will be enabled and the max-age is the number you set.
:::

**Example**

```xml
<hsts_max_age>600000</hsts_max_age>
```

## interserver_listen_host {#interserver_listen_host}

Restriction on hosts that can exchange data between ClickHouse servers.
If Keeper is used, the same restriction will be applied to the communication between different Keeper instances.

:::note
By default, the value is equal to the [`listen_host`](#listen_host) setting.
:::

**Example**

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

Type:

Default:

## interserver_http_credentials {#interserver_http_credentials}

A username and a password used to connect to other servers during [replication](../../engines/table-engines/mergetree-family/replication.md). Additionally, the server authenticates other replicas using these credentials.
`interserver_http_credentials` must therefore be the same for all replicas in a cluster.

:::note
- By default, if `interserver_http_credentials` section is omitted, authentication is not used during replication.
- `interserver_http_credentials` settings do not relate to a ClickHouse client credentials [configuration](../../interfaces/cli.md#configuration_files).
- These credentials are common for replication via `HTTP` and `HTTPS`.
:::

The following settings can be configured by sub-tags:

- `user` — Username.
- `password` — Password.
- `allow_empty` — If `true`, then other replicas are allowed to connect without authentication even if credentials are set. If `false`, then connections without authentication are refused. Default: `false`.
- `old` — Contains old `user` and `password` used during credential rotation. Several `old` sections can be specified.

**Credentials Rotation**

ClickHouse supports dynamic interserver credentials rotation without stopping all replicas at the same time to update their configuration. Credentials can be changed in several steps.

To enable authentication, set `interserver_http_credentials.allow_empty` to `true` and add credentials. This allows connections with authentication and without it.

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

After configuring all replicas set `allow_empty` to `false` or remove this setting. It makes authentication with new credentials mandatory.

To change existing credentials, move the username and the password to `interserver_http_credentials.old` section and update `user` and `password` with new values. At this point the server uses new credentials to connect to other replicas and accepts connections with either new or old credentials.

```xml
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

## ldap_servers {#ldap_servers}

List LDAP servers with their connection parameters here to:
- use them as authenticators for dedicated local users, who have an 'ldap' authentication mechanism specified instead of 'password'
- use them as remote user directories.

The following settings can be configured by sub-tags:

| Setting                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                              |
|--------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bind_dn` | Template used to construct the DN to bind to. The resulting DN will be constructed by replacing all `\{user_name\}` substrings of the template with the actual user name during each authentication attempt.                                                                                                                                                                                                                               |
| `enable_tls` | Flag to trigger use of secure connection to the LDAP server. Specify `no` for plain text (`ldap://`) protocol (not recommended). Specify `yes` for LDAP over SSL/TLS (`ldaps://`) protocol (recommended, the default). Specify `starttls` for legacy StartTLS protocol (plain text (`ldap://`) protocol, upgraded to TLS).                                                                                                               |
| `host` | LDAP server hostname or IP, this parameter is mandatory and cannot be empty.                                                                                                                                                                                                                                                                                                                                                             |
| `port` | LDAP server port, default is 636 if `enable_tls` is set to true, `389` otherwise.                                                                                                                                                                                                                                                                                                                                                        |
| `tls_ca_cert_dir` | path to the directory containing CA certificates.                                                                                                                                                                                                                                                                                                                                                                                        |
| `tls_ca_cert_file` | path to CA certificate file.                                                                                                                                                                                                                                                                                                                                                                                                             |
| `tls_cert_file` | path to certificate file.                                                                                                                                                                                                                                                                                                                                                                                                                |
| `tls_cipher_suite` | allowed cipher suite (in OpenSSL notation).                                                                                                                                                                                                                                                                                                                                                                                              |
| `tls_key_file` | path to certificate key file.                                                                                                                                                                                                                                                                                                                                                                                                            |
| `tls_minimum_protocol_version` | The minimum protocol version of SSL/TLS. Accepted values are: `ssl2`, `ssl3`, `tls1.0`, `tls1.1`, `tls1.2` (the default).                                                                                                                                                                                                                                                                                                                |
| `tls_require_cert` | SSL/TLS peer certificate verification behavior. Accepted values are: `never`, `allow`, `try`, `demand` (the default).                                                                                                                                                                                                                                                                                                                    |
| `user_dn_detection` | Section with LDAP search parameters for detecting the actual user DN of the bound user. This is mainly used in search filters for further role mapping when the server is Active Directory. The resulting user DN will be used when replacing `\{user_dn\}` substrings wherever they are allowed. By default, user DN is set equal to bind DN, but once search is performed, it will be updated with to the actual detected user DN value. |
| `verification_cooldown` | A period of time, in seconds, after a successful bind attempt, during which a user will be assumed to be successfully authenticated for all consecutive requests without contacting the LDAP server. Specify `0` (the default) to disable caching and force contacting the LDAP server for each authentication request.                                                                                                                  |

Setting `user_dn_detection` can be configured with sub-tags:

| Setting         | Description                                                                                                                                                                                                                                                                                                                                    |
|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `base_dn`       | template used to construct the base DN for the LDAP search. The resulting DN will be constructed by replacing all `\{user_name\}` and '\{bind_dn\}' substrings of the template with the actual user name and bind DN during the LDAP search.                                                                                                       |
| `scope`         | scope of the LDAP search. Accepted values are: `base`, `one_level`, `children`, `subtree` (the default).                                                                                                                                                                                                                                       |
| `search_filter` | template used to construct the search filter for the LDAP search. The resulting filter will be constructed by replacing all `\{user_name\}`, `\{bind_dn\}`, and `\{base_dn\}` substrings of the template with the actual user name, bind DN, and base DN during the LDAP search. Note, that the special characters must be escaped properly in XML.  |

Example:

```xml
<my_ldap_server>
    <host>localhost</host>
    <port>636</port>
    <bind_dn>uid={user_name},ou=users,dc=example,dc=com</bind_dn>
    <verification_cooldown>300</verification_cooldown>
    <enable_tls>yes</enable_tls>
    <tls_minimum_protocol_version>tls1.2</tls_minimum_protocol_version>
    <tls_require_cert>demand</tls_require_cert>
    <tls_cert_file>/path/to/tls_cert_file</tls_cert_file>
    <tls_key_file>/path/to/tls_key_file</tls_key_file>
    <tls_ca_cert_file>/path/to/tls_ca_cert_file</tls_ca_cert_file>
    <tls_ca_cert_dir>/path/to/tls_ca_cert_dir</tls_ca_cert_dir>
    <tls_cipher_suite>ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:AES256-GCM-SHA384</tls_cipher_suite>
</my_ldap_server>
```

Example (typical Active Directory with configured user DN detection for further role mapping):

```xml
<my_ad_server>
    <host>localhost</host>
    <port>389</port>
    <bind_dn>EXAMPLE\{user_name}</bind_dn>
    <user_dn_detection>
        <base_dn>CN=Users,DC=example,DC=com</base_dn>
        <search_filter>(&amp;(objectClass=user)(sAMAccountName={user_name}))</search_filter>
    </user_dn_detection>
    <enable_tls>no</enable_tls>
</my_ad_server>
```

## listen_host {#listen_host}

Restriction on hosts that requests can come from. If you want the server to answer all of them, specify `::`.

Examples:

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## logger {#logger}

The location and format of log messages.

**Keys**:

| Key                    | Description                                                                                                                                                        |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `async` | When `true` (default) logging will happen asynchronously (one background thread per output channel). Otherwise it will log inside the thread calling LOG           |
| `async_queue_max_size` | When using async logging, the max amount of messages that will be kept in the the queue waiting for flushing. Extra messages will be dropped                       |
| `console` | Enable logging to the console. Set to `1` or `true` to enable. Default is `1` if ClickHouse does not run in daemon mode, `0` otherwise.                            |
| `console_log_level` | Log level for console output. Defaults to `level`.                                                                                                                 |
| `console_shutdown_log_level` | Shutdown level is used to set the console log level at server Shutdown.   
| `console_startup_log_level` | Startup level is used to set the console log level at server startup. After startup log level is reverted to the `console_log_level` setting                                   |   
| `count` | Rotation policy: How many historical log files ClickHouse are kept at most.                                                                                        |
| `errorlog` | The path to the error log file.                                                                                                                                    |
| `formatting.type` | Log format for console output. Currently, only `json` is supported                                                                                                 |
| `level` | Log level. Acceptable values: `none` (turn logging off), `fatal`, `critical`, `error`, `warning`, `notice`, `information`,`debug`, `trace`, `test`                 |
| `log` | The path to the log file.                                                                                                                                          |
| `rotation` | Rotation policy: Controls when log files are rotated. Rotation can be based on size, time, or a combination of both. Examples: 100M, daily, 100M,daily. Once the log file exceeds the specified size or when the specified time interval is reached, it is renamed and archived, and a new log file is created. |
| `shutdown_level` | Shutdown level is used to set the root logger level at server Shutdown.                                                                                            |
| `size` | Rotation policy: Maximum size of the log files in bytes. Once the log file size exceeds this threshold, it is renamed and archived, and a new log file is created. |
| `startup_level` | Startup level is used to set the root logger level at server startup. After startup log level is reverted to the `level` setting                                   |
| `stream_compress` | Compress log messages using LZ4. Set to `1` or `true` to enable.                                                                                                   |
| `syslog_level` | Log level for logging to syslog.                                                                                                                                   |
| `use_syslog` | Also forward log output to syslog.                                                                                                                                 |

**Log format specifiers**

File names in `log` and `errorLog` paths support below format specifiers for the resulting file name (the directory part does not support them).

Column "Example" shows the output at `2023-07-06 18:32:07`.

| Specifier    | Description                                                                                                         | Example                  |
|--------------|---------------------------------------------------------------------------------------------------------------------|--------------------------|
| `%%`         | Literal %                                                                                                           | `%`                        |
| `%n`         | New-line character                                                                                                  |                          |
| `%t`         | Horizontal tab character                                                                                            |                          |
| `%Y`         | Year as a decimal number, e.g. 2017                                                                                 | `2023`                     |
| `%y`         | Last 2 digits of year as a decimal number (range [00,99])                                                           | `23`                       |
| `%C`         | First 2 digits of year as a decimal number (range [00,99])                                                          | `20`                       |
| `%G`         | Four-digit [ISO 8601 week-based year](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), i.e. the year that contains the specified week. Normally useful only with `%V`  | `2023`       |
| `%g`         | Last 2 digits of [ISO 8601 week-based year](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), i.e. the year that contains the specified week.                         | `23`         |
| `%b`         | Abbreviated month name, e.g. Oct (locale dependent)                                                                 | `Jul`                      |
| `%h`         | Synonym of %b                                                                                                       | `Jul`                      |
| `%B`         | Full month name, e.g. October (locale dependent)                                                                    | `July`                     |
| `%m`         | Month as a decimal number (range [01,12])                                                                           | `07`                       |
| `%U`         | Week of the year as a decimal number (Sunday is the first day of the week) (range [00,53])                          | `27`                       |
| `%W`         | Week of the year as a decimal number (Monday is the first day of the week) (range [00,53])                          | `27`                       |
| `%V`         | ISO 8601 week number (range [01,53])                                                                                | `27`                       |
| `%j`         | Day of the year as a decimal number (range [001,366])                                                               | `187`                      |
| `%d`         | Day of the month as a zero-padded decimal number (range [01,31]). Single digit is preceded by zero.                 | `06`                       |
| `%e`         | Day of the month as a space-padded decimal number (range [1,31]). Single digit is preceded by a space.              | `&nbsp; 6`                 |
| `%a`         | Abbreviated weekday name, e.g. Fri (locale dependent)                                                               | `Thu`                      |
| `%A`         | Full weekday name, e.g. Friday (locale dependent)                                                                   | `Thursday`                 |
| `%w`         | Weekday as a integer number with Sunday as 0 (range [0-6])                                                          | `4`                        |
| `%u`         | Weekday as a decimal number, where Monday is 1 (ISO 8601 format) (range [1-7])                                      | `4`                        |
| `%H`         | Hour as a decimal number, 24 hour clock (range [00-23])                                                             | `18`                       |
| `%I`         | Hour as a decimal number, 12 hour clock (range [01,12])                                                             | `06`                       |
| `%M`         | Minute as a decimal number (range [00,59])                                                                          | `32`                       |
| `%S`         | Second as a decimal number (range [00,60])                                                                          | `07`                       |
| `%c`         | Standard date and time string, e.g. Sun Oct 17 04:41:13 2010 (locale dependent)                                     | `Thu Jul  6 18:32:07 2023` |
| `%x`         | Localized date representation (locale dependent)                                                                    | `07/06/23`                 |
| `%X`         | Localized time representation, e.g. 18:40:20 or 6:40:20 PM (locale dependent)                                       | `18:32:07`                 |
| `%D`         | Short MM/DD/YY date, equivalent to %m/%d/%y                                                                         | `07/06/23`                 |
| `%F`         | Short YYYY-MM-DD date, equivalent to %Y-%m-%d                                                                       | `2023-07-06`               |
| `%r`         | Localized 12-hour clock time (locale dependent)                                                                     | `06:32:07 PM`              |
| `%R`         | Equivalent to "%H:%M"                                                                                               | `18:32`                    |
| `%T`         | Equivalent to "%H:%M:%S" (the ISO 8601 time format)                                                                 | `18:32:07`                 |
| `%p`         | Localized a.m. or p.m. designation (locale dependent)                                                               | `PM`                       |
| `%z`         | Offset from UTC in the ISO 8601 format (e.g. -0430), or no characters if the time zone information is not available | `+0800`                    |
| `%Z`         | Locale-dependent time zone name or abbreviation, or no characters if the time zone information is not available     | `Z AWST `                  |

**Example**

```xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server-%F-%T.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server-%F-%T.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <stream_compress>true</stream_compress>
</logger>
```

To print log messages only in the console:

```xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**Per-level Overrides**

The log level of individual log names can be overridden. For example, to mute all messages of loggers "Backup" and "RBAC".

```xml
<logger>
    <levels>
        <logger>
            <name>Backup</name>
            <level>none</level>
        </logger>
        <logger>
            <name>RBAC</name>
            <level>none</level>
        </logger>
    </levels>
</logger>
```

**syslog**

To write log messages additionally to syslog:

```xml
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

Keys for `<syslog>`:

| Key        | Description                                                                                                                                                                                                                                                    |
|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `address`  | The address of syslog in format `host\[:port\]`. If omitted, the local daemon is used.                                                                                                                                                                         |
| `hostname` | The name of the host from which logs are send (optional).                                                                                                                                                                                                      |
| `facility` | The syslog [facility keyword](https://en.wikipedia.org/wiki/Syslog#Facility). Must be specified uppercase with a "LOG_" prefix, e.g. `LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`, etc. Default: `LOG_USER` if `address` is specified, `LOG_DAEMON` otherwise.                                           |
| `format`   | Log message format. Possible values: `bsd` and `syslog.`                                                                                                                                                                                                       |

**Log formats**

You can specify the log format that will be outputted in the console log. Currently, only JSON is supported.

**Example**

Here is an example of an output JSON log:

```json
{
  "date_time_utc": "2024-11-06T09:06:09Z",
  "date_time": "1650918987.180175",
  "thread_name": "#1",
  "thread_id": "254545",
  "level": "Trace",
  "query_id": "",
  "logger_name": "BaseDaemon",
  "message": "Received signal 2",
  "source_file": "../base/daemon/BaseDaemon.cpp; virtual void SignalListener::run()",
  "source_line": "192"
}
```

To enable JSON logging support, use the following snippet:

```xml
<logger>
    <formatting>
        <type>json</type>
        <!-- Can be configured on a per-channel basis (log, errorlog, console, syslog), or globally for all channels (then just omit it). -->
        <!-- <channel></channel> -->
        <names>
            <date_time>date_time</date_time>
            <thread_name>thread_name</thread_name>
            <thread_id>thread_id</thread_id>
            <level>level</level>
            <query_id>query_id</query_id>
            <logger_name>logger_name</logger_name>
            <message>message</message>
            <source_file>source_file</source_file>
            <source_line>source_line</source_line>
        </names>
    </formatting>
</logger>
```

**Renaming keys for JSON logs**

Key names can be modified by changing tag values inside the `<names>` tag. For example, to change `DATE_TIME` to `MY_DATE_TIME`, you can use `<date_time>MY_DATE_TIME</date_time>`.

**Omitting keys for JSON logs**

Log properties can be omitted by commenting out the property. For example, if you do not want your log to print `query_id`, you can comment out the `<query_id>` tag.

## send_crash_reports {#send_crash_reports}

Settings for sending of crash reports to the ClickHouse core developers team.

Enabling it, especially in pre-production environments, is highly appreciated.

Keys:

| Key                   | Description                                                                                                                          |
|-----------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `enabled` | Boolean flag to enable the feature, `true` by default. Set to `false` to avoid sending crash reports.                                |
| `endpoint` | You can override the endpoint URL for sending crash reports.                                                                         |
| `send_logical_errors` | `LOGICAL_ERROR` is like an `assert`, it is a bug in ClickHouse. This boolean flag enables sending this exceptions (Default: `true`). |

**Recommended usage**

```xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

## ssh_server {#ssh_server}

The public part of the host key will be written to the known_hosts file
on the SSH client side on the first connect.

Host Key Configurations are inactive by default.
Uncomment the host key configurations, and provide the path to the respective ssh key to active them:

Example:

```xml
<ssh_server>
    <host_rsa_key>path_to_the_ssh_key</host_rsa_key>
    <host_ecdsa_key>path_to_the_ssh_key</host_ecdsa_key>
    <host_ed25519_key>path_to_the_ssh_key</host_ed25519_key>
</ssh_server>
```

## tcp_ssh_port {#tcp_ssh_port}

Port for the SSH server which allows the user to connect and execute queries in an interactive fashion using the embedded client over the PTY.

Example:

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
```

## storage_configuration {#storage_configuration}

Allows for multi-disk configuration of storage.

Storage configuration follows the structure:

```xml
<storage_configuration>
    <disks>
        <!-- configuration -->
    </disks>
    <policies>
        <!-- configuration -->
    </policies>
</storage_configuration>
```

### Configuration of disks {#configuration-of-disks}

Configuration of `disks` follows the structure given below:

```xml
<storage_configuration>
    <disks>
        <disk_name_1>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>
        ...
    </disks>
</storage_configuration>
```

The sub-tags above define the following settings for `disks`:

| Setting                 | Description                                                                                           |
|-------------------------|-------------------------------------------------------------------------------------------------------|
| `<disk_name_N>`         | The name of the disk, which should be unique.                                                         |
| `path`                  | The path to which server data will be stored (`data` and `shadow` catalogues). It should end with `/` |
| `keep_free_space_bytes` | Size of the reserved free space on disk.                                                              |

:::note
The order of the disks does not matter.
:::

### Configuration of policies {#configuration-of-policies}

The sub-tags above define the following settings for `policies`:

| Setting                      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `policy_name_N`              | Name of the policy. Policy names must be unique.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `volume_name_N`              | The volume name. Volume names must be unique.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `disk`                       | The disk located inside the volume.                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `max_data_part_size_bytes`   | The maximum size of a chunk of data that can reside on any of the disks in this volume. If the merge results in a chunk size expected to be larger than max_data_part_size_bytes, the chunk will be written to the next volume. Basically this feature allows you to store new / small chunks on a hot (SSD) volume and move them to a cold (HDD) volume when they reach a large size. Do not use this option if the policy has only one volume.                                                                 |
| `move_factor`                | The share of available free space on the volume. If the space becomes less, the data will start transferring to the next volume, if there is one. For transfer, chunks are sorted by size from larger to smaller (descending) and chunks whose total size is sufficient to meet the `move_factor` condition are selected, if the total size of all chunks is insufficient, all chunks will be moved.                                                                                                             |
| `perform_ttl_move_on_insert` | Disables moving data with expired TTL on insertion. By default (if enabled), if we insert a piece of data that has already expired according to the move on life rule, it is immediately moved to the volume / disk specified in the move rule. This can significantly slow down insertion in case the target volume / disk is slow (e.g. S3). If disabled, the expired portion of the data is written to the default volume and then immediately moved to the volume specified in the rule for the expired TTL. |
| `load_balancing`             | Disk balancing policy, `round_robin` or `least_used`.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `least_used_ttl_ms`          | Sets the timeout (in milliseconds) to update the available space on all disks (`0` - always update, `-1` - never update, default value is `60000`). Note, if the disk is only used by ClickHouse and will not be subject to file system resizing on the fly, you can use the `-1` value. In all other cases this is not recommended, as it will eventually lead to incorrect space allocation.                                                                                                                   |
| `prefer_not_to_merge`        | Disables merging parts of the data on this volume. Note: this is potentially harmful and can cause slowdown. When this setting is enabled (don't do this), merging data on this volume is prohibited (which is bad). This allows control of how ClickHouse interacts with slow disks. We recommend not to use this at all.                                                                                                                                                                                       |
| `volume_priority`            | Defines the priority (order) in which volumes are filled. The smaller the value, the higher the priority. The parameter values must be natural numbers and cover the range from 1 to N (N is the largest parameter value specified) with no gaps.                                                                                                                                                                                                                                                                |

For the `volume_priority`:
- If all volumes have this parameter, they are prioritized in the specified order.
- If only _some_ volumes have it, volumes that do not have it have the lowest priority. Those that do have it are prioritized according to the tag value, the priority of the rest is determined by the order of description in the configuration file relative to each other.
- If _no_ volumes are given this parameter, their order is determined by the order of the description in the configuration file.
- The priority of volumes may not be identical.

## macros {#macros}

Parameter substitutions for replicated tables.

Can be omitted if replicated tables are not used.

For more information, see the section [Creating replicated tables](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables).

**Example**

```xml
<macros incl="macros" optional="true" />
```

## replica_group_name {#replica_group_name}

Replica group name for database Replicated.

The cluster created by Replicated database will consist of replicas in the same group.
DDL queries will only wait for the replicas in the same group.

Empty by default.

**Example**

```xml
<replica_group_name>backups</replica_group_name>
```

## max_session_timeout {#max_session_timeout}

Maximum session timeout, in seconds.

Example:

```xml
<max_session_timeout>3600</max_session_timeout>
```

## merge_tree {#merge_tree}

Fine-tuning for tables in the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

For more information, see the MergeTreeSettings.h header file.

**Example**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## metric_log {#metric_log}

It is disabled by default.

**Enabling**

To manually turn on metrics history collection [`system.metric_log`](../../operations/system-tables/metric_log.md), create `/etc/clickhouse-server/config.d/metric_log.xml` with the following content:

```xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>
</clickhouse>
```

**Disabling**

To disable `metric_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_metric_log.xml` with the following content:

```xml
<clickhouse>
    <metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters/>

## replicated_merge_tree {#replicated_merge_tree}

Fine-tuning for tables in the [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md). This setting has a higher priority.

For more information, see the MergeTreeSettings.h header file.

**Example**

```xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

## opentelemetry_span_log {#opentelemetry_span_log}

Settings for the [`opentelemetry_span_log`](../system-tables/opentelemetry_span_log.md) system table.

<SystemLogParameters/>

Example:

```xml
<opentelemetry_span_log>
    <engine>
        engine MergeTree
        partition by toYYYYMM(finish_date)
        order by (finish_date, finish_time_us, trace_id)
    </engine>
    <database>system</database>
    <table>opentelemetry_span_log</table>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</opentelemetry_span_log>
```

## openSSL {#openSSL}

SSL client/server configuration.

Support for SSL is provided by the `libpoco` library. The available configuration options are explained in [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h). Default values can be found in [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp).

Keys for server/client settings:

| Option                        | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                            | Default Value                              |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------|
| `cacheSessions` | Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. Acceptable values: `true`, `false`.                                                                                                                                                                                                                                                                                                                                         | `false`                                    |
| `caConfig` | Path to the file or directory that contains trusted CA certificates. If this points to a file, it must be in PEM format and can contain several CA certificates. If this points to a directory, it must contain one .pem file per CA certificate. The filenames are looked up by the CA subject name hash value. Details can be found in the man page of [SSL_CTX_load_verify_locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html). |                                            |
| `certificateFile` | Path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` contains the certificate.                                                                                                                                                                                                                                                                                                                                                |                                            |
| `cipherList` | Supported OpenSSL encryptions.                                                                                                                                                                                                                                                                                                                                                                                                                                         | `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`  |
| `disableProtocols` | Protocols that are not allowed to be used.                                                                                                                                                                                                                                                                                                                                                                                                                             |                                            |
| `extendedVerification` | If enabled, verify that the certificate CN or SAN matches the peer hostname.                                                                                                                                                                                                                                                                                                                                                                                           | `false`                                    |
| `fips` | Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.                                                                                                                                                                                                                                                                                                                                                                                 | `false`                                    |
| `invalidCertificateHandler` | Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>` .                                                                                                                                                                                                                                                                           | `RejectCertificateHandler`                 |
| `loadDefaultCAFile` | Wether built-in CA certificates for OpenSSL will be used. ClickHouse assumes that builtin CA certificates are in the file `/etc/ssl/cert.pem` (resp. the directory `/etc/ssl/certs`) or in file (resp. directory) specified by the environment variable `SSL_CERT_FILE` (resp. `SSL_CERT_DIR`).                                                                                                                                                                        | `true`                                     |
| `preferServerCiphers` | Client-preferred server ciphers.                                                                                                                                                                                                                                                                                                                                                                                                                                       | `false`                                    |
| `privateKeyFile` | Path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.                                                                                                                                                                                                                                                                                                                                              |                                            |
| `privateKeyPassphraseHandler` | Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.                                                                                                                                                                                                | `KeyConsoleHandler`                        |
| `requireTLSv1` | Require a TLSv1 connection. Acceptable values: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                        | `false`                                    |
| `requireTLSv1_1` | Require a TLSv1.1 connection. Acceptable values: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                      | `false`                                    |
| `requireTLSv1_2` | Require a TLSv1.2 connection. Acceptable values: `true`, `false`.                                                                                                                                                                                                                                                                                                                                                                                                      | `false`                                    |
| `sessionCacheSize` | The maximum number of sessions that the server caches. A value of `0` means unlimited sessions.                                                                                                                                                                                                                                                                                                                                                                        | [1024\*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978)                            |
| `sessionIdContext` | A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. This parameter is always recommended since it helps avoid problems both if the server caches the session and if the client requested caching.                                                                                                                                                        | `$\{application.name\}`                      |
| `sessionTimeout` | Time for caching the session on the server in hours.                                                                                                                                                                                                                                                                                                                                                                                                                   | `2`                                        |
| `verificationDepth` | The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.                                                                                                                                                                                                                                                                                                                                            | `9`                                        |
| `verificationMode` | The method for checking the node's certificates. Details are in the description of the [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) class. Possible values: `none`, `relaxed`, `strict`, `once`.                                                                                                                                                                                                         | `relaxed`                                  |

**Example of settings:**

```xml
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

## part_log {#part_log}

Logging events that are associated with [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). For instance, adding or merging data. You can use the log to simulate merge algorithms and compare their characteristics. You can visualize the merge process.

Queries are logged in the [system.part_log](/operations/system-tables/part_log) table, not in a separate file. You can configure the name of this table in the `table` parameter (see below).

<SystemLogParameters/>

**Example**

```xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</part_log>
```

## processors_profile_log {#processors_profile_log}

Settings for the [`processors_profile_log`](../system-tables/processors_profile_log.md) system table.

<SystemLogParameters/>

The default settings are:

```xml
<processors_profile_log>
    <database>system</database>
    <table>processors_profile_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</processors_profile_log>
```

## prometheus {#prometheus}

Exposing metrics data for scraping from [Prometheus](https://prometheus.io).

Settings:

- `endpoint` – HTTP endpoint for scraping metrics by prometheus server. Start from '/'.
- `port` – Port for `endpoint`.
- `metrics` – Expose metrics from the [system.metrics](/operations/system-tables/metrics) table.
- `events` – Expose metrics from the [system.events](/operations/system-tables/events) table.
- `asynchronous_metrics` – Expose current metrics values from the [system.asynchronous_metrics](/operations/system-tables/asynchronous_metrics) table.
- `errors` - Expose the number of errors by error codes occurred since the last server restart. This information could be obtained from the [system.errors](/operations/system-tables/errors) as well.

**Example**

```xml
<clickhouse>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <!-- highlight-start -->
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
        <errors>true</errors>
    </prometheus>
    <!-- highlight-end -->
</clickhouse>
```

Check (replace `127.0.0.1` with the IP addr or hostname of your ClickHouse server):
```bash
curl 127.0.0.1:9363/metrics
```

## query_log {#query_log}

Setting for logging queries received with the [log_queries=1](../../operations/settings/settings.md) setting.

Queries are logged in the [system.query_log](/operations/system-tables/query_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

<SystemLogParameters/>

If the table does not exist, ClickHouse will create it. If the structure of the query log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

```xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_log>
```

## query_metric_log {#query_metric_log}

It is disabled by default.

**Enabling**

To manually turn on metrics history collection [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md), create `/etc/clickhouse-server/config.d/query_metric_log.xml` with the following content:

```xml
<clickhouse>
    <query_metric_log>
        <database>system</database>
        <table>query_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_metric_log>
</clickhouse>
```

**Disabling**

To disable `query_metric_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` with the following content:

```xml
<clickhouse>
    <query_metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters/>

## query_cache {#query_cache}

[Query cache](../query-cache.md) configuration.

The following settings are available:

| Setting                   | Description                                                                            | Default Value |
|---------------------------|----------------------------------------------------------------------------------------|---------------|
| `max_entries` | The maximum number of `SELECT` query results stored in the cache.                      | `1024`        |
| `max_entry_size_in_bytes` | The maximum size in bytes `SELECT` query results may have to be saved in the cache.    | `1048576`     |
| `max_entry_size_in_rows` | The maximum number of rows `SELECT` query results may have to be saved in the cache.   | `30000000`    |
| `max_size_in_bytes` | The maximum cache size in bytes. `0` means the query cache is disabled.                | `1073741824`  |

:::note
- Changed settings take effect immediately.
- Data for the query cache is allocated in DRAM. If memory is scarce, make sure to set a small value for `max_size_in_bytes` or disable the query cache altogether.
:::

**Example**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

## query_thread_log {#query_thread_log}

Setting for logging threads of queries received with the [log_query_threads=1](/operations/settings/settings#log_query_threads) setting.

Queries are logged in the [system.query_thread_log](/operations/system-tables/query_thread_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

<SystemLogParameters/>

If the table does not exist, ClickHouse will create it. If the structure of the query thread log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

```xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_thread_log>
```

## query_views_log {#query_views_log}

Setting for logging views (live, materialized etc) dependant of queries received with the [log_query_views=1](/operations/settings/settings#log_query_views) setting.

Queries are logged in the [system.query_views_log](/operations/system-tables/query_views_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

<SystemLogParameters/>

If the table does not exist, ClickHouse will create it. If the structure of the query views log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

```xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_views_log>
```

## text_log {#text_log}

Settings for the [text_log](/operations/system-tables/text_log) system table for logging text messages.

<SystemLogParameters/>

Additionally:

| Setting | Description                                                                                                                                                                                                 | Default Value       |
|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| `level` | Maximum Message Level (by default `Trace`) which will be stored in a table.                                                                                                                                 | `Trace`             |

**Example**

```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```

## trace_log {#trace_log}

Settings for the [trace_log](/operations/system-tables/trace_log) system table operation.

<SystemLogParameters/>

The default server configuration file `config.xml` contains the following settings section:

```xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
    <symbolize>false</symbolize>
</trace_log>
```

## asynchronous_insert_log {#asynchronous_insert_log}

Settings for the [asynchronous_insert_log](/operations/system-tables/asynchronous_insert_log) system table for logging async inserts.

<SystemLogParameters/>

**Example**

```xml
<clickhouse>
    <asynchronous_insert_log>
        <database>system</database>
        <table>asynchronous_insert_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </asynchronous_insert_log>
</clickhouse>
```

## crash_log {#crash_log}

Settings for the [crash_log](../../operations/system-tables/crash_log.md) system table operation.

The following settings can be configured by sub-tags:

| Setting                            | Description                                                                                                                                             | Default             | Note                                                                                                               |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|--------------------------------------------------------------------------------------------------------------------|
| `buffer_size_rows_flush_threshold` | Threshold for amount of lines. If the threshold is reached, flushing logs to the disk is launched in background.                                        | `max_size_rows / 2` |                                                                                                                    |
| `database` | Name of the database.                                                                                                                                   |                     |                                                                                                                    |
| `engine` | [MergeTree Engine Definition](/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table) for a system table. |                     | Cannot be used if `partition_by` or `order_by` defined. If not specified `MergeTree` is selected by default        |
| `flush_interval_milliseconds` | Interval for flushing data from the buffer in memory to the table.                                                                                      | `7500`              |                                                                                                                    |
| `flush_on_crash` | Sets whether logs should be dumped to the disk in case of a crash.                                                                                      | `false`             |                                                                                                                    |
| `max_size_rows` | Maximal size in lines for the logs. When the amount of non-flushed logs reaches the max_size, logs are dumped to the disk.                              | `1024`           |                                                                                                                    |
| `order_by` | [Custom sorting key](/engines/table-engines/mergetree-family/mergetree#order_by) for a system table. Can't be used if `engine` defined.      |                     | If `engine` is specified for system table, `order_by` parameter should be specified directly inside 'engine'       |
| `partition_by` | [Custom partitioning key](/engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table.                               |                     | If `engine` is specified for system table, `partition_by` parameter should be specified directly inside 'engine'   |
| `reserved_size_rows` | Pre-allocated memory size in lines for the logs.                                                                                                        | `1024`              |                                                                                                                    |
| `settings` | [Additional parameters](/engines/table-engines/mergetree-family/mergetree/#settings) that control the behavior of the MergeTree (optional).   |                     | If `engine` is specified for system table, `settings` parameter should be specified directly inside 'engine'       |
| `storage_policy` | Name of the storage policy to use for the table (optional).                                                                                             |                     | If `engine` is specified for system table, `storage_policy` parameter should be specified directly inside 'engine' |
| `table` | Name of the system table.                                                                                                                               |                     |                                                                                                                    |
| `ttl` | Specifies the table [TTL](/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl).                                              |                     | If `engine` is specified for system table, `ttl` parameter should be specified directly inside 'engine'            |

The default server configuration file `config.xml` contains the following settings section:

```xml
<crash_log>
    <database>system</database>
    <table>crash_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1024</max_size_rows>
    <reserved_size_rows>1024</reserved_size_rows>
    <buffer_size_rows_flush_threshold>512</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</crash_log>
```

## custom_cached_disks_base_directory {#custom_cached_disks_base_directory}

This setting specifies the cache path for custom (created from SQL) cached disks.
`custom_cached_disks_base_directory` has higher priority for custom disks over `filesystem_caches_path` (found in `filesystem_caches_path.xml`),
which is used if the former one is absent.
The filesystem cache setting path must lie inside that directory,
otherwise an exception will be thrown preventing the disk from being created.

:::note
This will not affect disks created on an older version for which the server was upgraded.
In this case, an exception will not be thrown, to allow the server to successfully start.
:::

Example:

```xml
<custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
```

## backup_log {#backup_log}

Settings for the [backup_log](../../operations/system-tables/backup_log.md) system table for logging `BACKUP` and `RESTORE` operations.

<SystemLogParameters/>

**Example**

```xml
<clickhouse>
    <backup_log>
        <database>system</database>
        <table>backup_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </backup_log>
</clickhouse>
```

## blob_storage_log {#blob_storage_log}

Settings for the [`blob_storage_log`](../system-tables/blob_storage_log.md) system table.

<SystemLogParameters/>

Example:

```xml
<blob_storage_log>
    <database>system</database
    <table>blob_storage_log</table
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds
    <ttl>event_date + INTERVAL 30 DAY</ttl>
</blob_storage_log>
```

## query_masking_rules {#query_masking_rules}

Regexp-based rules, which will be applied to queries as well as all log messages before storing them in server logs,
[`system.query_log`](/operations/system-tables/query_log), [`system.text_log`](/operations/system-tables/text_log), [`system.processes`](/operations/system-tables/processes) tables, and in logs sent to the client. That allows preventing
sensitive data leakage from SQL queries such as names, emails, personal identifiers or credit card numbers to logs.

**Example**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

**Config fields**:

| Setting   | Description                                                                   |
|-----------|-------------------------------------------------------------------------------|
| `name` | name for the rule (optional)                                                  |
| `regexp` | RE2 compatible regular expression (mandatory)                                 |
| `replace` | substitution string for sensitive data (optional, by default - six asterisks) |

The masking rules are applied to the whole query (to prevent leaks of sensitive data from malformed / non-parseable queries).

The [`system.events`](/operations/system-tables/events) table has counter `QueryMaskingRulesMatch` which has an overall number of query masking rules matches.

For distributed queries each server has to be configured separately, otherwise, subqueries passed to other
nodes will be stored without masking.

## remote_servers {#remote_servers}

Configuration of clusters used by the [Distributed](../../engines/table-engines/special/distributed.md) table engine and by the `cluster` table function.

**Example**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

For the value of the `incl` attribute, see the section "[Configuration files](/operations/configuration-files)".

**See Also**

- [skip_unavailable_shards](../../operations/settings/settings.md#skip_unavailable_shards)
- [Cluster Discovery](../../operations/cluster-discovery.md)
- [Replicated database engine](../../engines/database-engines/replicated.md)

## remote_url_allow_hosts {#remote_url_allow_hosts}

List of hosts which are allowed to be used in URL-related storage engines and table functions.

When adding a host with the `\<host\>` xml tag:
- it should be specified exactly as in the URL, as the name is checked before DNS resolution. For example: `<host>clickhouse.com</host>`
- if the port is explicitly specified in the URL, then host:port is checked as a whole. For example: `<host>clickhouse.com:80</host>`
- if the host is specified without a port, then any port of the host is allowed. For example: if `<host>clickhouse.com</host>` is specified then `clickhouse.com:20` (FTP), `clickhouse.com:80` (HTTP), `clickhouse.com:443` (HTTPS) etc are allowed.
- if the host is specified as an IP address, then it is checked as specified in the URL. For example: `[2a02:6b8:a::a]`.
- if there are redirects and support for redirects is enabled, then every redirect (the location field) is checked.

For example:

```sql
<remote_url_allow_hosts>
    <host>clickhouse.com</host>
</remote_url_allow_hosts>
```

## timezone {#timezone}

The server's time zone.

Specified as an IANA identifier for the UTC timezone or geographic location (for example, Africa/Abidjan).

The time zone is necessary for conversions between String and DateTime formats when DateTime fields are output to text format (printed on the screen or in a file), and when getting DateTime from a string. Besides, the time zone is used in functions that work with the time and date if they didn't receive the time zone in the input parameters.

**Example**

```xml
<timezone>Asia/Istanbul</timezone>
```

**See also**

- [session_timezone](../settings/settings.md#session_timezone)

## tcp_port {#tcp_port}

Port for communicating with clients over the TCP protocol.

**Example**

```xml
<tcp_port>9000</tcp_port>
```

## tcp_port_secure {#tcp_port_secure}

TCP port for secure communication with clients. Use it with [OpenSSL](#openssl) settings.

**Default value**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql_port {#mysql_port}

Port for communicating with clients over MySQL protocol.

:::note
- Positive integers specify the port number to listen to
- Empty values are used to disable communication with clients over MySQL protocol.
:::

**Example**

```xml
<mysql_port>9004</mysql_port>
```

## postgresql_port {#postgresql_port}

Port for communicating with clients over PostgreSQL protocol.

:::note
- Positive integers specify the port number to listen to
- Empty values are used to disable communication with clients over PostgreSQL protocol.
:::

**Example**

```xml
<postgresql_port>9005</postgresql_port>
```

## url_scheme_mappers {#url_scheme_mappers}

Configuration for translating shortened or symbolic URL prefixes into full URLs.

Example:

```xml
<url_scheme_mappers>
    <s3>
        <to>https://{bucket}.s3.amazonaws.com</to>
    </s3>
    <gs>
        <to>https://storage.googleapis.com/{bucket}</to>
    </gs>
    <oss>
        <to>https://{bucket}.oss.aliyuncs.com</to>
    </oss>
</url_scheme_mappers>
```

## user_defined_path {#user_defined_path}

The directory with user defined files. Used for SQL user defined functions [SQL User Defined Functions](/sql-reference/functions/udf).

**Example**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

## users_config {#users_config}

Path to the file that contains:

- User configurations.
- Access rights.
- Settings profiles.
- Quota settings.

**Example**

```xml
<users_config>users.xml</users_config>
```

## access_control_improvements {#access_control_improvements}

Settings for optional improvements in the access control system.

| Setting                                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | Default |
|-------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `on_cluster_queries_require_cluster_grant` | Sets whether `ON CLUSTER` queries require the `CLUSTER` grant.                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `true`  |
| `role_cache_expiration_time_seconds` | Sets the number of seconds since last access, that a role is stored in the Role Cache.                                                                                                                                                                                                                                                                                                                                                                                                                           | `600`   |
| `select_from_information_schema_requires_grant` | Sets whether `SELECT * FROM information_schema.<table>` requires any grants and can be executed by any user. If set to true, then this query requires `GRANT SELECT ON information_schema.<table>`, just as for ordinary tables.                                                                                                                                                                                                                                                                                 | `true`  |
| `select_from_system_db_requires_grant` | Sets whether `SELECT * FROM system.<table>` requires any grants and can be executed by any user. If set to true then this query requires `GRANT SELECT ON system.<table>` just as for non-system tables. Exceptions: a few system tables (`tables`, `columns`, `databases`, and some constant tables like `one`, `contributors`) are still accessible for everyone; and if there is a `SHOW` privilege (e.g. `SHOW USERS`) granted then the corresponding system table (i.e. `system.users`) will be accessible. | `true`  |
| `settings_constraints_replace_previous` | Sets whether a constraint in a settings profile for some setting will cancel actions of the previous constraint (defined in other profiles) for that setting, including fields which are not set by the new constraint. It also enables the `changeable_in_readonly` constraint type.                                                                                                                                                                                                                            | `true`  |
| `table_engines_require_grant` | Sets whether creating a table with a specific table engine requires a grant.                                                                                                                                                                                                                                                                                                                                                                                                                                     | `false` |
| `throw_on_unmatched_row_policies` | Sets whether reading from a table should throw an exception if the table has row policies, but none of them are for the current user | `false` |
| `users_without_row_policies_can_read_rows` | Sets whether users without permissive row policies can still read rows using a `SELECT` query. For example, if there are two users A and B and a row policy is defined only for A, then if this setting is true, user B will see all rows. If this setting is false, user B will see no rows.                                                                                                                                                                                                                    | `true`  |

Example:

```xml
<access_control_improvements>
    <throw_on_unmatched_row_policies>true</throw_on_unmatched_row_policies>
    <users_without_row_policies_can_read_rows>true</users_without_row_policies_can_read_rows>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
    <select_from_system_db_requires_grant>true</select_from_system_db_requires_grant>
    <select_from_information_schema_requires_grant>true</select_from_information_schema_requires_grant>
    <settings_constraints_replace_previous>true</settings_constraints_replace_previous>
    <table_engines_require_grant>false</table_engines_require_grant>
    <role_cache_expiration_time_seconds>600</role_cache_expiration_time_seconds>
</access_control_improvements>
```

## s3queue_log {#s3queue_log}

Settings for the `s3queue_log` system table.

<SystemLogParameters/>

The default settings are:

```xml
<s3queue_log>
    <database>system</database>
    <table>s3queue_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</s3queue_log>
```

## dead_letter_queue {#dead_letter_queue}

Setting for the 'dead_letter_queue' system table.

<SystemLogParameters/>

The default settings are:

```xml
<dead_letter_queue>
    <database>system</database>
    <table>dead_letter</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</dead_letter_queue>
```

## zookeeper {#zookeeper}

Contains settings that allow ClickHouse to interact with a [ZooKeeper](http://zookeeper.apache.org/) cluster. ClickHouse uses ZooKeeper for storing metadata of replicas when using replicated tables. If replicated tables are not used, this section of parameters can be omitted.

The following settings can be configured by sub-tags:

| Setting                                    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
|--------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `node` | ZooKeeper endpoint. You can set multiple endpoints. Eg. `<node index="1"><host>example_host</host><port>2181</port></node>`. The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.                                                                                                                                                                                                                                                                                            |
| `operation_timeout_ms` | Maximum timeout for one operation in milliseconds.                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `session_timeout_ms` | Maximum timeout for the client session in milliseconds.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `root` (optional)                          | The znode that is used as the root for znodes used by the ClickHouse server.                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `fallback_session_lifetime.min` (optional) | Minimum limit for the lifetime of a zookeeper session to the fallback node when primary is unavailable (load-balancing). Set in seconds. Default: 3 hours.                                                                                                                                                                                                                                                                                                                                                              |
| `fallback_session_lifetime.max` (optional) | Maximum limit for the lifetime of a zookeeper session to the fallback node when primary is unavailable (load-balancing). Set in seconds. Default: 6 hours.                                                                                                                                                                                                                                                                                                                                                              |
| `identity` (optional)                      | User and password required by ZooKeeper to access requested znodes.                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `use_compression` (optional)               | Enables compression in Keeper protocol if set to true.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `use_xid_64` (optional)                    | Enables 64-bit transaction IDs. Set to `true` to enable extended transaction ID format. Default: `false`.                                                                                                                                                                                                                                                                                                                                                |
| `pass_opentelemetry_tracing_context` (optional) | Enables propagation of OpenTelemetry tracing context to Keeper requests. When enabled, tracing spans will be created for Keeper operations, allowing distributed tracing across ClickHouse and Keeper. Requires `use_xid_64` to be enabled. See [Tracing ClickHouse Keeper Requests](/operations/opentelemetry#tracing-clickhouse-keeper-requests) for more details. Default: `false`.                                                                                                                                      |

There is also the `zookeeper_load_balancing` setting (optional) which lets you select the algorithm for ZooKeeper node selection:

| Algorithm Name                   | Description                                                                                                                    |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `random`                         | randomly selects one of ZooKeeper nodes.                                                                                       |
| `in_order`                       | selects the first ZooKeeper node, if it's not available then the second, and so on.                                            |
| `nearest_hostname`               | selects a ZooKeeper node with a hostname that is most similar to the server's hostname, hostname is compared with name prefix. |
| `hostname_levenshtein_distance`  | just like nearest_hostname, but it compares hostname in a levenshtein distance manner.                                         |
| `first_or_random`                | selects the first ZooKeeper node, if it's not available then randomly selects one of remaining ZooKeeper nodes.                |
| `round_robin`                    | selects the first ZooKeeper node, if reconnection happens selects the next.                                                    |

**Example configuration**

```xml
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
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
    <!-- Optional. Enable 64-bit transaction IDs. -->
    <use_xid_64>false</use_xid_64>
    <!-- Optional. Enable OpenTelemetry tracing context propagation (requires use_xid_64). -->
    <pass_opentelemetry_tracing_context>false</pass_opentelemetry_tracing_context>
</zookeeper>
```

**See Also**

- [Replication](../../engines/table-engines/mergetree-family/replication.md)
- [ZooKeeper Programmer's Guide](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
- [Optional secured communication between ClickHouse and Zookeeper](/operations/ssl-zookeeper)

## use_minimalistic_part_header_in_zookeeper {#use_minimalistic_part_header_in_zookeeper}

Storage method for data part headers in ZooKeeper. This setting only applies to the [`MergeTree`](/engines/table-engines/mergetree-family) family. It can be specified:

**Globally in the [merge_tree](#merge_tree) section of the `config.xml` file**

ClickHouse uses the setting for all the tables on the server. You can change the setting at any time. Existing tables change their behaviour when the setting changes.

**For each table**

When creating a table, specify the corresponding [engine setting](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). The behaviour of an existing table with this setting does not change, even if the global setting changes.

**Possible values**

- `0` — Functionality is turned off.
- `1` — Functionality is turned on.

If [`use_minimalistic_part_header_in_zookeeper = 1`](#use_minimalistic_part_header_in_zookeeper), then [replicated](../../engines/table-engines/mergetree-family/replication.md) tables store the headers of the data parts compactly using a single `znode`. If the table contains many columns, this storage method significantly reduces the volume of the data stored in Zookeeper.

:::note
After applying `use_minimalistic_part_header_in_zookeeper = 1`, you can't downgrade the ClickHouse server to a version that does not support this setting. Be careful when upgrading ClickHouse on servers in a cluster. Don't upgrade all the servers at once. It is safer to test new versions of ClickHouse in a test environment, or on just a few servers of a cluster.

Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.
:::

## distributed_ddl {#distributed_ddl}

Manage executing [distributed ddl queries](../../sql-reference/distributed-ddl.md) (`CREATE`, `DROP`, `ALTER`, `RENAME`) on cluster.
Works only if [ZooKeeper](/operations/server-configuration-parameters/settings#zookeeper) is enabled.

The configurable settings within `<distributed_ddl>` include:

| Setting                | Description                                                                                                                       | Default Value                          |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| `cleanup_delay_period` | cleaning starts after new node event is received if the last cleaning wasn't made sooner than `cleanup_delay_period` seconds ago. | `60` seconds                           |
| `max_tasks_in_queue` | the maximum number of tasks that can be in the queue.                                                                             | `1,000`                                |
| `path` | the path in Keeper for the `task_queue` for DDL queries                                                                           |                                        |
| `pool_size` | how many `ON CLUSTER` queries can be run simultaneously                                                                           |                                        |
| `profile` | the profile used to execute the DDL queries                                                                                       |                                        |
| `task_max_lifetime` | delete node if its age is greater than this value.                                                                                | `7 * 24 * 60 * 60` (a week in seconds) |

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

**See also**

- [Access Control and Account Management](/operations/access-rights#access-control-usage)

## allow_plaintext_password {#allow_plaintext_password}

Sets whether plaintext-password types (insecure) are allowed or not.

```xml
<allow_plaintext_password>1</allow_plaintext_password>
```

## allow_no_password {#allow_no_password}

Sets whether an insecure password type of no_password is allowed or not.

```xml
<allow_no_password>1</allow_no_password>
```

## allow_implicit_no_password {#allow_implicit_no_password}

Forbids creating a user with no password unless 'IDENTIFIED WITH no_password' is explicitly specified.

```xml
<allow_implicit_no_password>1</allow_implicit_no_password>
```

## default_session_timeout {#default_session_timeout}

Default session timeout, in seconds.

```xml
<default_session_timeout>60</default_session_timeout>
```

## default_password_type {#default_password_type}

Sets the password type to be automatically set for in queries like `CREATE USER u IDENTIFIED BY 'p'`.

Accepted values are:
- `plaintext_password`
- `sha256_password`
- `double_sha1_password`
- `bcrypt_password`

```xml
<default_password_type>sha256_password</default_password_type>
```

## user_directories {#user_directories}

Section of the configuration file that contains settings:
- Path to configuration file with predefined users.
- Path to folder where users created by SQL commands are stored.
- ZooKeeper node path where users created by SQL commands are stored and replicated.

If this section is specified, the path from [users_config](/operations/server-configuration-parameters/settings#users_config) and [access_control_path](../../operations/server-configuration-parameters/settings.md#access_control_path) won't be used.

The `user_directories` section can contain any number of items, the order of the items means their precedence (the higher the item the higher the precedence).

**Examples**

```xml
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

```xml
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

To add an LDAP server as a remote user directory of users that are not defined locally, define a single `ldap` section with the following settings:

| Setting  | Description                                                                                                                                                                                                                                                                                                                                                                    |
|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `roles` | section with a list of locally defined roles that will be assigned to each user retrieved from the LDAP server. If no roles are specified, user will not be able to perform any actions after authentication. If any of the listed roles is not defined locally at the time of authentication, the authentication attempt will fail as if the provided password was incorrect. |
| `server` | one of LDAP server names defined in `ldap_servers` config section. This parameter is mandatory and cannot be empty.                                                                                                                                                                                                                                                            |

**Example**

```xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

## top_level_domains_list {#top_level_domains_list}

Defines a list of custom top level domains to add where each entry is, of the format `<name>/path/to/file</name>`.

For example:

```xml
<top_level_domains_lists>
    <public_suffix_list>/path/to/public_suffix_list.dat</public_suffix_list>
</top_level_domains_lists>
```

See also:
- function [`cutToFirstSignificantSubdomainCustom`](../../sql-reference/functions/url-functions.md/#cutToFirstSignificantSubdomainCustom) and variations thereof,
  which accepts a custom TLD list name, returning the part of the domain that includes top-level subdomains up to the first significant subdomain.

## proxy {#proxy}

Define proxy servers for HTTP and HTTPS requests, currently supported by S3 storage, S3 table functions, and URL functions.

There are three ways to define proxy servers:
- environment variables
- proxy lists
- remote proxy resolvers.

Bypassing proxy servers for specific hosts is also supported with the use of `no_proxy`.

**Environment variables**

The `http_proxy` and `https_proxy` environment variables allow you to specify a
proxy server for a given protocol. If you have it set on your system, it should work seamlessly.

This is the simplest approach if a given protocol has
only one proxy server and that proxy server doesn't change.

**Proxy lists**

This approach allows you to specify one or more
proxy servers for a protocol. If more than one proxy server is defined,
ClickHouse uses the different proxies on a round-robin basis, balancing the
load across the servers. This is the simplest approach if there is more than
one proxy server for a protocol and the list of proxy servers doesn't change.

**Configuration template**

```xml
<proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```
Select a parent field in the tabs below to view their children:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>

| Field     | Description                         |
|-----------|-------------------------------------|
| `<http>` | A list of one or more HTTP proxies  |
| `<https>` | A list of one or more HTTPS proxies |

  </TabItem>
  <TabItem value="http_https" label="<http> and <https>">

| Field   | Description          |
|---------|----------------------|
| `<uri>` | The URI of the proxy |

  </TabItem>
</Tabs>

**Remote proxy resolvers**

It's possible that the proxy servers change dynamically. In that
case, you can define the endpoint of a resolver. ClickHouse sends
an empty GET request to that endpoint, the remote resolver should return the proxy host.
ClickHouse will use it to form the proxy URI using the following template: `\{proxy_scheme\}://\{proxy_host\}:{proxy_port}`

**Configuration template**

```xml
<proxy>
    <http>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>80</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </http>

    <https>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>3128</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </https>

</proxy>
```

Select a parent field in the tabs below to view their children:

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>

| Field    | Description                      |
|----------|----------------------------------|
| `<http>` | A list of one or more resolvers* |
| `<https>` | A list of one or more resolvers* |

  </TabItem>
  <TabItem value="http_https" label="<http> and <https>">

| Field       | Description                                   |
|-------------|-----------------------------------------------|
| `<resolver>` | The endpoint and other details for a resolver |

:::note
You can have multiple `<resolver>` elements, but only the first
`<resolver>` for a given protocol is used. Any other `<resolver>`
elements for that protocol are ignored. That means load balancing
(if needed) should be implemented by the remote resolver.
:::

  </TabItem>
  <TabItem value="resolver" label="<resolver>">

| Field               | Description                                                                                                                                                                            |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `<endpoint>`        | The URI of the proxy resolver                                                                                                                                                          |
| `<proxy_scheme>`    | The protocol of the final proxy URI. This can be either `http` or `https`.                                                                                                             |
| `<proxy_port>`      | The port number of the proxy resolver                                                                                                                                                  |
| `<proxy_cache_time>` | The time in seconds that values from the resolver should be cached by ClickHouse. Setting this value to `0` causes ClickHouse to contact the resolver for every HTTP or HTTPS request. |

  </TabItem>
</Tabs>

**Precedence**

Proxy settings are determined in the following order:

| Order | Setting                |
|-------|------------------------|
| 1.    | Remote proxy resolvers |
| 2.    | Proxy lists            |
| 3.    | Environment variables  |

ClickHouse will check the highest priority resolver type for the request protocol. If it is not defined,
it will check the next highest priority resolver type, until it reaches the environment resolver.
This also allows a mix of resolver types can be used.

## disable_tunneling_for_https_requests_over_http_proxy {#disable_tunneling_for_https_requests_over_http_proxy}

By default, tunneling (i.e, `HTTP CONNECT`) is used to make `HTTPS` requests over `HTTP` proxy. This setting can be used to disable it.

**no_proxy**

By default, all requests will go through the proxy. In order to disable it for specific hosts, the `no_proxy` variable must be set.
It can be set inside the `<proxy>` clause for list and remote resolvers and as an environment variable for environment resolver.
It supports IP addresses, domains, subdomains and `'*'` wildcard for full bypass. Leading dots are stripped just like curl does.

**Example**

The below configuration bypasses proxy requests to `clickhouse.cloud` and all of its subdomains (e.g, `auth.clickhouse.cloud`).
The same applies to GitLab, even though it has a leading dot. Both `gitlab.com` and `about.gitlab.com` would bypass the proxy.

```xml
<proxy>
    <no_proxy>clickhouse.cloud,.gitlab.com</no_proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

## workload_path {#workload_path}

The directory used as a storage for all `CREATE WORKLOAD` and `CREATE RESOURCE` queries. By default `/workload/` folder under server working directory is used.

**Example**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**See Also**
- [Workload Hierarchy](/operations/workload-scheduling.md#workloads)
- [workload_zookeeper_path](#workload_zookeeper_path)

## workload_zookeeper_path {#workload_zookeeper_path}

The path to a ZooKeeper node, which is used as a storage for all `CREATE WORKLOAD` and `CREATE RESOURCE` queries. For consistency all SQL definitions are stored as a value of this single znode. By default ZooKeeper is not used and definitions are stored on [disk](#workload_path).

**Example**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**See Also**
- [Workload Hierarchy](/operations/workload-scheduling.md#workloads)
- [workload_path](#workload_path)

## zookeeper_log {#zookeeper_log}

Settings for the [`zookeeper_log`](/operations/system-tables/zookeeper_log) system table.

The following settings can be configured by sub-tags:

<SystemLogParameters/>

**Example**

```xml
<clickhouse>
    <zookeeper_log>
        <database>system</database>
        <table>zookeeper_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <ttl>event_date + INTERVAL 1 WEEK DELETE</ttl>
    </zookeeper_log>
</clickhouse>
```