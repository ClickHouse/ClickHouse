---
title: Source 之外的服务器设置
---

<div id="asynchronous_metric_log">
  ## asynchronous_metric_log
</div>

在 ClickHouse Cloud 部署中默认已启用。

如果您的环境中该设置默认未启用，可根据 ClickHouse 的安装方式按以下说明启用或禁用。

**启用**

要手动开启异步指标日志历史记录的收集 [`system.asynchronous_metric_log`](../../operations/system-tables/asynchronous_metric_log.md)，请创建 `/etc/clickhouse-server/config.d/asynchronous_metric_log.xml`，内容如下：

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

**禁用**

要禁用 `asynchronous_metric_log` 设置，请创建以下文件 `/etc/clickhouse-server/config.d/disable_asynchronous_metric_log.xml`，内容如下：

```xml
<clickhouse><asynchronous_metric_log remove="1" /></clickhouse>
```

<SystemLogParameters />

<div id="auth_use_forwarded_address">
  ## auth_use_forwarded_address
</div>

对通过代理连接的客户端，使用源地址进行身份验证。

:::note
使用此设置时应格外谨慎，因为转发地址很容易被伪造。接受此类身份验证的服务器不应被直接访问，而应仅通过受信任的代理访问。
:::

<div id="backups">
  ## 备份
</div>

执行 [`BACKUP` 和 `RESTORE`](/zh/operations/backup/overview) 语句时使用的备份设置。

以下设置可通过子标签配置：

{/* SQL
  WITH settings AS (
  SELECT arrayJoin([
    ('allow_concurrent_backups', 'Bool','确定是否允许同一主机上的多个备份操作并发运行。', 'true'),
    ('allow_concurrent_restores', 'Bool', '确定是否允许同一主机上的多个恢复操作并发运行。', 'true'),
    ('allowed_disk', 'String', '使用 `File()` 时要备份到的磁盘。要使用 `File`，必须设置此项。', ''),
    ('allowed_path', 'String', '使用 `File()` 时要备份到的路径。要使用 `File`，必须设置此项。', ''),
    ('attempts_to_collect_metadata_before_sleep', 'UInt', '比较已收集的元数据后，如果发现不一致，在进入等待前收集元数据的尝试次数。', '2'),
    ('collect_metadata_timeout', 'UInt64', '备份期间收集元数据的超时时间（毫秒）。', '600000'),
    ('compare_collected_metadata', 'Bool', '如果为 true，则会将已收集的元数据与现有元数据进行比较，以确保它们在备份期间未发生变化。', 'true'),
    ('create_table_timeout', 'UInt64', '恢复期间创建表的超时时间（毫秒）。', '300000'),
    ('max_attempts_after_bad_version', 'UInt64', '在协调备份/恢复期间遇到 bad version 错误后，可重试的最大尝试次数。', '3'),
    ('max_sleep_before_next_attempt_to_collect_metadata', 'UInt64', '下次尝试收集元数据前的最长等待时间（毫秒）。', '100'),
    ('min_sleep_before_next_attempt_to_collect_metadata', 'UInt64', '下次尝试收集元数据前的最短等待时间（毫秒）。', '5000'),
    ('remove_backup_files_after_failure', 'Bool', '如果 `BACKUP` 命令失败，ClickHouse 会尝试删除失败前已复制到备份中的文件；否则会保留这些已复制的文件。', 'true'),
    ('sync_period_ms', 'UInt64', '协调备份/恢复的同步周期（毫秒）。', '5000'),
    ('test_inject_sleep', 'Bool', '与测试相关的等待', 'false'),
    ('test_randomize_order', 'Bool', '如果为 true，则会出于测试目的随机打乱某些操作的顺序。', 'false'),
    ('zookeeper_path', 'String', '使用 `ON CLUSTER` 子句时，在 ZooKeeper 中存储备份和恢复元数据的路径。', '/clickhouse/backups')
  ]) AS t )
  SELECT concat('`', t.1, '`') AS Setting, t.2 AS Type, t.3 AS Description, concat('`', t.4, '`') AS Default FROM settings FORMAT Markdown
  */ }

| 设置                                                  | 类型     | 描述                                                            | 默认值                   |
| :-------------------------------------------------- | :----- | :------------------------------------------------------------ | :-------------------- |
| `allow_concurrent_backups`                          | Bool   | 决定是否允许在同一主机上并发运行多个备份操作。                                       | `true`                |
| `allow_concurrent_restores`                         | Bool   | 决定是否允许在同一主机上并发运行多个恢复操作。                                       | `true`                |
| `allowed_disk`                                      | String | 使用 `File()` 时备份写入的磁盘。必须设置此项才能使用 `File`。                       | &#96;&#96;            |
| `allowed_path`                                      | String | 使用 `File()` 时备份写入的路径。必须设置此项才能使用 `File`。                       | &#96;&#96;            |
| `attempts_to_collect_metadata_before_sleep`         | UInt   | 比较已收集的元数据后如果发现不一致，在进入休眠前收集元数据的尝试次数。                           | `2`                   |
| `collect_metadata_timeout`                          | UInt64 | 备份期间收集元数据的超时时间 (毫秒) 。                                         | `600000`              |
| `compare_collected_metadata`                        | Bool   | 如果为 true，则会将收集到的元数据与现有元数据进行比较，以确保它们在备份期间未发生变化。                | `true`                |
| `create_table_timeout`                              | UInt64 | 恢复期间创建表的超时时间 (毫秒) 。                                           | `300000`              |
| `max_attempts_after_bad_version`                    | UInt64 | 在协调备份/恢复期间遇到 bad version 错误后，允许重试的最大次数。                       | `3`                   |
| `max_sleep_before_next_attempt_to_collect_metadata` | UInt64 | 下次尝试收集元数据前的最长休眠时间 (毫秒) 。                                      | `100`                 |
| `min_sleep_before_next_attempt_to_collect_metadata` | UInt64 | 下次尝试收集元数据前的最短休眠时间 (毫秒) 。                                      | `5000`                |
| `remove_backup_files_after_failure`                 | Bool   | 如果 `BACKUP` 命令失败，ClickHouse 会尝试删除失败前已复制到备份中的文件；否则会保留这些已复制的文件。 | `true`                |
| `sync_period_ms`                                    | UInt64 | 协调备份/恢复的同步周期 (毫秒) 。                                           | `5000`                |
| `test_inject_sleep`                                 | Bool   | 用于测试的休眠设置                                                     | `false`               |
| `test_randomize_order`                              | Bool   | 如果为 true，则会出于测试目的随机打乱某些操作的顺序。                                 | `false`               |
| `zookeeper_path`                                    | String | 使用 `ON CLUSTER` 子句时，ZooKeeper 中用于存储备份和恢复元数据的 path。            | `/clickhouse/backups` |

该设置默认配置如下：

```xml
<backups>
    ....
</backups>
```

<div id="background_schedule_pool_log">
  ## background_schedule_pool_log
</div>

包含通过各种后台线程池执行的所有后台任务的信息。

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

<div id="bcrypt_workfactor">
  ## bcrypt_workfactor
</div>

使用 [Bcrypt 算法](https://wildlyinaccurate.com/bcrypt-choosing-a-work-factor/) 的 `bcrypt_password` 身份验证类型的工作因子。
工作因子定义了计算哈希值以及验证密码所需的计算量和时间。

```xml
<bcrypt_workfactor>12</bcrypt_workfactor>
```

:::warning
对于需要高频身份验证的应用，
由于 bcrypt 在较高工作因子下计算开销较大，
请考虑改用其他身份验证方法。
:::

<div id="table_engines_require_grant">
  ## table_engines_require_grant
</div>

如果设置为 true，用户必须具备相应的 grant，才能使用特定引擎创建表，例如 `GRANT TABLE ENGINE ON TinyLog to user`。

:::note
默认情况下，出于向后兼容考虑，使用特定表引擎创建表时会忽略 grant；不过，你可以将此项设置为 true 来更改这一行为。
:::

<div id="builtin_dictionaries_reload_interval">
  ## builtin_dictionaries_reload_interval
</div>

重新加载内置字典前的时间间隔 (以秒为单位) 。

ClickHouse 每隔 x 秒重新加载一次内置字典，因此无需重启服务器即可“实时”编辑字典。

**示例**

```xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

<div id="compression">
  ## 压缩
</div>

[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 引擎表的数据压缩设置。

:::note
如果你刚开始使用 ClickHouse，建议不要更改此设置。
:::

**配置模板**：

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

**`<case>` 字段**:

* `min_part_size` – 数据分区片段的最小大小。
* `min_part_size_ratio` – 数据分区片段大小与表大小的比率。
* `method` – 压缩方法。允许的值：`lz4`、`lz4hc`、`zstd`、`deflate_qpl`。
* `level` – 压缩级别。请参见 [编解码器](/zh/sql-reference/statements/create/table#general-purpose-codecs)。

:::note
你可以配置多个 `<case>` 段。
:::

**条件满足时的操作**:

* 如果某个数据分区片段匹配了一组条件，ClickHouse 会使用指定的压缩方法。
* 如果某个数据分区片段匹配多组条件，ClickHouse 会使用第一组匹配的条件。

:::note
如果某个数据分区片段未满足任何条件，ClickHouse 会使用 `lz4` 压缩。
:::

**示例**

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

<div id="encryption">
  ## encryption
</div>

配置用于获取供[加密编解码器](/zh/sql-reference/statements/create/table#encryption-codecs)使用的密钥的命令。密钥 (或多个密钥) 应写入环境变量，或在配置文件中设置。

密钥可以是十六进制值，也可以是长度为 16 字节的字符串。

**示例**

从配置中加载：

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
不建议将密钥存储在配置文件中，这样并不安全。你可以将密钥移到安全磁盘上的单独配置文件中，然后在 `config.d/` 文件夹中放置一个指向该配置文件的符号链接。
:::

当密钥为十六进制格式时，可从配置中加载：

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

从环境变量中加载私钥：

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

这里，`current_key_id` 用于设置当前的加密密钥，而所有指定的密钥都可用于解密。

以下每种方法都可用于多个密钥：

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

这里，`current_key_id` 表示当前用于加密的密钥。

此外，用户还可以添加长度必须为 12 字节的 nonce (默认情况下，加密和解密过程使用由零字节组成的 nonce) ：

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

或者也可以设置为十六进制：

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
上述所有内容同样适用于 `aes_256_gcm_siv` (但密钥长度必须为 32 字节) 。
:::

<div id="error_log">
  ## error_log
</div>

默认已禁用。

**启用**

如需手动启用错误历史记录收集 [`system.error_log`](../../operations/system-tables/error_log.md)，请创建 `/etc/clickhouse-server/config.d/error_log.xml`，内容如下：

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

**禁用**

要禁用 `error_log` 设置，请创建以下文件 `/etc/clickhouse-server/config.d/disable_error_log.xml`，内容如下：

```xml
<clickhouse>
    <error_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="custom_settings_prefixes">
  ## custom_settings_prefixes
</div>

[自定义设置](/zh/operations/settings/query-level#custom_settings)使用的前缀列表。
多个前缀之间应以逗号分隔。

**示例**

```xml
<custom_settings_prefixes>SQL_</custom_settings_prefixes>
```

**另请参阅**

* [自定义设置](/zh/operations/settings/query-level#custom_settings)

<div id="core_dump">
  ## core_dump
</div>

配置 core 转储文件大小的软限制。

:::note
硬限制需通过系统工具进行配置。
:::

**示例**

```xml
<core_dump>
     <size_limit>1073741824</size_limit>
</core_dump>
```

<div id="default_profile">
  ## default_profile
</div>

默认的设置 profile。profile 位于设置 `user_config` 指定的文件中。

**示例**

```xml
<default_profile>default</default_profile>
```

<div id="dictionaries_config">
  ## dictionaries_config
</div>

字典配置文件的路径。

路径：

* 指定绝对路径，或相对于服务器配置文件的路径。
* 路径中可以包含通配符 * 和 ?。

另请参见：

* &quot;[字典](../../sql-reference/statements/create/dictionary/overview.md)&quot;。

**示例**

```xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

<div id="user_defined_executable_functions_config">
  ## user_defined_executable_functions_config
</div>

可执行用户自定义函数的配置文件路径。

路径：

* 指定绝对路径，或相对于服务器配置文件的路径。
* 路径可以包含通配符 `*` 和 `?`。

另请参见：

* &quot;[可执行用户自定义函数](/zh/sql-reference/functions/udf#executable-user-defined-functions)。&quot;

**示例**

```xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

<div id="graphite">
  ## graphite
</div>

将数据发送到 [Graphite](https://github.com/graphite-project)。

设置：

* `host` – Graphite 服务器。
* `port` – Graphite 服务器上的端口。
* `interval` – 发送时间间隔，以秒为单位。
* `timeout` – 发送数据的超时时间，以秒为单位。
* `root_path` – 键前缀。
* `metrics` – 发送来自 [system.metrics](/zh/operations/system-tables/metrics) 表的数据。
* `events` – 发送来自 [system.events](/zh/operations/system-tables/events) 表的 delta 数据，这些数据是在该时间段内累计的。
* `events_cumulative` – 发送来自 [system.events](/zh/operations/system-tables/events) 表的累计数据。
* `asynchronous_metrics` – 发送来自 [system.asynchronous&#95;metrics](/zh/operations/system-tables/asynchronous_metrics) 表的数据。

你可以配置多个 `<graphite>` 配置段。例如，可以用它按不同的时间间隔发送不同的数据。

**示例**

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

<div id="graphite_rollup">
  ## graphite_rollup
</div>

Graphite 数据精简设置。

更多信息，请参见 [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md)。

**示例**

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

<div id="http_handlers">
  ## http_handlers
</div>

允许使用自定义 HTTP handler。
要添加新的 http handler，只需新增一个 `<rule>`。
规则会按定义顺序从上到下进行检查，
第一个匹配的规则会运行对应的 handler。
没有匹配条件 (只有 `handler`) 的规则会匹配所有请求；由于规则会按顺序检查，
因此这类规则仅适合作为最后放置的 fallback。

以下 settings 可通过 sub-tags 进行配置 (除 `handler` 外，所有这些 sub-tags 都是可选的) ：

| Sub-tags             | 定义                                                                                                                                              |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                | 用于匹配请求 URL 路径。匹配时会忽略 query string                                                                                                               |
| `url_prefix`         | 用于将请求 URL 路径与基础路径匹配：即该路径本身，或在路径段边界下位于其下方的任何内容 (例如，&#39;/api/v1&#39; 可匹配 /api/v1、/api/v1/ 和 /api/v1/write，但不匹配 /api/v1beta) 。匹配时会忽略 query string |
| `url_regexp`         | 用于将请求 URL 路径与 regular expression 进行匹配。匹配时会忽略 query string                                                                                       |
| `full_url`           | 用于匹配完整的请求 URL `scheme://host:port/path`。匹配时会忽略 query string，且 host 是 connection IP 地址 (不是 `Host` 请求头)                                           |
| `full_url_prefix`    | 用于将完整的请求 URL `scheme://host:port/path` 与 base URL `scheme://host:port/base_path` 在路径段边界上进行匹配 (参见 `url_prefix`) 。匹配时会忽略 query string             |
| `full_url_regexp`    | 用于将完整的请求 URL `scheme://host:port/path` 与 regular expression 进行匹配。匹配时会忽略 query string                                                            |
| `methods`            | 用于匹配请求方法，可以使用逗号分隔多个 method 匹配项                                                                                                                  |
| `headers`            | 用于匹配请求头，匹配每个子元素 (子元素名称即请求头名称)                                                                                                                   |
| `headers_regexp`     | 与 `headers` 类似，但每个子元素的值都会按 regular expression 进行匹配                                                                                              |
| `empty_query_string` | 检查 URL 中是否不存在 query string                                                                                                                      |
| `handler`            | 请求 handler (必需)                                                                                                                                 |

:::note
除了 `url_regexp`、`full_url_regexp` 和 `headers_regexp` 外，你也可以在 `url`、`full_url` 或 `headers` 中使用 `regex:` 前缀来编写 regular expression (例如 `<url>regex:/api/.*</url>`) 。这样做仍然受支持以保持 backward compatibility，但已废弃：建议优先使用专用的 `url_regexp`、`full_url_regexp` 和 `headers_regexp` sub-tags。
:::

`handler` 包含以下 settings，这些 settings 可通过 sub-tags 进行配置：

| Sub-tags           | 定义                                                                                                                                     |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| `url`              | redirect 的目标位置                                                                                                                         |
| `type`             | 支持的 types：static、dynamic&#95;query&#95;handler、predefined&#95;query&#95;handler、redirect                                               |
| `status`           | 与 static type 配合使用，表示响应状态码                                                                                                             |
| `query_param_name` | 与 dynamic&#95;query&#95;handler type 配合使用，提取并执行 HTTP request params 中与 `<query_param_name>` 对应的值                                       |
| `query`            | 与 predefined&#95;query&#95;handler type 配合使用，在调用 handler 时执行查询                                                                         |
| `content_type`     | 与 static type 配合使用，表示响应 content-type                                                                                                   |
| `response_content` | 与 static type 配合使用，表示发送给 client 的 Response 内容；当使用前缀 &#39;file://&#39; 或 &#39;config://&#39; 时，会从 file 或 configuration 中读取内容并发送给 client |

除规则列表外，还可以指定 `<defaults/>`，表示启用所有默认 handlers。

示例：

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

<div id="http_server_default_response">
  ## http_server_default_response
</div>

访问 ClickHouse HTTP(s) 服务器时默认显示的页面。
默认值为 &quot;Ok.&quot; (末尾带有换行符)

**示例**

访问 `http://localhost: http_port` 时，将打开 `https://tabix.io/`。

```xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

<div id="http_options_response">
  ## http_options_response
</div>

用于向 `OPTIONS` HTTP 请求的响应中添加请求头。
`OPTIONS` 方法用于发起 CORS 预检请求。

更多信息，请参见 [OPTIONS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/OPTIONS)。

示例：

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

<div id="hsts_max_age">
  ## hsts_max_age
</div>

HSTS 的过期时长，单位为秒。

:::note
值为 `0` 表示 ClickHouse 禁用 HSTS。若设置为正数，则会启用 HSTS，且 max-age 为您设置的值。
:::

**示例**

```xml
<hsts_max_age>600000</hsts_max_age>
```

<div id="interserver_listen_host">
  ## interserver_listen_host
</div>

限制可在 ClickHouse 服务器之间交换数据的主机。
如果使用 Keeper，相同的限制也会应用于不同 Keeper 实例之间的通信。

:::note
默认情况下，该值等同于 [`listen_host`](#listen_host) 设置。
:::

**示例**

```xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

类型：

默认值：

<div id="interserver_http_credentials">
  ## interserver_http_credentials
</div>

在[复制](../../engines/table-engines/mergetree-family/replication.md)过程中，用于连接其他服务器的用户名和密码。此外，服务器还会使用这些凭据对其他副本进行身份验证。
因此，集群中的所有副本必须使用相同的 `interserver_http_credentials`。

:::note

* 默认情况下，如果省略 `interserver_http_credentials` 部分，则在复制过程中不使用身份验证。
* `interserver_http_credentials` 设置与 ClickHouse 客户端凭据[配置](../../interfaces/client.md#configuration_files)无关。
* 这些凭据同时适用于通过 `HTTP` 和 `HTTPS` 进行的复制。
  :::

以下设置可通过子标签配置：

* `user` — 用户名。
* `password` — 密码。
* `allow_empty` — 如果为 `true`，则即使已设置凭据，也允许其他副本在不进行身份验证的情况下连接。如果为 `false`，则会拒绝未进行身份验证的连接。默认值：`false`。
* `old` — 包含凭据轮换期间使用的旧 `user` 和 `password`。可以指定多个 `old` 部分。

**凭据轮换**

ClickHouse 支持动态轮换 interserver 凭据，无需同时停止所有副本来更新配置。凭据可以分几个步骤进行更改。

要启用身份验证，请将 `interserver_http_credentials.allow_empty` 设置为 `true` 并添加凭据。这样既允许带身份验证的连接，也允许不带身份验证的连接。

```xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

在完成所有副本的配置后，将 `allow_empty` 设为 `false`，或删除此设置。这样会强制使用新凭据进行身份验证。

要更改现有凭据，请将用户名和密码移至 `interserver_http_credentials.old` 部分，并用新值更新 `user` 和 `password`。此时，服务器会使用新凭据连接到其他副本，同时接受使用新旧任一凭据建立的连接。

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

当所有副本都已应用新的凭据后，即可移除旧凭据。

<div id="ldap_servers">
  ## ldap_servers
</div>

在此处列出 LDAP 服务器及其连接参数，以便：

* 将其用作专用本地用户的身份验证器；这些用户指定的是 `ldap` 身份验证机制，而不是 `password`
* 将其用作远程用户目录。

以下设置可通过子标签配置：

| Setting                        | Description                                                                                                                                                                                |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `bind_dn`                      | 用于构造绑定 DN 的模板。在每次身份验证尝试期间，系统会将模板中所有 `\{user_name\}` 子串替换为实际用户名，从而生成最终的 DN。                                                                                                                 |
| `enable_tls`                   | 用于控制是否与 LDAP 服务器建立安全连接的标志。纯文本 (`ldap://`) 协议请指定 `no` (不推荐) 。LDAP over SSL/TLS (`ldaps://`) 协议请指定 `yes` (推荐，也是默认值) 。旧版 StartTLS 协议请指定 `starttls` (先使用纯文本 (`ldap://`) 协议，再升级为 TLS) 。         |
| `host`                         | LDAP 服务器的主机名或 IP；此参数必填，且不能为空。                                                                                                                                                              |
| `port`                         | LDAP 服务器端口；如果 `enable_tls` 设置为 true，则默认值为 636，否则为 `389`。                                                                                                                                   |
| `tls_ca_cert_dir`              | 包含 CA 证书的目录路径。                                                                                                                                                                             |
| `tls_ca_cert_file`             | CA 证书文件路径。                                                                                                                                                                                 |
| `tls_cert_file`                | 证书文件路径。                                                                                                                                                                                    |
| `tls_cipher_suite`             | 允许使用的密码套件 (采用 OpenSSL 表示法) 。                                                                                                                                                               |
| `tls_key_file`                 | 证书密钥文件路径。                                                                                                                                                                                  |
| `tls_minimum_protocol_version` | SSL/TLS 的最低协议版本。可接受的值为：`ssl2`、`ssl3`、`tls1.0`、`tls1.1`、`tls1.2` (默认值) 。                                                                                                                    |
| `tls_require_cert`             | SSL/TLS 对端证书验证行为。可接受的值为：`never`、`allow`、`try`、`demand` (默认值) 。                                                                                                                             |
| `user_dn_detection`            | 包含 LDAP 搜索参数的部分，用于检测已绑定用户的实际 user DN。这主要用于服务器为 Active Directory 时，在后续角色映射所使用的搜索过滤器中。生成的 user DN 会在所有允许使用 `\{user_dn\}` 子串替换的地方被使用。默认情况下，user DN 与 bind DN 相同；但执行搜索后，它会更新为实际检测到的 user DN 值。 |
| `verification_cooldown`        | 成功绑定后的一段时间 (以秒为单位) 内，系统会假定该用户在所有后续请求中都已成功通过身份验证，而无需联系 LDAP 服务器。指定 `0` (默认值) 可禁用缓存，并强制每次身份验证请求都联系 LDAP 服务器。                                                                                 |

设置 `user_dn_detection` 可通过子标签配置：

| Setting         | Description                                                                                                                                           |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `base_dn`       | 用于构造 LDAP 搜索 base DN 的模板。在 LDAP 搜索期间，系统会将模板中所有 `\{user_name\}` 和 `\{bind_dn\}` 子串替换为实际用户名和 bind DN，从而生成最终的 DN。                                        |
| `scope`         | LDAP 搜索的范围。可接受的值为：`base`、`one_level`、`children`、`subtree` (默认值) 。                                                                                     |
| `search_filter` | 用于构造 LDAP 搜索过滤器的模板。在 LDAP 搜索期间，系统会将模板中所有 `\{user_name\}`、`\{bind_dn\}` 和 `\{base_dn\}` 子串替换为实际用户名、bind DN 和 base DN，从而生成最终的过滤器。请注意，在 XML 中必须正确转义特殊字符。 |

示例：

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

示例 (典型的 Active Directory，已配置用于后续角色映射的 user DN 检测) ：

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

<div id="listen_host">
  ## listen_host
</div>

限制允许发起请求的来源主机。如果希望服务器响应来自所有主机的请求，请指定 `::`。

示例：

```xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

<div id="logger">
  ## 日志记录器
</div>

日志消息的位置和格式。

**键**：

| Key                          | Description                                                                                                    |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `async`                      | 当为 `true` (默认值) 时，将以异步方式记录日志 (每个输出通道一个后台线程) 。否则，会在调用 LOG 的线程中记录日志                                              |
| `async_queue_max_size`       | 使用异步日志时，队列中等待 flush 的消息最大保留数量。超出的消息将被丢弃                                                                        |
| `console`                    | 启用向控制台输出日志。设置为 `1` 或 `true` 即可启用。如果 ClickHouse 未以守护进程模式运行，默认值为 `1`，否则为 `0`。                                    |
| `console_log_level`          | 控制台输出的日志级别。默认使用 `level`。                                                                                       |
| `console_shutdown_log_level` | Shutdown level 用于在 server 关闭时设置控制台日志级别。                                                                        |
| `console_startup_log_level`  | Startup level 用于在 server 启动时设置控制台日志级别。启动完成后，日志级别会恢复为 `console_log_level` 设置                                    |
| `count`                      | 轮转策略：ClickHouse 最多保留多少个历史日志文件。                                                                                 |
| `errorlog`                   | 错误日志文件的路径。                                                                                                     |
| `formatting.type`            | 控制台输出的日志格式。目前仅支持 `json`。                                                                                       |
| `level`                      | 日志级别。可接受的值：`none` (关闭日志记录) 、`fatal`、`critical`、`error`、`warning`、`notice`、`information`、`debug`、`trace`、`test` |
| `log`                        | 日志文件的路径。                                                                                                       |
| `rotation`                   | 轮转策略：控制何时轮转日志文件。轮转可以基于大小、时间或两者结合。示例：100M、daily、100M,daily。一旦日志文件超过指定大小，或达到指定时间间隔，就会将其重命名并归档，同时创建新的日志文件。        |
| `shutdown_level`             | Shutdown level 用于在 server 关闭时设置根 logger 级别。                                                                    |
| `size`                       | 轮转策略：日志文件的最大大小 (以字节为单位) 。一旦日志文件大小超过该阈值，就会将其重命名并归档，同时创建新的日志文件。                                                  |
| `startup_level`              | Startup level 用于在 server 启动时设置根 logger 级别。启动完成后，日志级别会恢复为 `level` 设置                                            |
| `stream_compress`            | 使用 LZ4 压缩日志消息。设置为 `1` 或 `true` 即可启用。                                                                           |
| `syslog_level`               | 输出到 syslog 时使用的日志级别。                                                                                           |
| `use_syslog`                 | 同时将日志输出转发到 syslog。                                                                                             |

**日志格式说明符**

`log` 和 `errorLog` 路径中的文件名支持以下格式说明符，用于生成最终文件名 (目录部分不支持这些说明符) 。

“Example”列显示的是 `2023-07-06 18:32:07` 时的输出。

| 说明符  | 描述                                                                                                             | 示例                         |
| ---- | -------------------------------------------------------------------------------------------------------------- | -------------------------- |
| `%%` | 字面量 %                                                                                                          | `%`                        |
| `%n` | 换行符                                                                                                            |                            |
| `%t` | 水平制表符                                                                                                          |                            |
| `%Y` | 以十进制数表示的年份，例如 2017                                                                                             | `2023`                     |
| `%y` | 以十进制数表示的年份后 2 位 (范围 [00,99])                                                                                   | `23`                       |
| `%C` | 以十进制数表示的年份前 2 位 (范围 [00,99])                                                                                   | `20`                       |
| `%G` | 四位 [ISO 8601 week-based year](https://en.wikipedia.org/wiki/ISO_8601#Week_dates)，即包含指定周的年份。通常仅在与 `%V` 搭配使用时有意义 | `2023`                     |
| `%g` | [ISO 8601 week-based year](https://en.wikipedia.org/wiki/ISO_8601#Week_dates) 的后 2 位，即包含指定周的年份。                | `23`                       |
| `%b` | 缩写月份名称，例如 Oct (取决于区域设置)                                                                                        | `Jul`                      |
| `%h` | `%b` 的同义形式                                                                                                     | `Jul`                      |
| `%B` | 完整月份名称，例如 October (取决于区域设置)                                                                                    | `July`                     |
| `%m` | 以十进制数表示的月份 (范围 [01,12])                                                                                        | `07`                       |
| `%U` | 以十进制数表示的一年中的周数 (星期日为一周的第一天)  (范围 [00,53])                                                                      | `27`                       |
| `%W` | 以十进制数表示的一年中的周数 (星期一为一周的第一天)  (范围 [00,53])                                                                      | `27`                       |
| `%V` | ISO 8601 周数 (范围 [01,53])                                                                                       | `27`                       |
| `%j` | 以十进制数表示的一年中的第几天 (范围 [001,366])                                                                                 | `187`                      |
| `%d` | 以零填充的十进制数表示的月中日期 (范围 [01,31]) 。个位数前补零。                                                                         | `06`                       |
| `%e` | 以空格填充的十进制数表示的月中日期 (范围 [1,31]) 。个位数前补一个空格。                                                                      | `&nbsp; 6`                 |
| `%a` | 缩写星期名称，例如 Fri (取决于区域设置)                                                                                        | `Thu`                      |
| `%A` | 完整星期名称，例如 Friday (取决于区域设置)                                                                                     | `Thursday`                 |
| `%w` | 以整数表示的星期几，其中星期日为 0 (范围 [0-6])                                                                                  | `4`                        |
| `%u` | 以十进制数表示的星期几，其中星期一为 1 (ISO 8601 格式)  (范围 [1-7])                                                                 | `4`                        |
| `%H` | 以十进制数表示的小时，24 小时制 (范围 [00-23])                                                                                 | `18`                       |
| `%I` | 以十进制数表示的小时，12 小时制 (范围 [01,12])                                                                                 | `06`                       |
| `%M` | 以十进制数表示的分钟 (范围 [00,59])                                                                                        | `32`                       |
| `%S` | 以十进制数表示的秒 (范围 [00,60])                                                                                         | `07`                       |
| `%c` | 标准日期时间字符串，例如 Sun Oct 17 04:41:13 2010 (取决于区域设置)                                                                | `Thu Jul  6 18:32:07 2023` |
| `%x` | 本地化日期表示 (取决于区域设置)                                                                                              | `07/06/23`                 |
| `%X` | 本地化时间表示，例如 18:40:20 或 6:40:20 PM (取决于区域设置)                                                                     | `18:32:07`                 |
| `%D` | 简短的 MM/DD/YY 日期，等同于 %m/%d/%y                                                                                   | `07/06/23`                 |
| `%F` | 简短的 YYYY-MM-DD 日期格式，等同于 %Y-%m-%d                                                                               | `2023-07-06`               |
| `%r` | 本地化的 12 小时制时间 (取决于区域设置)                                                                                        | `06:32:07 PM`              |
| `%R` | 等同于 &quot;%H:%M&quot;                                                                                          | `18:32`                    |
| `%T` | 等同于 &quot;%H:%M:%S&quot; (ISO 8601 时间格式)                                                                       | `18:32:07`                 |
| `%p` | 本地化的上午/下午标记 (取决于区域设置)                                                                                          | `PM`                       |
| `%z` | ISO 8601 格式的 UTC 偏移量 (例如 -0430) ；如果时区信息不可用，则不显示任何字符                                                            | `+0800`                    |
| `%Z` | 与区域设置相关的时区名称或缩写；如果时区信息不可用，则不显示任何字符                                                                             | `Z AWST `                  |

**示例**

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

要仅在控制台输出日志消息：

```xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**按级别覆盖**

可以单独覆盖各个日志名称的日志级别。例如，可屏蔽日志记录器 &quot;Backup&quot; 和 &quot;RBAC&quot; 的所有消息。

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

要将日志消息额外写入 syslog：

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

`<syslog>` 的键：

| Key        | Description                                                                                                                                                                                 |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `address`  | syslog 地址，格式为 `host\[:port\]`。如果省略，则使用本地守护进程。                                                                                                                                               |
| `hostname` | 发送日志的主机名 (可选) 。                                                                                                                                                                             |
| `facility` | syslog 的 [facility 关键字](https://en.wikipedia.org/wiki/Syslog#Facility)。必须以带有 `LOG_` 前缀的大写形式指定，例如 `LOG_USER`、`LOG_DAEMON`、`LOG_LOCAL3` 等。默认值：如果指定了 `address`，则为 `LOG_USER`；否则为 `LOG_DAEMON`。 |
| `format`   | 日志消息格式。可选值：`bsd` 和 `syslog.`                                                                                                                                                                |

**日志格式**

您可以指定要输出到控制台日志中的日志格式。目前仅支持 JSON。

**示例**

以下是输出 JSON 日志的示例：

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

要启用 JSON 日志支持，请使用以下配置片段：

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

**重命名 JSON 日志中的键**

可以通过修改 `<names>` 标签内各项标签的值来更改键名。例如，要将 `DATE_TIME` 改为 `MY_DATE_TIME`，可以使用 `<date_time>MY_DATE_TIME</date_time>`。

**省略 JSON 日志中的键**

可以通过将相应属性注释掉来省略日志属性。例如，如果不希望日志输出 `query_id`，可以将 `<query_id>` 标签注释掉。

<div id="send_crash_reports">
  ## send_crash_reports
</div>

用于将崩溃报告发送给 ClickHouse 核心开发团队的设置。

如果启用该设置，我们将非常感谢，尤其是在预生产环境中。

键：

| Key                   | Description                                                                        |
| --------------------- | ---------------------------------------------------------------------------------- |
| `enabled`             | 用于启用此功能的布尔值标志，默认为 `true`。设置为 `false` 可避免发送崩溃报告。                                    |
| `endpoint`            | 你可以覆盖用于发送崩溃报告的端点 URL。                                                              |
| `send_logical_errors` | `LOGICAL_ERROR` 类似于 `assert`，表示 ClickHouse 中存在缺陷。此布尔值标志用于控制是否发送这类异常 (默认值：`true`) 。 |

**推荐用法**

```xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

<div id="ssh_server">
  ## ssh_server
</div>

主机密钥的公钥部分会在首次连接时写入 SSH 客户端的 known&#95;hosts 文件。

主机密钥配置默认未启用。
取消注释这些主机密钥配置，并提供相应 SSH 密钥的路径以启用它们：

示例：

```xml
<ssh_server>
    <host_rsa_key>path_to_the_ssh_key</host_rsa_key>
    <host_ecdsa_key>path_to_the_ssh_key</host_ecdsa_key>
    <host_ed25519_key>path_to_the_ssh_key</host_ed25519_key>
</ssh_server>
```

<div id="tcp_ssh_port">
  ## tcp_ssh_port
</div>

SSH 服务器的端口，用户可通过该端口借助 PTY 中的嵌入式客户端进行交互式连接并执行查询。

示例：

```xml
<tcp_ssh_port>9022</tcp_ssh_port>
```

<div id="storage_configuration">
  ## storage_configuration
</div>

支持对存储进行多磁盘配置。

存储配置结构如下：

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

<div id="configuration-of-disks">
  ### `disks` 的配置
</div>

`disks` 的配置结构如下：

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

上述子标签为 `disks` 定义了以下设置：

| Setting                 | Description                                         |
| ----------------------- | --------------------------------------------------- |
| `<disk_name_N>`         | 磁盘名称，必须唯一。                                          |
| `path`                  | 用于存储 server 数据 (`data` 和 `shadow` 目录) 的路径。应以 `/` 结尾 |
| `keep_free_space_bytes` | 磁盘上预留可用空间的大小。                                    |

:::note
磁盘的顺序无关紧要。
:::

<div id="configuration-of-policies">
  ### 策略配置
</div>

上述子标签为 `policies` 定义了以下配置项：

| Setting                      | Description                                                                                                                                                   |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `policy_name_N`              | 策略名称。策略名称必须唯一。                                                                                                                                                |
| `volume_name_N`              | 卷名称。卷名称必须唯一。                                                                                                                                                  |
| `disk`                       | 卷中的磁盘。                                                                                                                                                        |
| `max_data_part_size_bytes`   | 此卷中任一磁盘可存放的数据块的最大大小。如果合并后生成的数据块预计会大于 `max_data_part_size_bytes`，则该数据块将写入下一个卷。此功能本质上允许你将新的/较小的数据块存储在热 (SSD) 卷上，并在其变大后移动到冷 (HDD) 卷上。如果策略中只有一个卷，请不要使用此选项。        |
| `move_factor`                | 卷上可用空间的占比。如果可用空间低于该值，数据将开始传输到下一个卷 (如果存在) 。传输时，数据块会按大小从大到小 (降序) 排序，并选择总大小足以满足 `move_factor` 条件的数据块；如果所有数据块的总大小仍不足，则会移动所有数据块。                                   |
| `perform_ttl_move_on_insert` | 禁用在插入时移动 TTL 已过期的数据。默认情况下 (启用时) ，如果插入的一部分数据根据 TTL 移动规则已经过期，则会立即将其移动到该规则指定的卷/磁盘。如果目标卷/磁盘较慢 (例如 S3) ，这可能会显著降低插入速度。如果禁用，则已过期的数据部分会先写入默认卷，然后立即移动到规则中为过期 TTL 指定的卷。 |
| `load_balancing`             | 磁盘均衡策略，`round_robin` 或 `least_used`。                                                                                                                          |
| `least_used_ttl_ms`          | 设置更新所有磁盘可用空间的超时时间 (以毫秒为单位)  (`0` - 始终更新，`-1` - 从不更新，默认值为 `60000`) 。注意，如果磁盘仅由 ClickHouse 使用，且文件系统不会在运行时动态扩缩容，则可以使用 `-1`。在其他所有情况下都不建议这样做，因为最终会导致空间分配不准确。        |
| `prefer_not_to_merge`        | 禁止在此卷上合并数据 parts。注意：这可能带来负面影响并导致性能下降。启用此设置时 (不要这样做) ，将禁止在此卷上执行数据合并 (这并不好) 。这可用于控制 ClickHouse 与慢速磁盘的交互方式。我们建议完全不要使用它。                                          |
| `volume_priority`            | 定义填充卷时的优先级 (顺序) 。值越小，优先级越高。参数值必须是自然数，并且必须无空缺地覆盖从 1 到 N 的范围 (N 是指定的最大参数值) 。                                                                                    |

对于 `volume_priority`：

* 如果所有卷都有此参数，则按指定顺序确定优先级。
* 如果只有&#95;部分&#95;卷有此参数，则未设置该参数的卷优先级最低。已设置该参数的卷按标签值确定优先级，其余卷之间的优先级则由它们在配置文件中的描述顺序决定。
* 如果&#95;没有&#95;任何卷设置此参数，则它们的顺序由其在配置文件中的描述顺序决定。
* 卷的优先级不能相同。

<div id="macros">
  ## macros
</div>

用于复制表的参数替换。

如果不使用复制表，则可省略。

更多信息，请参阅[创建复制表](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables)一节。

**示例**

```xml
<macros incl="macros" optional="true" />
```

<div id="replica_group_name">
  ## replica_group_name
</div>

Replicated 数据库的副本组名称。

Replicated 数据库 创建的集群将由同一组中的副本组成。
DDL 查询只会等待同一组中的副本。

默认为空。

**示例**

```xml
<replica_group_name>backups</replica_group_name>
```

<div id="max_session_timeout">
  ## max_session_timeout
</div>

最大会话超时时长 (单位：秒) 。

示例：

```xml
<max_session_timeout>3600</max_session_timeout>
```

<div id="merge_tree">
  ## merge_tree
</div>

针对 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表的细化设置。

更多信息，请参阅 MergeTreeSettings.h 头文件。

**示例**

```xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

<div id="metric_log">
  ## metric_log
</div>

默认已禁用。

**启用**

如需手动启用指标历史采集 [`system.metric_log`](../../operations/system-tables/metric_log.md)，请创建 `/etc/clickhouse-server/config.d/metric_log.xml`，内容如下：

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

**禁用**

要禁用 `metric_log` 设置，请创建以下文件 `/etc/clickhouse-server/config.d/disable_metric_log.xml`，内容如下：

```xml
<clickhouse>
    <metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="replicated_merge_tree">
  ## replicated_merge_tree
</div>

用于 [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表的微调设置。此设置的优先级更高。

更多信息，请参阅 MergeTreeSettings.h 头文件。

**示例**

```xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

<div id="opentelemetry_span_log">
  ## opentelemetry_span_log
</div>

[`opentelemetry_span_log`](../system-tables/opentelemetry_span_log.md) 系统表的相关设置。

<SystemLogParameters />

示例：

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

<div id="openSSL">
  ## openSSL
</div>

SSL 客户端/服务器配置。

SSL 支持由 `libpoco` 库提供。可用的配置选项见 [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)。默认值可在 [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp) 中找到。

服务器/客户端设置的键名：

| 选项                            | 说明                                                                                                                                                                                                                                                                     | 默认值                                                                                        |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `cacheSessions`               | 启用或禁用会话缓存。必须与 `sessionIdContext` 搭配使用。可接受的值：`true`、`false`。                                                                                                                                                                                                            | `false`                                                                                    |
| `caConfig`                    | 包含受信任 CA 证书的文件或目录路径。如果指向文件，该文件必须采用 PEM 格式，并且可以包含多个 CA 证书。如果指向目录，则该目录中每个 CA 证书都必须对应一个 .pem 文件。文件名会根据 CA subject name 哈希值进行查找。详见 [SSL&#95;CTX&#95;load&#95;verify&#95;locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html) 的 man 手册页。 |                                                                                            |
| `certificateFile`             | PEM 格式的客户端/服务器证书文件的路径。如果 `privateKeyFile` 中已包含证书，则可省略。                                                                                                                                                                                                                 |                                                                                            |
| `cipherList`                  | OpenSSL 支持的加密方式。                                                                                                                                                                                                                                                       | `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`                                                  |
| `disableProtocols`            | 禁止使用的协议。                                                                                                                                                                                                                                                               |                                                                                            |
| `extendedVerification`        | 如果启用，则验证证书的 CN 或 SAN 是否与 peer 主机名匹配。                                                                                                                                                                                                                                   | `false`                                                                                    |
| `fips`                        | 启用 OpenSSL FIPS 模式。如果该库使用的 OpenSSL 版本支持 FIPS，则支持此功能。                                                                                                                                                                                                                   | `false`                                                                                    |
| `invalidCertificateHandler`   | 用于处理无效证书验证的类 (CertificateHandler 的子类) 。例如：`<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>`。                                                                                                                            | `RejectCertificateHandler`                                                                 |
| `loadDefaultCAFile`           | 是否使用 OpenSSL 的内置 CA 证书。ClickHouse 假定，内置 CA 证书位于文件 `/etc/ssl/cert.pem` (或目录 `/etc/ssl/certs`) 中，或者位于环境变量 `SSL_CERT_FILE` (或 `SSL_CERT_DIR`) 指定的文件 (或目录) 中。                                                                                                              | `true`                                                                                     |
| `preferServerCiphers`         | 优先使用客户端选择的服务器密码套件。                                                                                                                                                                                                                                                     | `false`                                                                                    |
| `privateKeyFile`              | 包含 PEM 证书私钥的文件路径。该文件可同时包含私钥和证书。                                                                                                                                                                                                                                        |                                                                                            |
| `privateKeyPassphraseHandler` | 类 (PrivateKeyPassphraseHandler 的子类) ，用于获取访问私钥所需的口令短语。例如：`<privateKeyPassphraseHandler>`、`<name>KeyFileHandler</name>`、`<options><password>test</password></options>`、`</privateKeyPassphraseHandler>`。                                                                 | `KeyConsoleHandler`                                                                        |
| `requireTLSv1`                | 要求使用 TLSv1 连接。可接受的值：`true`、`false`。                                                                                                                                                                                                                                    | `false`                                                                                    |
| `requireTLSv1_1`              | 要求使用 TLSv1.1 连接。可接受值：`true`、`false`。                                                                                                                                                                                                                                   | `false`                                                                                    |
| `requireTLSv1_2`              | 要求使用 TLSv1.2 连接。可接受的值：`true`、`false`。                                                                                                                                                                                                                                  | `false`                                                                                    |
| `sessionCacheSize`            | 服务器可缓存的会话最大数量。值为 `0` 表示会话数量不受限制。                                                                                                                                                                                                                                       | [1024*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978) |
| `sessionIdContext`            | 一组唯一的随机字符，server 会将其附加到每个生成的标识符上。字符串长度不得超过 `SSL_MAX_SSL_SESSION_ID_LENGTH`。始终建议设置此参数，因为无论是 server 缓存了 session，还是 client 请求了缓存，它都有助于避免出现问题。                                                                                                                             | `$\{application.name\}`                                                                    |
| `sessionTimeout`              | 服务器缓存会话的时长，以小时为单位。                                                                                                                                                                                                                                                     | `2`                                                                                        |
| `verificationDepth`           | 验证链的最大长度。如果证书链长度超过设定值，验证将失败。                                                                                                                                                                                                                                           | `9`                                                                                        |
| `verificationMode`            | 节点证书的校验方式。详细信息请参见 [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) 类的说明。可选值：`none`、`relaxed`、`strict`、`once`。                                                                                                | `relaxed`                                                                                  |

**设置示例：**

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

<div id="part_log">
  ## part_log
</div>

记录与 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 相关的日志事件，例如添加数据或合并数据时产生的事件。你可以使用这些日志来模拟合并算法，并比较它们的特性。你还可以将合并过程可视化。

这些记录会写入 [system.part&#95;log](/zh/operations/system-tables/part_log) 表中，而不是单独的文件。你可以在 `table` 参数中配置该表的名称 (见下文) 。

<SystemLogParameters />

**示例**

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

<div id="processors_profile_log">
  ## processors_profile_log
</div>

[`processors_profile_log`](../system-tables/processors_profile_log.md) 系统表的相关设置。

<SystemLogParameters />

默认设置如下：

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

<div id="prometheus">
  ## prometheus
</div>

暴露指标数据，供 [Prometheus](https://prometheus.io) 抓取。

设置：

* `endpoint` – 供 Prometheus 服务器抓取指标的 HTTP 端点。必须以 &#39;/&#39; 开头。
* `port` – `endpoint` 使用的端口。
* `metrics` – 暴露 [system.metrics](/zh/operations/system-tables/metrics) 表中的指标。
* `events` – 暴露 [system.events](/zh/operations/system-tables/events) 表中的指标。
* `asynchronous_metrics` – 暴露 [system.asynchronous&#95;metrics](/zh/operations/system-tables/asynchronous_metrics) 表中的当前指标值。
* `errors` - 暴露自上次服务器重启以来按错误代码统计的错误数量。此信息也可从 [system.errors](/zh/operations/system-tables/errors) 中获取。

**示例**

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

检查 (将 `127.0.0.1` 替换为你的 ClickHouse 服务器的 IP 地址或主机名) ：

```bash
curl 127.0.0.1:9363/metrics
```

<div id="query_log">
  ## query_log
</div>

用于记录在启用 [log&#95;queries=1](../../operations/settings/settings.md) 设置时接收到的查询。

查询会记录到 [system.query&#95;log](/zh/operations/system-tables/query_log) 表中，而不是单独的文件中。你可以在 `table` 参数中更改该表的名称 (见下文) 。

<SystemLogParameters />

如果该表不存在，ClickHouse 会创建它。如果 ClickHouse 服务器更新后查询日志的结构发生变化，旧结构的表会被重命名，并自动创建一个新表。

**示例**

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

<div id="query_metric_log">
  ## query_metric_log
</div>

默认处于禁用状态。

**启用**

如需手动启用指标历史记录收集 [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md)，请创建 `/etc/clickhouse-server/config.d/query_metric_log.xml`，内容如下：

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

**禁用**

如需禁用 `query_metric_log` 设置，请创建以下文件 `/etc/clickhouse-server/config.d/disable_query_metric_log.xml`，内容如下：

```xml
<clickhouse>
    <query_metric_log remove="1" />
</clickhouse>
```

<SystemLogParameters />

<div id="query_cache">
  ## query_cache
</div>

[查询缓存](../query-cache.md)的配置。

可用设置如下：

| 设置                        | 描述                            | 默认值          |
| ------------------------- | ----------------------------- | ------------ |
| `max_entries`             | 缓存中可存储的 `SELECT` 查询结果的最大数量。   | `1024`       |
| `max_entry_size_in_bytes` | 可保存到缓存中的 `SELECT` 查询结果的最大字节数。 | `1048576`    |
| `max_entry_size_in_rows`  | 可保存到缓存中的 `SELECT` 查询结果的最大行数。  | `30000000`   |
| `max_size_in_bytes`       | 缓存的最大字节数。`0` 表示禁用查询缓存。        | `1073741824` |

:::note

* 修改后的设置会立即生效。
* 查询缓存的数据分配在 DRAM 中。如果内存较为紧张，请将 `max_size_in_bytes` 设为较小的值，或直接禁用查询缓存。
  :::

**示例**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

<div id="query_thread_log">
  ## query_thread_log
</div>

用于记录在启用 [log&#95;query&#95;threads=1](/zh/operations/settings/settings#log_query_threads) 设置后接收到的查询线程的日志。

查询会记录到 [system.query&#95;thread&#95;log](/zh/operations/system-tables/query_thread_log) 表中，而不是单独的文件中。你可以在 `table` 参数中更改该表的名称 (见下文) 。

<SystemLogParameters />

如果该表不存在，ClickHouse 会创建它。如果在 ClickHouse server 更新后，查询线程日志的结构发生了变化，则旧结构的表会被重命名，并自动创建一个新表。

**示例**

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

<div id="query_views_log">
  ## query_views_log
</div>

用于记录通过 [log&#95;query&#95;views=1](/zh/operations/settings/settings#log_query_views) 设置接收的查询所涉及视图 (live、materialized 等) 的设置。

查询会记录到 [system.query&#95;views&#95;log](/zh/operations/system-tables/query_views_log) 表中，而不是单独的文件中。您可以在 `table` 参数中更改该表的名称 (见下文) 。

<SystemLogParameters />

如果该表不存在，ClickHouse 会创建它。如果在更新 ClickHouse server 时，查询视图日志的结构发生了变化，则会将旧结构的表重命名，并自动创建一个新表。

**示例**

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

<div id="text_log">
  ## text_log
</div>

用于记录文本消息的系统表 [text&#95;log](/zh/operations/system-tables/text_log) 的设置。

<SystemLogParameters />

此外：

| 设置      | 说明                            | 默认值     |
| ------- | ----------------------------- | ------- |
| `level` | 将存储到表中的最高消息级别 (默认为 `Trace`) 。 | `Trace` |

**示例**

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

<div id="trace_log">
  ## trace_log
</div>

[trace&#95;log](/zh/operations/system-tables/trace_log) 系统表操作的相关设置。

<SystemLogParameters />

默认的服务器配置文件 `config.xml` 包含以下设置部分：

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

<div id="asynchronous_insert_log">
  ## asynchronous_insert_log
</div>

用于为记录异步插入的 [asynchronous&#95;insert&#95;log](/zh/operations/system-tables/asynchronous_insert_log) 系统表进行配置的设置。

<SystemLogParameters />

**示例**

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

<div id="crash_log">
  ## crash_log
</div>

[crash&#95;log](../../operations/system-tables/crash_log.md) 系统表操作的设置。

以下设置可通过子标签进行配置：

| Setting                            | Description                                                                                                       | Default             | Note                                                              |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------- | ------------------- | ----------------------------------------------------------------- |
| `buffer_size_rows_flush_threshold` | 行数阈值。达到该阈值后，将在后台启动把日志刷新到磁盘的操作。                                                                                    | `max_size_rows / 2` |                                                                   |
| `database`                         | 数据库名称。                                                                                                            |                     |                                                                   |
| `engine`                           | 系统表的 [MergeTree 引擎定义](/zh/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-creating-a-table)。 |                     | 如果定义了 `partition_by` 或 `order_by`，则不能使用。如果未指定，默认选择 `MergeTree`    |
| `flush_interval_milliseconds`      | 将数据从内存缓冲区刷新到表的时间间隔。                                                                                               | `7500`              |                                                                   |
| `flush_on_crash`                   | 设置在发生崩溃时是否应将日志转储到磁盘。                                                                                              | `false`             |                                                                   |
| `max_size_rows`                    | 日志的最大行数。当未刷新的日志数量达到 `max_size_rows` 时，日志将被转储到磁盘。                                                                  | `1024`              |                                                                   |
| `order_by`                         | 系统表的[自定义排序键](/zh/engines/table-engines/mergetree-family/mergetree#order_by)。如果定义了 `engine`，则不能使用。                    |                     | 如果为系统表指定了 `engine`，则应直接在 &#39;engine&#39; 内指定 `order_by` 参数       |
| `partition_by`                     | 系统表的[自定义分区键](/zh/engines/table-engines/mergetree-family/custom-partitioning-key.md)。                                 |                     | 如果为系统表指定了 `engine`，则应直接在 &#39;engine&#39; 内指定 `partition_by` 参数   |
| `reserved_size_rows`               | 为日志预分配的内存行数。                                                                                                      | `1024`              |                                                                   |
| `settings`                         | 控制 MergeTree 行为的[附加参数](/zh/engines/table-engines/mergetree-family/mergetree/#settings) (可选) 。                        |                     | 如果为系统表指定了 `engine`，则应直接在 &#39;engine&#39; 内指定 `settings` 参数       |
| `storage_policy`                   | 该表使用的存储策略名称 (可选) 。                                                                                                |                     | 如果为系统表指定了 `engine`，则应直接在 &#39;engine&#39; 内指定 `storage_policy` 参数 |
| `table`                            | 系统表名称。                                                                                                            |                     |                                                                   |
| `ttl`                              | 指定表的 [TTL](/zh/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-ttl)。                         |                     | 如果为系统表指定了 `engine`，则应直接在 &#39;engine&#39; 内指定 `ttl` 参数            |

默认服务器配置文件 `config.xml` 包含以下设置部分：

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

<div id="custom_cached_disks_base_directory">
  ## custom_cached_disks_base_directory
</div>

此设置用于指定自定义 (通过 SQL 创建的) 缓存磁盘的缓存路径。
对于自定义磁盘，`custom_cached_disks_base_directory` 的优先级高于 `filesystem_caches_path` (位于 `filesystem_caches_path.xml` 中) ；
如果前者未设置，则使用后者。
文件系统缓存的设置路径必须位于该目录内，
否则将抛出异常，从而阻止该磁盘被创建。

:::note
这不会影响在较早版本中创建、且之后对服务器执行了升级的磁盘。
在这种情况下，不会抛出异常，以便服务器能够成功启动。
:::

示例：

```xml
<custom_cached_disks_base_directory>/var/lib/clickhouse/caches/</custom_cached_disks_base_directory>
```

<div id="backup_log">
  ## backup_log
</div>

用于记录 `BACKUP` 和 `RESTORE` 操作的 [backup&#95;log](../../operations/system-tables/backup_log.md) 系统表设置。

<SystemLogParameters />

**示例**

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

<div id="blob_storage_log">
  ## blob_storage_log
</div>

[`blob_storage_log`](../system-tables/blob_storage_log.md) 系统表的设置。

<SystemLogParameters />

示例：

```xml
<blob_storage_log>
    <database>system</database
    <table>blob_storage_log</table
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds
    <ttl>event_date + INTERVAL 30 DAY</ttl>
</blob_storage_log>
```

<div id="query_masking_rules">
  ## query_masking_rules
</div>

基于 Regexp 的规则会在查询以及所有日志消息写入服务器日志、
[`system.query_log`](/zh/operations/system-tables/query_log)、[`system.text_log`](/zh/operations/system-tables/text_log)、[`system.processes`](/zh/operations/system-tables/processes) 表，以及发送给客户端的日志之前应用。这样可以防止 SQL 查询中的敏感数据 (如姓名、电子邮件、个人标识符或信用卡号) 泄露到日志中。

**示例**

```xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

**配置字段**:

| Setting   | Description                   |
| --------- | ----------------------------- |
| `name`    | 规则名称 (可选)                     |
| `regexp`  | 与 RE2 兼容的正则表达式 (必填)           |
| `replace` | 用于替换敏感数据的替换字符串 (可选，默认值为六个星号)  |

脱敏规则会应用于整个查询 (以防格式错误或无法解析的查询泄露敏感数据) 。

[`system.events`](/zh/operations/system-tables/events) 表中有一个计数器 `QueryMaskingRulesMatch`，用于统计查询脱敏规则匹配的总次数。

对于分布式查询，每台服务器都必须分别配置，否则传递到其他
节点的子查询将会在未脱敏的情况下存储。

<div id="remote_servers">
  ## remote_servers
</div>

用于 [Distributed](../../engines/table-engines/special/distributed.md) 表引擎和 `cluster` 表函数的集群配置。

**示例**

```xml
<remote_servers incl="clickhouse_remote_servers" />
```

有关 `incl` 属性的值，请参见“[配置文件](/zh/operations/configuration-files)”一节。

**另请参阅**

* [skip&#95;unavailable&#95;shards](../../operations/settings/settings.md#skip_unavailable_shards)
* [集群发现](../../operations/cluster-discovery.md)
* [Replicated 数据库引擎](../../engines/database-engines/replicated.md)

<div id="remote_url_allow_hosts">
  ## remote_url_allow_hosts
</div>

允许在与 URL 相关的存储引擎和表函数中使用的 hosts 列表。

添加带有 `\<host\>` xml tag 的 host 时：

* 必须与 URL 中的写法完全一致，因为在 DNS 解析之前就会检查 host 名称。例如：`<host>clickhouse.com</host>`
* 如果在 URL 中显式指定了端口，则会将 host:port 作为整体进行检查。例如：`<host>clickhouse.com:80</host>`
* 如果指定 host 时未带端口，则允许该 host 的任意端口。例如：如果指定了 `<host>clickhouse.com</host>`，则允许 `clickhouse.com:20` (FTP)、`clickhouse.com:80` (HTTP)、`clickhouse.com:443` (HTTPS) 等。
* 如果将 host 指定为 IP 地址，则会按 URL 中指定的形式进行检查。例如：`[2a02:6b8:a::a]`。
* 如果存在重定向且已启用重定向支持，则会检查每一次重定向 (location field) 。

例如：

```sql
<remote_url_allow_hosts>
    <host>clickhouse.com</host>
</remote_url_allow_hosts>
```

<div id="timezone">
  ## 时区
</div>

服务器的时区。

使用 UTC 时区或地理位置的 IANA 标识符指定 (例如，Africa/Abidjan) 。

当将日期时间字段输出为文本格式 (打印到屏幕上或写入文件中) 时，或者从字符串获取日期时间时，时区是 String 与 日期时间 格式之间转换所必需的。此外，对于处理时间和日期的函数，如果其输入参数中未提供时区，也会使用该时区。

**示例**

```xml
<timezone>Asia/Istanbul</timezone>
```

**另请参阅**

* [session&#95;timezone](../settings/settings.md#session_timezone)

<div id="tcp_port">
  ## tcp_port
</div>

用于通过 TCP 协议与客户端进行通信的端口。

**示例**

```xml
<tcp_port>9000</tcp_port>
```

<div id="tcp_port_secure">
  ## tcp_port_secure
</div>

用于与客户端进行安全通信的 TCP 端口。请结合 [OpenSSL](#openssl) 配置使用。

**默认值**

```xml
<tcp_port_secure>9440</tcp_port_secure>
```

<div id="mysql_port">
  ## mysql_port
</div>

用于通过 MySQL 协议与客户端通信的端口。

:::note

* 正整数表示要监听的端口号
* 空值用于禁用通过 MySQL 协议与客户端进行通信。
  :::

**示例**

```xml
<mysql_port>9004</mysql_port>
```

<div id="postgresql_port">
  ## postgresql_port
</div>

用于通过 PostgreSQL 协议与客户端通信的端口。

:::note

* 正整数表示要监听的端口号
* 空值用于禁用通过 PostgreSQL 协议与客户端通信。
  :::

**示例**

```xml
<postgresql_port>9005</postgresql_port>
```

<div id="url_scheme_mappers">
  ## url_scheme_mappers
</div>

用于将缩写或符号形式的 URL 前缀转换为完整 URL 的配置。

示例：

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

<div id="user_defined_path">
  ## user_defined_path
</div>

存放用户自定义文件的目录。供 SQL 用户自定义函数 [SQL User Defined Functions](/zh/sql-reference/functions/udf) 使用。

**示例**

```xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

<div id="users_config">
  ## users_config
</div>

包含以下内容的文件的路径：

* 用户配置。
* 访问权限。
* 设置 profile。
* 配额设置。

**示例**

```xml
<users_config>users.xml</users_config>
```

<div id="access_control_improvements">
  ## access_control_improvements
</div>

访问控制系统中可选改进项的设置。

| Setting                                         | Description                                                                                                                                                                                                                                                                 | Default |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `on_cluster_queries_require_cluster_grant`      | 设置 `ON CLUSTER` 查询是否需要 `CLUSTER` 授权。                                                                                                                                                                                                                                        | `true`  |
| `role_cache_expiration_time_seconds`            | 设置角色自上次访问后在角色缓存中保留的秒数。                                                                                                                                                                                                                                                      | `600`   |
| `select_from_information_schema_requires_grant` | 设置 `SELECT * FROM information_schema.<table>` 是否需要授权，或者是否可由任意用户执行。如果设为 true，则此查询需要 `GRANT SELECT ON information_schema.<table>`，与普通表相同。                                                                                                                                     | `true`  |
| `select_from_system_db_requires_grant`          | 设置 `SELECT * FROM system.<table>` 是否需要授权，或者是否可由任意用户执行。如果设为 true，则此查询需要 `GRANT SELECT ON system.<table>`，与非系统表相同。例外情况：少数系统表 (`tables`、`columns`、`databases`，以及一些常量表，如 `one`、`contributors`) 仍可供所有人访问；此外，如果已授予 `SHOW` 权限 (例如 `SHOW USERS`) ，则对应的系统表 (即 `system.users`) 也可以访问。 | `true`  |
| `settings_constraints_replace_previous`         | 设置 settings profile 中某个设置的约束，是否会覆盖该设置此前的约束 (定义在其他 profile 中) 的作用，包括新约束未设置的字段。它还会启用 `changeable_in_readonly` 约束类型。                                                                                                                                                           | `true`  |
| `table_engines_require_grant`                   | 设置使用特定表引擎创建表是否需要授权。                                                                                                                                                                                                                                                         | `false` |
| `throw_on_unmatched_row_policies`               | 设置从表中读取时，如果该表存在行策略，但当前用户没有任何匹配的行策略，是否应抛出异常。                                                                                                                                                                                                                                 | `false` |
| `users_without_row_policies_can_read_rows`      | 设置没有允许型行策略的用户是否仍可使用 `SELECT` 查询读取行。例如，如果有两个用户 A 和 B，且仅为 A 定义了行策略，那么当此设置为 true 时，用户 B 将看到所有行；当此设置为 false 时，用户 B 将看不到任何行。                                                                                                                                                     | `true`  |

示例：

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

<div id="s3queue_log">
  ## s3queue_log
</div>

`s3queue_log` 系统表的相关设置。

<SystemLogParameters />

默认设置如下：

```xml
<s3queue_log>
    <database>system</database>
    <table>s3queue_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</s3queue_log>
```

<div id="dead_letter_queue">
  ## dead_letter_queue
</div>

&#39;dead&#95;letter&#95;queue&#39; 系统表的设置项。

<SystemLogParameters />

默认设置如下：

```xml
<dead_letter_queue>
    <database>system</database>
    <table>dead_letter</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</dead_letter_queue>
```

<div id="zookeeper">
  ## zookeeper
</div>

包含允许 ClickHouse 与 [ZooKeeper](http://zookeeper.apache.org/) 集群交互的设置。使用复制表时，ClickHouse 会使用 ZooKeeper 存储副本的元数据。如果不使用复制表，则可以省略这一节参数。

以下设置可通过子标签进行配置：

| Setting                                         | Description                                                                                                                                                                                                               |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `node`                                          | ZooKeeper 端点。你可以设置多个端点。例如：`<node index="1"><host>example_host</host><port>2181</port></node>`。`index` 属性指定尝试连接 ZooKeeper 集群时的节点顺序。                                                                                        |
| `operation_timeout_ms`                          | 单个操作的最大超时时间，单位为毫秒。                                                                                                                                                                                                        |
| `session_timeout_ms`                            | 客户端会话的最大超时时间，单位为毫秒。                                                                                                                                                                                                       |
| `root` (optional)                               | 作为 ClickHouse 服务器 使用的各 znode 的根 znode。                                                                                                                                                                                 |
| `fallback_session_lifetime.min` (optional)      | 当主节点不可用时，连接到备用节点的 ZooKeeper 会话生存期下限 (负载均衡) 。以秒为单位设置。默认值：3 小时。                                                                                                                                                             |
| `fallback_session_lifetime.max` (optional)      | 当主节点不可用时，连接到备用节点的 ZooKeeper 会话生存期上限 (负载均衡) 。以秒为单位设置。默认值：6 小时。                                                                                                                                                             |
| `identity` (optional)                           | 访问请求的 znode 时，ZooKeeper 所需的用户和密码。                                                                                                                                                                                         |
| `use_compression` (optional)                    | 如果设置为 true，则在 Keeper 协议中启用压缩。                                                                                                                                                                                             |
| `use_xid_64` (optional)                         | 启用 64 位事务 ID。设置为 `true` 以启用扩展事务 ID 格式。默认值：`false`。                                                                                                                                                                        |
| `pass_opentelemetry_tracing_context` (optional) | 启用将 OpenTelemetry 追踪上下文传播到 Keeper 请求。启用后，会为 Keeper 操作创建追踪 span，从而实现跨 ClickHouse 和 Keeper 的分布式链路追踪。更多详情请参见 [Tracing ClickHouse Keeper Requests](/zh/operations/opentelemetry#tracing-clickhouse-keeper-requests)。默认值：`false`。 |

此外还有 `zookeeper_load_balancing` 设置 (可选) ，可用于选择 ZooKeeper 节点的选择算法：

| Algorithm Name                   | Description                                                  |
| -------------------------------- | ------------------------------------------------------------ |
| `random`                         | 随机选择一个 ZooKeeper 节点。                                         |
| `in_order`                       | 选择第一个 ZooKeeper 节点；如果它不可用，则选择第二个，依此类推。                       |
| `nearest_hostname`               | 选择主机名与服务器主机名最相似的 ZooKeeper 节点，主机名按名称前缀进行比较。                  |
| `hostname_levenshtein_distance`  | 与 nearest&#95;hostname 类似，但使用 Levenshtein distance 的方式比较主机名。 |
| `hostname_longest_common_prefix` | 与 nearest&#95;hostname 类似，但会优先选择其主机名与服务器主机名具有最长公共前缀的节点。      |
| `hostname_longest_common_suffix` | 与 nearest&#95;hostname 类似，但会优先选择其主机名与服务器主机名具有最长公共后缀的节点。      |
| `first_or_random`                | 选择第一个 ZooKeeper 节点；如果它不可用，则从其余 ZooKeeper 节点中随机选择一个。          |
| `round_robin`                    | 选择第一个 ZooKeeper 节点；如果发生重连，则选择下一个。                            |

**配置示例**

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
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / hostname_longest_common_prefix / hostname_longest_common_suffix / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
    <!-- Optional. Enable 64-bit transaction IDs. -->
    <use_xid_64>false</use_xid_64>
    <!-- Optional. Enable OpenTelemetry tracing context propagation. -->
    <pass_opentelemetry_tracing_context>false</pass_opentelemetry_tracing_context>
</zookeeper>
```

**另请参阅**

* [复制](../../engines/table-engines/mergetree-family/replication.md)
* [ZooKeeper 程序员指南](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
* [ClickHouse 与 ZooKeeper 之间的可选安全通信](/zh/operations/ssl-zookeeper)

<div id="use_minimalistic_part_header_in_zookeeper">
  ## use_minimalistic_part_header_in_zookeeper
</div>

在 ZooKeeper 中存储数据分区片段头信息的方法。此设置仅适用于 [`MergeTree`](/zh/engines/table-engines/mergetree-family) 家族。可通过以下方式指定：

**在 `config.xml` 文件的 [merge&#95;tree](#merge_tree) 部分中全局指定**

ClickHouse 会将此设置应用于服务器上的所有表。你可以随时更改此设置。设置变更后，现有表的行为也会随之改变。

**为每个表单独指定**

创建表时，指定相应的[引擎设置](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。对于已设置该选项的现有表，即使全局设置发生变化，其行为也不会改变。

**可能的值**

* `0` — 功能关闭。
* `1` — 功能开启。

如果 [`use_minimalistic_part_header_in_zookeeper = 1`](#use_minimalistic_part_header_in_zookeeper)，那么[复制](../../engines/table-engines/mergetree-family/replication.md)表会使用单个 `znode` 以紧凑方式存储数据分区片段的头信息。如果表包含很多列，这种存储方式会显著减少存储在 ZooKeeper 中的数据量。

:::note
应用 `use_minimalistic_part_header_in_zookeeper = 1` 后，你将无法把 ClickHouse 服务器 降级到不支持此设置的版本。升级 cluster 中各服务器上的 ClickHouse 时请务必小心。不要一次性升级所有服务器。更安全的做法是先在测试环境中，或仅在 cluster 中少数几台服务器上测试 ClickHouse 的新版本。

已使用此设置存储的数据分区片段头信息，无法恢复为之前的 (非紧凑) 表示形式。
:::

<div id="distributed_ddl">
  ## distributed_ddl
</div>

管理在集群上执行[分布式 DDL 查询](../../sql-reference/distributed-ddl.md) (`CREATE`、`DROP`、`ALTER`、`RENAME`) 。
仅在启用 [ZooKeeper](/zh/operations/server-configuration-parameters/settings#zookeeper) 时生效。

`<distributed_ddl>` 中可配置的设置包括：

| Setting                | Description                                          | Default Value               |
| ---------------------- | ---------------------------------------------------- | --------------------------- |
| `cleanup_delay_period` | 如果距离上次清理已超过 `cleanup_delay_period` 秒，则在收到新节点事件后开始清理。 | `60` 秒                      |
| `max_tasks_in_queue`   | 队列中可容纳的最大任务数。                                        | `1,000`                     |
| `path`                 | Keeper 中 DDL 查询的 `task_queue` 路径                     |                             |
| `pool_size`            | 可同时运行的 `ON CLUSTER` 查询数量                             |                             |
| `profile`              | 用于执行 DDL 查询的 profile                                 |                             |
| `task_max_lifetime`    | 如果节点的存活时间超过此值，则删除该节点。                                | `7 * 24 * 60 * 60` (一周的秒数)  |

**示例**

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

<div id="access_control_path">
  ## access_control_path
</div>

ClickHouse 服务器用于存储通过 SQL 命令创建的用户和角色配置的文件夹路径。

**另请参阅**

* [访问控制与账户管理](/zh/operations/access-rights#access-control-usage)

<div id="allow_plaintext_password">
  ## allow_plaintext_password
</div>

设置是否允许使用明文密码类型 (不安全) 。

```xml
<allow_plaintext_password>1</allow_plaintext_password>
```

<div id="allow_no_password">
  ## allow_no_password
</div>

设置是否允许使用不安全的 no&#95;password 类型密码。

```xml
<allow_no_password>1</allow_no_password>
```

<div id="allow_implicit_no_password">
  ## allow_implicit_no_password
</div>

禁止创建未设置密码的用户，除非明确指定 &#39;IDENTIFIED WITH no&#95;password&#39;。

```xml
<allow_implicit_no_password>1</allow_implicit_no_password>
```

<div id="default_session_timeout">
  ## default_session_timeout
</div>

默认会话超时时长 (单位：秒) 。

```xml
<default_session_timeout>60</default_session_timeout>
```

<div id="default_password_type">
  ## default_password_type
</div>

设置在 `CREATE USER u IDENTIFIED BY 'p'` 这类查询中自动使用的密码类型。

可接受的值有：

* `plaintext_password`
* `sha256_password`
* `double_sha1_password`
* `bcrypt_password`

```xml
<default_password_type>sha256_password</default_password_type>
```

<div id="user_directories">
  ## user_directories
</div>

配置文件中包含以下设置的部分：

* 预定义用户的配置文件路径。
* 通过 SQL 命令创建的用户的存储目录路径。
* 通过 SQL 命令创建的用户的存储及复制所在的 ZooKeeper 节点路径。

如果指定了此部分，则不会使用 [users&#95;config](/zh/operations/server-configuration-parameters/settings#users_config) 和 [access&#95;control&#95;path](../../operations/server-configuration-parameters/settings.md#access_control_path) 中指定的路径。

`user_directories` 部分可以包含任意数量的条目，条目的顺序表示它们的优先次序 (条目越靠前，优先次序越高) 。

**示例**

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

用户、角色、行策略、配额和 profile 也可以存储在 ZooKeeper 中：

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

您也可以定义 `memory` 和 `ldap` 部分：`memory` 表示信息仅存储在内存中，不写入磁盘；`ldap` 表示将信息存储在 LDAP 服务器上。

要将 LDAP 服务器添加为未在本地定义的用户的远程用户目录，请定义一个单独的 `ldap` 部分，并使用以下设置：

| Setting  | Description                                                                                                                 |
| -------- | --------------------------------------------------------------------------------------------------------------------------- |
| `roles`  | 该部分包含一个本地定义的角色列表，这些角色会分配给从 LDAP 服务器检索到的每个用户。如果未指定任何角色，用户在完成身份验证后将无法执行任何操作。如果在身份验证时，所列的任何角色未在本地定义，则此次身份验证尝试将失败，就像提供的密码不正确一样。 |
| `server` | `ldap_servers` 配置部分中定义的 LDAP 服务器名称之一。此参数为必填项，且不能为空。                                                                         |

**示例**

```xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

<div id="top_level_domains_list">
  ## top_level_domains_list
</div>

定义要添加的自定义顶级域名列表，其中每个条目的格式均为 `<name>/path/to/file</name>`。

例如：

```xml
<top_level_domains_lists>
    <public_suffix_list>/path/to/public_suffix_list.dat</public_suffix_list>
</top_level_domains_lists>
```

另请参阅：

* 函数 [`cutToFirstSignificantSubdomainCustom`](../../sql-reference/functions/url-functions.md/#cutToFirstSignificantSubdomainCustom) 及其变体，
  它接受自定义 TLD 列表的名称，并返回域名中从顶级子域到第一个有效子域的那一部分。

<div id="proxy">
  ## 代理
</div>

为 HTTP 和 HTTPS 请求定义代理服务器。目前，S3 存储、S3 表函数和 URL 函数支持此功能。

定义代理服务器有三种方式：

* 环境变量
* 代理列表
* 远程代理解析器。

也支持使用 `no_proxy` 为特定主机绕过代理服务器。

**环境变量**

`http_proxy` 和 `https_proxy` 环境变量允许你为指定协议设置代理服务器。如果你已在系统中设置了它们，通常即可直接生效。

如果某种协议只有一个代理服务器，并且该代理服务器不会变化，这是最简单的方法。

**代理列表**

这种方法允许你为某种协议指定一个或多个代理服务器。如果定义了多个代理服务器，ClickHouse 会以轮询方式使用不同的代理，在各服务器之间平衡负载。如果某种协议有多个代理服务器，并且代理服务器列表不会变化，这是最简单的方法。

**配置模板**

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

在下方选项卡中选择一个父字段以查看其子字段：

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | 字段        | 描述                |
    | --------- | ----------------- |
    | `<http>`  | 一个或多个 HTTP 代理的列表  |
    | `<https>` | 一个或多个 HTTPS 代理的列表 |
  </TabItem>

  <TabItem value="http_https" label="<http> and <https>">
    | 字段      | 描述      |
    | ------- | ------- |
    | `<uri>` | 代理的 URI |
  </TabItem>
</Tabs>

**远程代理解析器**

代理服务器可能会动态变化。在这种情况下，
你可以定义一个解析器的端点。ClickHouse 会向该端点发送
一个空的 GET 请求，远程解析器应返回代理主机。
ClickHouse 将使用它通过以下模板构造代理 URI：`\{proxy_scheme\}://\{proxy_host\}:{proxy_port}`

**配置模板**

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

在下方选项卡中选择父字段以查看其子字段：

<Tabs>
  <TabItem value="proxy" label="<proxy>" default>
    | 字段        | 说明               |
    | --------- | ---------------- |
    | `<http>`  | 一个或多个解析器*的列表 |
    | `<https>` | 一个或多个解析器*的列表 |
  </TabItem>

  <TabItem value="http_https" label="<http> 和 <https>">
    | 字段           | 说明            |
    | ------------ | ------------- |
    | `<resolver>` | 解析器的端点及其他详细信息 |

    :::note
    你可以有多个 `<resolver>` 元素，但对于给定协议，
    只会使用第一个 `<resolver>`。该协议的其他 `<resolver>`
    元素都会被忽略。这意味着负载均衡
    (如果需要) 应由远程解析器来实现。
    :::
  </TabItem>

  <TabItem value="resolver" label="<resolver>">
    | 字段                   | 说明                                                                                     |
    | -------------------- | -------------------------------------------------------------------------------------- |
    | `<endpoint>`         | 代理解析器的 URI                                                                             |
    | `<proxy_scheme>`     | 最终代理 URI 的协议，可以是 `http` 或 `https`。                                                     |
    | `<proxy_port>`       | 代理解析器的端口号                                                                              |
    | `<proxy_cache_time>` | ClickHouse 缓存来自解析器的值的时长 (单位为秒) 。将此值设置为 `0` 会使 ClickHouse 在每次发起 HTTP 或 HTTPS 请求时都联系解析器。 |
  </TabItem>
</Tabs>

**优先次序**

代理设置按以下顺序确定：

| 顺序 | 设置      |
| -- | ------- |
| 1. | 远程代理解析器 |
| 2. | 代理列表    |
| 3. | 环境变量    |

ClickHouse 将检查该请求协议对应的最高优先级解析器类型。如果未定义，
则会继续检查优先级依次降低的下一个解析器类型，直到环境解析器为止。
这也意味着可以混合使用多种解析器类型。

<div id="disable_tunneling_for_https_requests_over_http_proxy">
  ## disable_tunneling_for_https_requests_over_http_proxy
</div>

默认情况下，通过 `HTTP` 代理发送 `HTTPS` 请求时会使用隧道 (即 `HTTP CONNECT`) 。可使用此设置将其禁用。

**no&#95;proxy**

默认情况下，所有请求都会经过代理。若要对特定主机禁用代理，必须设置 `no_proxy` 变量。
对于列表解析器和远程解析器，可以在 `<proxy>` 子句中设置；对于环境解析器，则可将其设置为环境变量。
它支持 IP 地址、域名、子域名，以及用于完全绕过代理的 `'*'` 通配符。前导点会像 curl 一样被去掉。

**示例**

以下配置会绕过发往 `clickhouse.cloud` 及其所有子域名 (例如 `auth.clickhouse.cloud`) 的代理请求。
GitLab 也是如此，即使它带有前导点。`gitlab.com` 和 `about.gitlab.com` 都会绕过代理。

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

<div id="workload_path">
  ## workload_path
</div>

用于存放所有 `CREATE WORKLOAD` 和 `CREATE RESOURCE` 查询的目录。默认使用 server 工作目录下的 `/workload/` 文件夹。

**示例**

```xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**另请参阅**

* [工作负载层级](/zh/operations/workload-scheduling.md#workloads)
* [workload&#95;zookeeper&#95;path](#workload_zookeeper_path)

<div id="workload_zookeeper_path">
  ## workload_zookeeper_path
</div>

指向 ZooKeeper 节点的路径，用作存储所有 `CREATE WORKLOAD` 和 `CREATE RESOURCE` 查询的位置。为保证一致性，所有 SQL 定义都作为这个单一 znode 的值存储。默认情况下不使用 ZooKeeper，这些定义会存储在[磁盘](#workload_path)上。

**示例**

```xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**另请参阅**

* [工作负载层级](/zh/operations/workload-scheduling.md#workloads)
* [workload&#95;path](#workload_path)

<div id="zookeeper_log">
  ## zookeeper_log
</div>

[`zookeeper_log`](/zh/operations/system-tables/zookeeper_log) 系统表的设置项。

以下设置可通过子标签进行配置：

<SystemLogParameters />

**示例**

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