---
description: '该引擎可与亚马逊 S3 生态集成，并支持流式导入。它与 Kafka 和 RabbitMQ 引擎类似，但提供了 S3 特有的功能。'
sidebar_label: 'S3Queue'
sidebar_position: 181
slug: /engines/table-engines/integrations/s3queue
title: 'S3Queue 表引擎'
doc_type: 'reference'
---

import ScalePlanFeatureBadge from '@theme/badges/ScalePlanFeatureBadge'

<div id="s3queue-table-engine">
  # S3Queue 表引擎
</div>

该引擎可与 [亚马逊 S3](https://aws.amazon.com/s3/) 生态集成，并支持流式导入。该引擎类似于 [Kafka](../../../engines/table-engines/integrations/kafka.md) 和 [RabbitMQ](../../../engines/table-engines/integrations/rabbitmq.md) 引擎，但还提供 S3 特有的功能。

务必理解 [S3Queue 实现的原始 PR](https://github.com/ClickHouse/ClickHouse/pull/49086/files#diff-e1106769c9c8fbe48dd84f18310ef1a250f2c248800fde97586b3104e9cd6af8R183) 中的这条说明：当 `MATERIALIZED VIEW` 连接到该引擎时，S3Queue 表引擎便会在后台开始采集数据。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE s3_queue_engine_table (name String, value UInt32)
    ENGINE = S3Queue(path, [NOSIGN, | aws_access_key_id, aws_secret_access_key,] format, [compression], [headers], [extra_credentials])
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    [loading_retries = 10,]
    [processing_threads_num = 16,]
    [parallel_inserts = false,]
    [enable_logging_to_queue_log = true,]
    [last_processed_path = "",]
    [tracked_files_limit = 1000,]
    [tracked_file_ttl_sec = 0,]
    [polling_min_timeout_ms = 1000,]
    [polling_max_timeout_ms = 600000,]
    [polling_backoff_ms = 30000,]
    [cleanup_interval_min_ms = 60000,]
    [cleanup_interval_max_ms = 60000,]
    [buckets = 0,]
    [list_objects_batch_size = 1000,]
    [enable_hash_ring_filtering = 0,]
    [max_processed_files_before_commit = 100,]
    [max_processed_rows_before_commit = 0,]
    [max_processed_bytes_before_commit = 0,]
    [max_processing_time_sec_before_commit = 0,]
```

:::warning
在 `24.7` 之前，除 `mode`、`after_processing` 和 `keeper_path` 外，所有设置都必须使用 `s3queue_` 前缀。
:::

**引擎参数**

`S3Queue` 的参数与 `S3` 表引擎支持的参数相同。请参阅[此处](../../../engines/table-engines/integrations/s3.md#parameters)的参数部分。

**示例**

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered';
```

使用命名集合：

```xml
<clickhouse>
    <named_collections>
        <s3queue_conf>
            <url>https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </s3queue_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue(s3queue_conf, format = 'CSV', compression_method = 'gzip')
SETTINGS
    mode = 'ordered';
```

<div id="settings">
  ## 设置
</div>

如需获取为该表配置的设置列表，请使用 `system.s3_queue_settings` 表。该功能自 `24.10` 起可用。

:::note 设置名称 (24.7+)
从 24.7 版本开始，指定 S3Queue 设置时，可以使用或不使用 `s3queue_` 前缀：

* **现代语法** (24.7+) ：`processing_threads_num`、`tracked_file_ttl_sec` 等。
* **旧语法** (所有版本) ：`s3queue_processing_threads_num`、`s3queue_tracked_file_ttl_sec` 等。

在 24.7+ 中，这两种形式都受支持。本页中的示例使用的是不带前缀的现代语法。
:::

<div id="mode">
  ### 模式
</div>

可能的值：

* unordered — 在 unordered 模式下，所有已处理文件的集合会通过 ZooKeeper 中的持久节点来跟踪。
* ordered — 在 ordered 模式下，文件按字典序处理。这意味着，如果名为 &#39;BBB&#39; 的文件在某个时间点已经处理过，之后又有一个名为 &#39;AA&#39; 的文件被添加到存储桶中，那么它会被忽略。只有成功消费文件的最大名称 (按字典序) ，以及加载失败后将被重试的文件名称，会存储在 ZooKeeper 中。

默认值：在 24.6 之前的版本中为 `ordered`。从 24.6 开始，不再有默认值，必须手动指定该设置。对于在更早版本中创建的表，出于兼容性考虑，默认值仍将保持为 `Ordered`。

<div id="after_processing">
  ### `after_processing`
</div>

成功处理后如何处理文件。

可选值：

* keep。
* delete。
* move。
* tag。

默认值：`keep`。

Move 需要额外设置。如果是在同一个存储桶内移动，则必须通过 `after_processing_move_prefix` 提供新的路径前缀。

移动到另一个 S3 存储桶时，需要通过 `after_processing_move_uri` 指定目标存储桶 URI，并通过 `after_processing_move_access_key_id` 和 `after_processing_move_secret_access_key` 提供 S3 访问凭据。

示例：

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered',
    after_processing = 'move',
    after_processing_retries = 20,
    after_processing_move_prefix = 'dst_prefix',
    after_processing_move_uri = 'https://clickhouse-public-datasets.s3.amazonaws.com/dst-bucket',
    after_processing_move_access_key_id = 'test',
    after_processing_move_secret_access_key = 'test';
```

将对象从一个 Azure 容器移动到另一个 Azure 容器时，需要将 blob 存储 connection string 配置为 `after_processing_move_connection_string`，并将容器名称配置为 `after_processing_move_container`。请参见[AzureQueue Settings](../../../engines/table-engines/integrations/azure-queue.md#settings)。

打标签时，需要通过 `after_processing_tag_key` 和 `after_processing_tag_value` 提供标签键和值。

<div id="after_processing_retries">
  ### `after_processing_retries`
</div>

对所请求的后处理操作，在放弃前的重试次数。

可能的值：

* 非负整数。

默认值：`10`。

<div id="after_processing_move_access_key_id">
  ### `after_processing_move_access_key_id`
</div>

如果目标端是另一个 S3 存储桶，此项表示用于将已成功处理的文件移动到该 S3 存储桶 的 Access Key ID。

可能的值：

* String。

默认值：空字符串。

<div id="after_processing_move_prefix">
  ### `after_processing_move_prefix`
</div>

用于移动已成功处理文件的路径前缀。既适用于在同一 存储桶 中移动，也适用于移动到另一个 存储桶。

可能的值：

* String。

默认值：空字符串。

<div id="after_processing_move_preserve_path">
  ### `after_processing_move_preserve_path`
</div>

如果为 `true`，在移动已成功处理的文件时，会将完整的源对象路径附加到 `after_processing_move_prefix` 后面，从而在目标端保留存储桶下的源目录结构。如果为 `false`，则只使用文件名，源目录结构会被展平。

可能的值：

* `true` / `false`。

默认值：`false`。

<div id="after_processing_move_secret_access_key">
  ### `after_processing_move_secret_access_key`
</div>

当目标端是另一个 S3 存储桶时，用于将已成功处理的文件移动到该 S3 存储桶的 Secret Access Key。

可能的值：

* String。

默认值：空字符串。

<div id="after_processing_move_uri">
  ### `after_processing_move_uri`
</div>

如果目标端是另一个 S3 存储桶，则此参数指定将成功处理后的文件移动到该 S3 存储桶 的 URI。

可能的值：

* String。

默认值：空字符串。

<div id="after_processing_tag_key">
  ### `after_processing_tag_key`
</div>

如果 `after_processing='tag'`，则用于为成功处理的文件打标签的标签键。

可能的值：

* String。

默认值：空字符串。

<div id="after_processing_tag_value">
  ### `after_processing_tag_value`
</div>

当 `after_processing='tag'` 时，用于为已成功处理的文件打标签的标签值。

可能的值：

* String。

默认值：空字符串。

<div id="keeper_path">
  ### `keeper_path`
</div>

ZooKeeper 中队列元数据的路径。如果未显式指定，ClickHouse 会根据 `s3queue_default_zookeeper_path`、数据库 UUID 和表 UUID 构建该路径。绝对值 (以 `/` 开头) 会按原样使用，而相对值则会追加到已配置的前缀后。诸如 `{database}` 或 `{uuid}` 之类的宏会在引擎连接到 ZooKeeper 之前展开。

若要指定辅助 ZooKeeper 集群，请在该值前加上已配置的名称，例如 `analytics_keeper:/clickhouse/queue/orders`。该名称必须存在于 `<auxiliary_zookeepers>` 中；否则引擎会报错 `Unknown auxiliary ZooKeeper name ...`。完整字符串 (包括前缀) 会保留在 `SHOW CREATE TABLE` 中，以便该语句可被逐字原样复用。

可能的值：

* String。

默认值：`/`。

<div id="loading_retries">
  ### `loading_retries`
</div>

将文件加载重试最多执行指定次数。
可能的值：

* 非负整数。

默认值：`10`。

<div id="processing_threads_num">
  ### `processing_threads_num`
</div>

用于处理的线程数。仅适用于 `Unordered` 模式。

默认值：CPU 数量或 16。

<div id="parallel_inserts">
  ### `parallel_inserts`
</div>

默认情况下，`processing_threads_num` 只会生成一条 `INSERT`，因此只会以多线程方式下载文件和进行解析。

但这会限制并行度，因此为了获得更高的吞吐量，请使用 `parallel_inserts=true`。这样就可以并行插入数据 (但请注意，这会导致为 MergeTree 家族生成更多的数据分区片段) 。

:::note
`INSERT` 会根据 `max_process*_before_commit` 设置生成。
:::

默认值：`false`。

<div id="enable_logging_to_queue_log">
  ### `enable_logging_to_queue_log`
</div>

启用向 `system.s3queue_log` 写入日志。

默认值：`1`。

<div id="polling_min_timeout_ms">
  ### `polling_min_timeout_ms`
</div>

指定 ClickHouse 在发起下一次轮询前的最短等待时间，单位为毫秒。

可能的值：

* 正整数。

默认值：`1000`。

<div id="polling_max_timeout_ms">
  ### `polling_max_timeout_ms`
</div>

定义 ClickHouse 在发起下一次轮询之前等待的最长时间，单位为毫秒。

可能的值：

* 正整数。

默认值：`600000`。

<div id="polling_backoff_ms">
  ### `polling_backoff_ms`
</div>

指定在未发现新文件时，在上一次轮询间隔基础上额外增加的等待时间。下一次轮询会在上一次间隔与此 backoff 值之和，或最大间隔 (二者取较小值) 之后进行。

可能的值：

* 正整数。

默认值：`30000`。

<div id="tracked_files_limit">
  ### `tracked_files_limit`
</div>

在使用 `'unordered'` 模式时，可限制 ZooKeeper 中节点的数量；对于 `'ordered'` 模式，此设置不起作用。
达到限制后，最早处理过的文件会从 ZooKeeper 节点中删除，并被重新处理。

可能的值：

* 正整数。

默认值：`1000`。

<div id="tracked_file_ttl_sec">
  ### `tracked_file_ttl_sec`
</div>

在 `unordered` 模式下，用于指定在 ZooKeeper 节点中保存已处理文件的最长时间 (以秒为单位，默认永久保存) ；对于 `ordered` 模式，此设置不起作用。
超过指定秒数后，文件会被重新导入。

可能的值：

* 正整数。

默认值：`0`。

<div id="cleanup_interval_min_ms">
  ### `cleanup_interval_min_ms`
</div>

用于 `Ordered` 模式。定义后台任务重新调度间隔的最小值；该任务负责维护已跟踪文件的生存时间 (TTL) 以及已跟踪文件集的最大数量。

默认值：`60000`。

<div id="cleanup_interval_max_ms">
  ### `cleanup_interval_max_ms`
</div>

用于 &#39;Ordered&#39; 模式。定义后台任务重新调度间隔的最大值；该后台任务负责维护 tracked files 的生存时间 (TTL) 以及 tracked files set 的最大数量。

默认值：`60000`。

<div id="buckets">
  ### `buckets`
</div>

用于 `ordered 模式`。自 `24.6` 起可用。如果 S3Queue 表有多个副本，并且它们都使用 Keeper 中相同的元数据目录，那么 `buckets` 的值至少应等于副本数。如果同时还使用了 `processing_threads` 设置，进一步增大 `buckets` 的值也是合理的，因为它决定了 `S3Queue` 处理的实际并行度。

<div id="use_persistent_processing_nodes">
  ### `use_persistent_processing_nodes`
</div>

默认情况下，S3Queue 表一直使用临时处理节点。如果 ZooKeeper 会话在 S3Queue 将已处理文件提交到 ZooKeeper 之前过期，而此时文件又已经开始处理，就可能导致数据重复。此设置会强制 server 在 Keeper 会话过期时消除出现重复的可能性。

<div id="persistent_processing_node_ttl_seconds">
  ### `persistent_processing_node_ttl_seconds`
</div>

如果 server 非正常终止，并且启用了 `use_persistent_processing_nodes`，则可能会留下未被移除的处理中节点。此设置定义了一个时间段，在此期间这些处理中节点可以被安全清理。相同的生存时间 (TTL) 也用于 `Ordered` 模式下的存储桶锁，而该锁的持有时间可能比单个处理中节点更长，因此该值也应将这一点考虑在内。

默认值：`21600` (6 小时) 。

<div id="s3-settings">
  ## S3 相关设置
</div>

该引擎支持所有与 S3 相关的设置。有关 S3 设置的更多信息，请参见[此文](../../../engines/table-engines/integrations/s3.md)。

<div id="s3-role-based-access">
  ## S3 基于角色的访问
</div>

<ScalePlanFeatureBadge feature="S3 Role-Based Access" />

S3Queue 表引擎支持基于角色的访问。
有关如何配置可访问您的存储桶的角色，请参阅[此处](/zh/cloud/data-sources/secure-s3)的文档。

角色配置完成后，可以通过 `extra_credentials` 参数传入 `roleARN`，如下所示：

```sql
CREATE TABLE s3_table
(
    ts DateTime,
    value UInt64
)
ENGINE = S3Queue(
                'https://<your_bucket>/*.csv',
                extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/<your_role>')
                ,'CSV')
SETTINGS
    ...
```

<div id="ordered-mode">
  ## S3Queue ordered 模式
</div>

`S3Queue` 的这种处理模式可以在 ZooKeeper 中存储更少的元数据，但有一个限制：按时间后添加的文件，其名称必须在字母数字顺序上更大。

`S3Queue` 的 `ordered` 模式与 `unordered` 模式一样，都支持 `(s3queue_)processing_threads_num` 设置 (`s3queue_` 前缀可选) ，可用于控制在服务器本地处理 `S3` 文件的线程数。

对于未启用分区的 `ordered` 模式，ClickHouse 可以从上次处理的 key 继续列举 S3，以避免重新列举整个前缀的历史内容。在分桶的 ordered 模式中，为避免跳过未处理的文件，恢复点会保守地选为所有桶中最小的已处理 key。
这种恢复列举优化仅适用于未启用分区的 ordered 模式下基于 S3 的队列 (不适用于 AzureQueue，也不适用于设置了 `partitioning_mode` 的情况) 。
此外，`ordered` 模式还引入了另一个名为 `(s3queue_)buckets` 的设置，表示“逻辑线程”。也就是说，在分布式场景下，当存在多个带有 `S3Queue` 表副本的服务器时，该设置定义了处理单元的数量。例如，每个 `S3Queue` 副本上的每个处理线程都会尝试锁定某个 `存储桶` 进行处理，而每个 `存储桶` 会根据文件名的哈希分配给特定文件。因此，在分布式场景中，强烈建议将 `(s3queue_)buckets` 设置为至少等于副本数，或更大。桶的数量大于副本数也是完全可行的。最优情况是 `(s3queue_)buckets` 的值等于 `number_of_replicas` 与 `(s3queue_)processing_threads_num` 的乘积。
不建议在 `24.6` 之前的版本中使用 `(s3queue_)processing_threads_num` 设置。
`(s3queue_)buckets` 设置从 `24.6` 版本开始可用。

<div id="select">
  ## 从 S3Queue 表引擎中 SELECT
</div>

默认情况下，禁止对 S3Queue 表执行 SELECT 查询。这遵循了常见的队列模式，即数据被读取一次后就会从队列中移除。禁止 SELECT 是为了防止意外的数据丢失。
不过，在某些场景下，这样做可能会很有用。为此，你需要将设置 `stream_like_engine_allow_direct_select` 设为 `True`。
S3Queue 引擎针对 SELECT 查询提供了一个特殊设置：`commit_on_select`。将其设为 `False` 可在读取后保留队列中的数据；设为 `True` 则会将其移除。

<div id="description">
  ## 描述
</div>

`SELECT` 对流式导入并不是特别有用 (调试时除外) ，因为每个文件只能导入一次。更实用的做法是使用 [materialized views](../../../sql-reference/statements/create/view.md) 创建实时处理线程。为此：

1. 使用该引擎创建一个表，用于从 S3 中指定路径消费数据，并将其视为数据流。
2. 创建一个具有所需结构的表。
3. 创建一个 materialized view，将该引擎中的数据转换后写入之前创建的表。

当 `MATERIALIZED VIEW` 关联到该引擎后，它就会开始在后台收集数据。

示例：

```sql
  CREATE TABLE s3queue_engine_table (name String, value UInt32)
    ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
    SETTINGS
        mode = 'unordered';

  CREATE TABLE stats (name String, value UInt32)
    ENGINE = MergeTree() ORDER BY name;

  CREATE MATERIALIZED VIEW consumer TO stats
    AS SELECT name, value FROM s3queue_engine_table;

  SELECT * FROM stats ORDER BY name;
```

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。
* `_file` — 文件名。
* `_size` — 文件大小。
* `_time` — 文件创建时间。

有关虚拟列的更多信息，请参见[这里](../../../engines/table-engines/index.md#table_engines-virtual_columns)。

<div id="wildcards-in-path">
  ## `path` 中的通配符
</div>

`path` 参数可以使用类似 bash 的通配符来指定多个文件。要处理的文件必须存在，并且匹配整个路径模式。文件列表是在执行 `SELECT` 时确定的 (而不是在 `CREATE` 时) 。

* `*` — 匹配任意数量的任意字符，但不包括 `/`，也可以为空字符串。
* `**` — 匹配任意数量的任意字符，包括 `/`，也可以为空字符串。
* `?` — 匹配任意单个字符。
* `{some_string,another_string,yet_another_one}` — 匹配字符串 `'some_string'`、`'another_string'`、`'yet_another_one'` 中的任意一个。
* `{N..M}` — 匹配从 N 到 M 范围内的任意数字，包括两个端点。N 和 M 可以有前导零，例如 `000..078`。

带有 `{}` 的写法与 [remote](../../../sql-reference/table-functions/remote.md) 表函数类似。

<div id="limitations">
  ## 限制
</div>

1. 出现重复行的原因可能包括：

* 在文件处理过程中，解析到一半时发生异常，且通过 `s3queue_loading_retries` 启用了重试；

* 在多个服务器上配置了指向 ZooKeeper 中同一路径的 `S3Queue`，并且在某台服务器成功提交已处理文件之前，Keeper 会话已过期，这可能导致另一台服务器接手处理该文件，而该文件可能已经被第一台服务器部分或全部处理；不过，如果 `use_persistent_processing_nodes = 1`，则从 25.8 版本开始不再存在这种情况。

* 服务器异常终止。

2. 如果在多个服务器上配置了指向 ZooKeeper 中同一路径的 `S3Queue`，并使用了 `Ordered` 模式，那么 `s3queue_loading_retries` 将不起作用。此问题很快会修复。

<div id="introspection">
  ## 内部信息
</div>

如需查看内部信息，可使用无状态表 `system.s3queue_metadata_cache` 和持久表 `system.s3queue_log`。

1. `system.s3queue_metadata_cache`。此表不是持久表，用于显示 `S3Queue` 的内存状态：当前正在处理哪些文件，以及哪些文件已处理或处理失败。

```sql
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_metadata_cache
(
    `database` String,
    `table` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` String,
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64)
    `exception` String
)
ENGINE = SystemS3Queue
COMMENT 'Contains in-memory state of S3Queue metadata and currently processed rows per file.' │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

示例：

```sql

SELECT *
FROM system.s3queue_metadata_cache

Row 1:
──────
zookeeper_path:        /clickhouse/s3queue/25ea5621-ae8c-40c7-96d0-cec959c5ab88/3b3f66a1-9866-4c2e-ba78-b6bfa154207e
file_name:             wikistat/original/pageviews-20150501-030000.gz
rows_processed:        5068534
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:31
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5068534,'SelectedBytes':198132283,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':2480,'S3QueueSetFileProcessedMicroseconds':9985,'S3QueuePullMicroseconds':273776,'LogTest':17}
exception:
```

2. `system.s3queue_log`。持久化表。包含与 `system.s3queue_metadata_cache` 相同的信息，但记录的是 `processed` 和 `failed` 文件。

该表具有以下结构：

```sql
SHOW CREATE TABLE system.s3queue_log

Query id: 0ad619c3-0f2a-4ee4-8b40-c73d86e04314

┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_log
(
    `event_date` Date,
    `event_time` DateTime,
    `table_uuid` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` Enum8('Processed' = 0, 'Failed' = 1),
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64),
    `exception` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

若要使用 `system.s3queue_log`，请在服务器配置文件中定义其配置：

```xml
    <s3queue_log>
        <database>system</database>
        <table>s3queue_log</table>
    </s3queue_log>
```

示例：

```sql
SELECT *
FROM system.s3queue_log

Row 1:
──────
event_date:            2023-10-13
event_time:            2023-10-13 13:10:12
table_uuid:
file_name:             wikistat/original/pageviews-20150501-020000.gz
rows_processed:        5112621
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:12
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5112621,'SelectedBytes':198577687,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':1934,'S3QueueSetFileProcessedMicroseconds':17063,'S3QueuePullMicroseconds':5841972,'LogTest':17}
exception:
```