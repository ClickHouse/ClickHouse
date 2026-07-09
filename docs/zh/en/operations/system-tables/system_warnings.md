---
description: '此表包含有关 ClickHouse 服务器的警告消息。'
keywords: [ '系统表', 'warnings' ]
slug: /operations/system-tables/system_warnings
title: 'system.warnings'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud />

<div id="description">
  ## 描述
</div>

此表显示有关 ClickHouse 服务器 的警告。
相同类型的警告会合并为一条。
例如，如果已附加的数据库数量 N 超过可配置的阈值 T，则会显示一条包含当前值 N 的记录，而不是显示 N 条单独的记录。
如果当前值降到阈值以下，该记录将从表中移除。

可以使用以下设置对此表进行配置：

* [max&#95;table&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_table_num_to_warn)
* [max&#95;database&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_database_num_to_warn)
* [max&#95;dictionary&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_dictionary_num_to_warn)
* [max&#95;view&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_view_num_to_warn)
* [max&#95;part&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_part_num_to_warn)
* [max&#95;pending&#95;mutations&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_pending_mutations_to_warn)
* [max&#95;pending&#95;mutations&#95;execution&#95;time&#95;to&#95;warn](/zh/operations/server-configuration-parameters/settings#max_pending_mutations_execution_time_to_warn)
* [max&#95;named&#95;collection&#95;num&#95;to&#95;warn](../server-configuration-parameters/settings.md#max_named_collection_num_to_warn)
* [resource&#95;overload&#95;warnings](/zh/operations/settings/server-overload#resource-overload-warnings)

<div id="columns">
  ## 列
</div>

* `message` ([String](../../sql-reference/data-types/string.md)) — 警告信息。
* `message_format_string` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — 用于格式化该消息的格式字符串。

<div id="example">
  ## 示例
</div>

```sql title="Query"
 SELECT * FROM system.warnings LIMIT 2 \G;
```

```text title="Response"
Row 1:
──────
message:               The number of active parts is more than 10.
message_format_string: The number of active parts is more than {}.

Row 2:
──────
message:               The number of attached databases is more than 2.
message_format_string: The number of attached databases is more than {}.
```