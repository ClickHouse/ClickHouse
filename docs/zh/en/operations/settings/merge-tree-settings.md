---
description: '`system.merge_tree_settings` 中的 MergeTree 设置'
slug: /operations/settings/merge-tree-settings
title: 'MergeTree 表设置'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import BetaBadge from '@theme/badges/BetaBadge';
import SettingsInfoBlock from '@theme/SettingsInfoBlock/SettingsInfoBlock';
import VersionHistory from '@theme/VersionHistory/VersionHistory';

系统表 `system.merge_tree_settings` 显示全局设置的 MergeTree 设置。

MergeTree 设置可以在服务器配置文件的 `merge_tree` 部分中设置，也可以在 `CREATE TABLE` 语句的 `SETTINGS` 子句中为每个 `MergeTree` 表单独指定。

自定义设置 `max_suspicious_broken_parts` 的示例：

在服务器配置文件中为所有 `MergeTree` 表配置默认值：

```text
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

针对特定表设置：

```sql
CREATE TABLE tab
(
    `A` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS max_suspicious_broken_parts = 500;
```

使用 `ALTER TABLE ... MODIFY SETTING` 修改特定表的设置：

```sql
ALTER TABLE tab MODIFY SETTING max_suspicious_broken_parts = 100;

-- reset to global default (value from system.merge_tree_settings)
ALTER TABLE tab RESET SETTING max_suspicious_broken_parts;
```

<div id="mergetree-settings">
  ## MergeTree 设置
</div>

{/* 以下设置由此脚本自动生成： 
  https://github.com/ClickHouse/clickhouse-docs/blob/main/scripts/settings/autogenerate-settings.sh
  */ }