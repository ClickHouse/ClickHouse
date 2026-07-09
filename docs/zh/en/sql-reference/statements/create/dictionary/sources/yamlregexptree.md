---
slug: /sql-reference/statements/create/dictionary/sources/yamlregexptree
title: 'YAMLRegExpTree 字典源'
sidebar_position: 15
sidebar_label: 'YAMLRegExpTree'
description: '将 YAML 文件配置为正则表达式树字典的源。'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

`YAMLRegExpTree` source 会从本地文件系统中的 YAML 文件加载正则表达式树。
它专用于 [`regexp_tree`](../layouts/regexp-tree.md) 字典布局，
可为基于模式的查找 (例如 User-Agent 解析) 提供分层的正则表达式到属性映射。

:::note
`YAMLRegExpTree` source 仅在 ClickHouse Open Source 中可用。
对于 ClickHouse Cloud，请改为将字典导出为 CSV，并通过 [ClickHouse 表 source](./clickhouse.md) 加载。
详见 [在 ClickHouse Cloud 中使用 regexp&#95;tree 字典](../layouts/regexp-tree#use-regular-expression-tree-dictionary-in-clickhouse-cloud)。
:::

<div id="configuration">
  ## 配置
</div>

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0);
```

设置字段：

| 设置     | 描述                                                          |
| ------ | ----------------------------------------------------------- |
| `PATH` | 包含正则表达式树的 YAML 文件的绝对路径。通过 DDL 创建时，该文件必须位于 `user_files` 目录中。 |

<div id="yaml-file-structure">
  ## YAML 文件结构
</div>

YAML 文件包含一组正则表达式树节点。每个节点都可以包含属性和子节点，从而构成层级结构：

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

每个节点具有以下结构：

* **`regexp`**：该节点的正则表达式。
* **属性**：用户定义的字典属性 (例如 `name`、`version`) 。属性值可以包含对正则表达式中捕获组的**反向引用**，写作 `\1` 或 `$1` (数字 1–9) 。这些引用会在查询时替换为匹配到的捕获组内容。
* **子节点**：子节点列表，每个子节点都有自己的属性，并且还可以继续包含更多子节点。子节点列表的名称可以任意指定 (例如上面的 `versions`) 。字符串匹配按深度优先方式进行：如果某个字符串匹配某个节点，也会继续检查其子节点。匹配层级最深的节点属性具有优先次序，会覆盖父节点中同名的属性。

<div id="related-pages">
  ## 相关页面
</div>

* [regexp&#95;tree 字典布局](../layouts/regexp-tree.md) — 布局配置、查询示例及匹配模式
* [dictGet](/zh/sql-reference/functions/ext-dict-functions#dictGet), [dictGetAll](/zh/sql-reference/functions/ext-dict-functions#dictGetAll) — 用于查询 regexp tree 字典的函数