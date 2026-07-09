---
alias: []
description: 'Pretty 格式文档'
input_format: false
keywords: ['Pretty']
output_format: true
slug: /interfaces/formats/Pretty
title: 'Pretty'
doc_type: 'reference'
---

import PrettyFormatSettings from './_snippets/common-pretty-format-settings.md';

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 描述
</div>

`Pretty` 格式会将数据输出为 Unicode 艺术表格，
并使用 ANSI 转义序列在终端中显示颜色。
表格会绘制出完整的网格，并且每一行在终端中占用两行。
每个结果块都会作为一个独立的表输出。
这一步是必需的，因为只有这样才能在不缓冲结果的情况下输出块 (否则就需要先缓冲，以预先计算所有值的可见宽度) 。

[NULL](/zh/sql-reference/syntax.md) 会输出为 `ᴺᵁᴸᴸ`。

<div id="example-usage">
  ## 使用示例
</div>

示例 (以 [`PrettyCompact`](./PrettyCompact.md) 格式为例) ：

```sql title="Query"
SELECT * FROM t_null
```

```response title="Response"
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

任何 `Pretty` 格式都不会对行进行转义。下面的示例展示的是 [`PrettyCompact`](./PrettyCompact.md) 格式：

```sql title="Query"
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

```response title="Response"
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

为避免向终端输出过多数据，只会打印前 `10,000` 行。
如果行数大于或等于 `10,000`，则会打印消息 &quot;Showed first 10 000&quot;。

:::note
这种格式仅适用于输出查询结果，不适用于解析数据。
:::

Pretty 格式支持输出总计值 (使用 `WITH TOTALS` 时) 和极值 (当 &#39;extremes&#39; 设置为 1 时) 。
在这些情况下，总计值和极值会在主数据之后以单独的表形式输出。
如下示例所示，使用的是 [`PrettyCompact`](./PrettyCompact.md) 格式：

```sql title="Query"
SELECT EventDate, count() AS c 
FROM test.hits 
GROUP BY EventDate 
WITH TOTALS 
ORDER BY EventDate 
FORMAT PrettyCompact
```

```response title="Response"
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

<div id="format-settings">
  ## 格式设置
</div>

<PrettyFormatSettings />