---
alias: []
description: 'JSONColumns 格式说明'
input_format: true
keywords: ['JSONColumns']
output_format: true
slug: /interfaces/formats/JSONColumns
title: 'JSONColumns'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

:::tip
`JSONColumns*` 格式的输出会先给出 ClickHouse 字段名，然后列出该字段在表中各行中的内容；
从视觉效果上看，数据相当于向左旋转了 90 度。
:::

在这种格式中，所有数据都表示为一个 JSON 对象。

:::note
`JSONColumns` 格式会先在内存中缓冲所有数据，再将其作为单个块输出，因此可能会导致较高的内存消耗。
:::

<div id="example-usage">
  ## 使用示例
</div>

<div id="inserting-data">
  ### 插入数据
</div>

使用一个名为 `football.json`、包含以下数据的 JSON 文件：

```json
{
    "date": ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    "season": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    "home_team": ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    "away_team": ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    "home_team_goals": [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    "away_team_goals": [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
}
```

插入数据：

```sql
INSERT INTO football FROM INFILE 'football.json' FORMAT JSONColumns;
```

<div id="reading-data">
  ### 读取数据
</div>

使用 `JSONColumns` 格式读取数据：

```sql
SELECT *
FROM football
FORMAT JSONColumns
```

输出将采用 JSON 格式：

```json
{
    "date": ["2022-04-30", "2022-04-30", "2022-04-30", "2022-05-02", "2022-05-02", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07", "2022-05-07"],
    "season": [2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021, 2021],
    "home_team": ["Sutton United", "Swindon Town", "Tranmere Rovers", "Port Vale", "Salford City", "Barrow", "Bradford City", "Bristol Rovers", "Exeter City", "Harrogate Town A.F.C.", "Hartlepool United", "Leyton Orient", "Mansfield Town", "Newport County", "Oldham Athletic", "Stevenage Borough", "Walsall"],
    "away_team": ["Bradford City", "Barrow", "Oldham Athletic", "Newport County", "Mansfield Town", "Northampton Town", "Carlisle United", "Scunthorpe United", "Port Vale", "Sutton United", "Colchester United", "Tranmere Rovers", "Forest Green Rovers", "Rochdale", "Crawley Town", "Salford City", "Swindon Town"],
    "home_team_goals": [1, 2, 2, 1, 2, 1, 2, 7, 0, 0, 0, 0, 2, 0, 3, 4, 0],
    "away_team_goals": [4, 1, 0, 2, 2, 3, 0, 0, 1, 2, 2, 1, 2, 2, 3, 2, 3]
}
```

<div id="format-settings">
  ## 格式设置
</div>

在导入期间，如果将设置 [`input_format_skip_unknown_fields`](/zh/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) 设为 `1`，则会跳过名称未知的列。
块中不存在的列将使用默认值填充 (此处可使用 [`input_format_defaults_for_omitted_fields`](/zh/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) 设置) 。