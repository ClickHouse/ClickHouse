---
alias: []
description: 'TSKV 格式文档'
input_format: true
keywords: ['TSKV']
output_format: true
slug: /interfaces/formats/TSKV
title: 'TSKV'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

与 [`TabSeparated`](./TabSeparated.md) 格式类似，但以 `name=value` 的形式输出值。
名称的转义方式与 [`TabSeparated`](./TabSeparated.md) 格式相同，`=` 符号也会被转义。

```text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=clickhouse     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```

```sql title="Query"
SELECT * FROM t_null FORMAT TSKV
```

```text title="Response"
x=1    y=\N
```

:::note
当存在大量小列时，这种格式并不高效，通常也没有理由使用它。
不过，就效率而言，它并不逊于 [`JSONEachRow`](../JSON/JSONEachRow.md) 格式。
:::

解析时，不同列的值支持任意顺序。
某些值可以省略，因为它们会被视为其默认值。
在这种情况下，默认值为零值和空行。
表中可指定的复杂值不支持作为默认值。

解析时允许添加一个额外的字段 `tskv`，无需等号或值。该字段会被忽略。

导入时，名称未知的列会被跳过，
前提是将设置 [`input_format_skip_unknown_fields`](/zh/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) 设为 `1`。

[NULL](/zh/sql-reference/syntax.md) 会格式化为 `\N`。

<div id="example-usage">
  ## 使用示例
</div>

<div id="inserting-data">
  ### 插入数据
</div>

使用下面这个名为 `football.tskv` 的 tskv 文件：

```tsv
date=2022-04-30 season=2021     home_team=Sutton United away_team=Bradford City home_team_goals=1       away_team_goals=4
date=2022-04-30 season=2021     home_team=Swindon Town  away_team=Barrow        home_team_goals=2       away_team_goals=1
date=2022-04-30 season=2021     home_team=Tranmere Rovers       away_team=Oldham Athletic       home_team_goals=2       away_team_goals=0
date=2022-05-02 season=2021     home_team=Port Vale     away_team=Newport County        home_team_goals=1       away_team_goals=2
date=2022-05-02 season=2021     home_team=Salford City  away_team=Mansfield Town        home_team_goals=2       away_team_goals=2
date=2022-05-07 season=2021     home_team=Barrow        away_team=Northampton Town      home_team_goals=1       away_team_goals=3
date=2022-05-07 season=2021     home_team=Bradford City away_team=Carlisle United       home_team_goals=2       away_team_goals=0
date=2022-05-07 season=2021     home_team=Bristol Rovers        away_team=Scunthorpe United     home_team_goals=7       away_team_goals=0
date=2022-05-07 season=2021     home_team=Exeter City   away_team=Port Vale     home_team_goals=0       away_team_goals=1
date=2022-05-07 season=2021     home_team=Harrogate Town A.F.C. away_team=Sutton United home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Hartlepool United     away_team=Colchester United     home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Leyton Orient away_team=Tranmere Rovers       home_team_goals=0       away_team_goals=1
date=2022-05-07 season=2021     home_team=Mansfield Town        away_team=Forest Green Rovers   home_team_goals=2       away_team_goals=2
date=2022-05-07 season=2021     home_team=Newport County        away_team=Rochdale      home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Oldham Athletic       away_team=Crawley Town  home_team_goals=3       away_team_goals=3
date=2022-05-07 season=2021     home_team=Stevenage Borough     away_team=Salford City  home_team_goals=4       away_team_goals=2
date=2022-05-07 season=2021     home_team=Walsall       away_team=Swindon Town  home_team_goals=0       away_team_goals=3
```

插入数据：

```sql
INSERT INTO football FROM INFILE 'football.tskv' FORMAT TSKV;
```

<div id="reading-data">
  ### 读取数据
</div>

使用 `TSKV` 格式读取数据：

```sql
SELECT *
FROM football
FORMAT TSKV
```

输出将采用以制表符分隔的格式，并包含两行表头，分别为列名和类型：

```tsv
date=2022-04-30 season=2021     home_team=Sutton United away_team=Bradford City home_team_goals=1       away_team_goals=4
date=2022-04-30 season=2021     home_team=Swindon Town  away_team=Barrow        home_team_goals=2       away_team_goals=1
date=2022-04-30 season=2021     home_team=Tranmere Rovers       away_team=Oldham Athletic       home_team_goals=2       away_team_goals=0
date=2022-05-02 season=2021     home_team=Port Vale     away_team=Newport County        home_team_goals=1       away_team_goals=2
date=2022-05-02 season=2021     home_team=Salford City  away_team=Mansfield Town        home_team_goals=2       away_team_goals=2
date=2022-05-07 season=2021     home_team=Barrow        away_team=Northampton Town      home_team_goals=1       away_team_goals=3
date=2022-05-07 season=2021     home_team=Bradford City away_team=Carlisle United       home_team_goals=2       away_team_goals=0
date=2022-05-07 season=2021     home_team=Bristol Rovers        away_team=Scunthorpe United     home_team_goals=7       away_team_goals=0
date=2022-05-07 season=2021     home_team=Exeter City   away_team=Port Vale     home_team_goals=0       away_team_goals=1
date=2022-05-07 season=2021     home_team=Harrogate Town A.F.C. away_team=Sutton United home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Hartlepool United     away_team=Colchester United     home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Leyton Orient away_team=Tranmere Rovers       home_team_goals=0       away_team_goals=1
date=2022-05-07 season=2021     home_team=Mansfield Town        away_team=Forest Green Rovers   home_team_goals=2       away_team_goals=2
date=2022-05-07 season=2021     home_team=Newport County        away_team=Rochdale      home_team_goals=0       away_team_goals=2
date=2022-05-07 season=2021     home_team=Oldham Athletic       away_team=Crawley Town  home_team_goals=3       away_team_goals=3
date=2022-05-07 season=2021     home_team=Stevenage Borough     away_team=Salford City  home_team_goals=4       away_team_goals=2
date=2022-05-07 season=2021     home_team=Walsall       away_team=Swindon Town  home_team_goals=0       away_team_goals=3
```

<div id="format-settings">
  ## 格式设置
</div>
