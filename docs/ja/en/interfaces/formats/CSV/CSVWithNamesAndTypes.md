---
alias: []
description: 'CSVWithNamesAndTypesフォーマットのドキュメント'
input_format: true
keywords: ['CSVWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/CSVWithNamesAndTypes
title: 'CSVWithNamesAndTypes'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

[TabSeparatedWithNamesAndTypes](../formats/TabSeparatedWithNamesAndTypes) と同様に、カラム名と型を含む 2 行のヘッダー行も出力します。

<div id="example-usage">
  ## 使用例
</div>

<div id="inserting-data">
  ### データの挿入
</div>

:::tip
[バージョン](https://github.com/ClickHouse/ClickHouse/releases) 23.1 以降、ClickHouse は `CSV` フォーマット使用時に CSV ファイル内のヘッダーを自動検出するため、`CSVWithNames` や `CSVWithNamesAndTypes` を使用する必要はありません。
:::

次の `football_types.csv` という CSV ファイルを使用します。

```csv
date,season,home_team,away_team,home_team_goals,away_team_goals
Date,Int16,LowCardinality(String),LowCardinality(String),Int8,Int8
2022-04-30,2021,Sutton United,Bradford City,1,4
2022-04-30,2021,Swindon Town,Barrow,2,1
2022-04-30,2021,Tranmere Rovers,Oldham Athletic,2,0
2022-05-02,2021,Salford City,Mansfield Town,2,2
2022-05-02,2021,Port Vale,Newport County,1,2
2022-05-07,2021,Barrow,Northampton Town,1,3
2022-05-07,2021,Bradford City,Carlisle United,2,0
2022-05-07,2021,Bristol Rovers,Scunthorpe United,7,0
2022-05-07,2021,Exeter City,Port Vale,0,1
2022-05-07,2021,Harrogate Town A.F.C.,Sutton United,0,2
2022-05-07,2021,Hartlepool United,Colchester United,0,2
2022-05-07,2021,Leyton Orient,Tranmere Rovers,0,1
2022-05-07,2021,Mansfield Town,Forest Green Rovers,2,2
2022-05-07,2021,Newport County,Rochdale,0,2
2022-05-07,2021,Oldham Athletic,Crawley Town,3,3
2022-05-07,2021,Stevenage Borough,Salford City,4,2
2022-05-07,2021,Walsall,Swindon Town,0,3
```

テーブルを作成します。

```sql
CREATE TABLE football
(
    `date` Date,
    `season` Int16,
    `home_team` LowCardinality(String),
    `away_team` LowCardinality(String),
    `home_team_goals` Int8,
    `away_team_goals` Int8
)
ENGINE = MergeTree
ORDER BY (date, home_team);
```

`CSVWithNamesAndTypes` フォーマットでデータを挿入します。

```sql
INSERT INTO football FROM INFILE 'football_types.csv' FORMAT CSVWithNamesAndTypes;
```

<div id="reading-data">
  ### データの読み込み
</div>

`CSVWithNamesAndTypes` フォーマットを使用してデータを読み込みます。

```sql
SELECT *
FROM football
FORMAT CSVWithNamesAndTypes
```

出力は、カラム名と型を示す2つのヘッダー行を含むCSVになります：

```csv
"date","season","home_team","away_team","home_team_goals","away_team_goals"
"Date","Int16","LowCardinality(String)","LowCardinality(String)","Int8","Int8"
"2022-04-30",2021,"Sutton United","Bradford City",1,4
"2022-04-30",2021,"Swindon Town","Barrow",2,1
"2022-04-30",2021,"Tranmere Rovers","Oldham Athletic",2,0
"2022-05-02",2021,"Port Vale","Newport County",1,2
"2022-05-02",2021,"Salford City","Mansfield Town",2,2
"2022-05-07",2021,"Barrow","Northampton Town",1,3
"2022-05-07",2021,"Bradford City","Carlisle United",2,0
"2022-05-07",2021,"Bristol Rovers","Scunthorpe United",7,0
"2022-05-07",2021,"Exeter City","Port Vale",0,1
"2022-05-07",2021,"Harrogate Town A.F.C.","Sutton United",0,2
"2022-05-07",2021,"Hartlepool United","Colchester United",0,2
"2022-05-07",2021,"Leyton Orient","Tranmere Rovers",0,1
"2022-05-07",2021,"Mansfield Town","Forest Green Rovers",2,2
"2022-05-07",2021,"Newport County","Rochdale",0,2
"2022-05-07",2021,"Oldham Athletic","Crawley Town",3,3
"2022-05-07",2021,"Stevenage Borough","Salford City",4,2
"2022-05-07",2021,"Walsall","Swindon Town",0,3
```

<div id="format-settings">
  ## フォーマット設定
</div>

:::note
設定 [input&#95;format&#95;with&#95;names&#95;use&#95;header](/ja/operations/settings/settings-formats.md/#input_format_with_names_use_header) が `1` に設定されている場合、
入力データのカラムは名前に基づいてテーブルのカラムに対応付けられます。設定 [input&#95;format&#95;skip&#95;unknown&#95;fields](../../../operations/settings/settings-formats.md/#input_format_skip_unknown_fields) が `1` に設定されている場合、不明な名前のカラムはスキップされます。
それ以外の場合は、1 行目がスキップされます。
:::

:::note
設定 [input&#95;format&#95;with&#95;types&#95;use&#95;header](../../../operations/settings/settings-formats.md/#input_format_with_types_use_header) が `1` に設定されている場合、
入力データの型は、テーブル内の対応するカラムの型と比較されます。それ以外の場合は、2 行目がスキップされます。
:::