---
description: '`FROM` 句のドキュメント'
sidebar_label: 'FROM'
slug: /sql-reference/statements/select/from
title: '`FROM` 句'
doc_type: 'reference'
---

`FROM` 句は、データの読み取り元を指定します。

* [テーブル](../../../engines/table-engines/index.md)
* [サブクエリ](../../../sql-reference/statements/select/index.md)
* [テーブル関数](/ja/sql-reference/table-functions)

[JOIN](../../../sql-reference/statements/select/join.md) 句や [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md) 句を使用して、`FROM` 句の機能を拡張することもできます。

サブクエリは、`FROM` 句内で括弧に囲んで指定できる別の `SELECT` クエリです。

SQL 標準の `VALUES` 句もテーブル式として使用できます。

```sql
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

詳細は、[Values テーブル関数](/ja/sql-reference/table-functions/values#sql-standard-values-clause)を参照してください。

`FROM` には複数のデータソースをカンマ区切りで指定でき、これはそれらに対して [CROSS JOIN](../../../sql-reference/statements/select/join.md) を実行するのと同等です。

`FROM` は `SELECT` 句の前に任意で記述できます。これは標準 SQL に対する ClickHouse 固有の拡張であり、`SELECT` ステートメントをより読みやすくします。例:

```sql
FROM table
SELECT *
```

<div id="final-modifier">
  ## FINAL 修飾子
</div>

`FINAL` を指定すると、ClickHouse は結果を返す前にデータを完全にマージします。これにより、指定されたテーブルエンジンでマージ中に行われるすべてのデータ変換も実行されます。

これは、次のテーブルエンジンを使用するテーブルからデータを選択する場合に適用されます。

* `ReplacingMergeTree`
* `SummingMergeTree`
* `AggregatingMergeTree`
* `CollapsingMergeTree`
* `VersionedCollapsingMergeTree`

`FINAL` を伴う `SELECT` クエリは並列に実行されます。[max&#95;final&#95;threads](/ja/operations/settings/settings#max_final_threads) 設定は、使用するスレッド数を制限します。

<div id="drawbacks">
  ### 欠点
</div>

`FINAL` を使用するクエリは、`FINAL` を使用しない同様のクエリよりもわずかに遅くなります。理由は次のとおりです。

* クエリ実行中にデータがマージされる。
* `FINAL` を含むクエリでは、クエリで指定したカラムに加えて主キーカラムも読み取る場合がある。

`FINAL` では、通常はマージ時に行われる処理をクエリ時にメモリ上で実行する必要があるため、追加のコンピュートリソースとメモリリソースが必要です。ただし、正確な結果を得るには `FINAL` が必要になることがあります (データがまだ完全にはマージされていない可能性があるためです) 。それでも、マージを強制するために `OPTIMIZE` を実行するよりは低コストです。

`FINAL` の代替として、`MergeTree` エンジンのバックグラウンド処理がまだ完了していないことを前提にした別のクエリを使い、集約を適用して対処できる場合があります (たとえば、重複を除外するなど) 。必要な結果を得るためにクエリで `FINAL` を使う必要がある場合は、使って問題ありませんが、追加の処理が必要になる点には注意してください。

`FINAL` は、セッションまたはユーザープロファイルを使って、クエリ内のすべてのテーブルに対して [FINAL](../../../operations/settings/settings.md#final) 設定により自動的に適用できます。

<div id="example-usage">
  ### 使用例
</div>

`FINAL` キーワードの使用

```sql
SELECT x, y FROM mytable FINAL WHERE x > 1;
```

`FINAL` をクエリレベルの設定として使用する

```sql
SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1;
```

`FINAL` をセッションレベルの設定として使う

```sql
SET final = 1;
SELECT x, y FROM mytable WHERE x > 1;
```

<div id="aliases-and-final">
  ### 別名とFINAL
</div>

テーブルに別名がある場合、`FINAL` はその別名の後に置きます。これは、通常テーブルに別名を付ける [`JOIN`](/ja/sql-reference/statements/select/join) クエリで特にわかりやすくなります。

```sql
SELECT t1.id, t2.name
FROM table1 AS t1 FINAL
INNER JOIN table2 AS t2 FINAL ON t1.id = t2.id;
```

`FINAL` はテーブル参照に対する修飾子であるため、完全な `table [AS alias]` 式の後に置く必要があります。alias の前に置くと (`FROM table1 FINAL AS t1`) 、構文エラーになります。

<div id="implementation-details">
  ## 実装の詳細
</div>

`FROM` 句が省略されている場合、データは `system.one` テーブルから読み取られます。
`system.one` テーブルには、行がちょうど1つだけ含まれています (このテーブルは、他の DBMS にある DUAL テーブルと同じ役割を果たします) 。

クエリを実行する際は、クエリで指定されたすべてのカラムが該当するテーブルから抽出されます。外側のクエリで不要なカラムは、サブクエリから取り除かれます。
クエリでカラムがまったく指定されていない場合 (たとえば `SELECT count() FROM t`) 、行数を計算するために、何らかのカラムが引き続きテーブルから抽出されます (最も小さいものが優先されます) 。