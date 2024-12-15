---
slug: /ja/sql-reference/statements/select/from
sidebar_label: FROM
---

# FROM句

`FROM`句は、データを読み取るためのソースを指定します:

- [テーブル](../../../engines/table-engines/index.md)
- [サブクエリ](../../../sql-reference/statements/select/index.md) 
- [テーブル関数](../../../sql-reference/table-functions/index.md#table-functions)

`FROM`句の機能を拡張するために、[JOIN](../../../sql-reference/statements/select/join.md) および [ARRAY JOIN](../../../sql-reference/statements/select/array-join.md)句も使用できます。

サブクエリは、`FROM`句内で括弧で囲まれた別の`SELECT`クエリです。

`FROM`には複数のデータソースを含めることができ、カンマで区切られます。これはそれらに対する[CROSS JOIN](../../../sql-reference/statements/select/join.md)を実行することと同等です。

`FROM`は、`SELECT`句の前にオプションで表示することができます。これは標準SQLのClickHouse特有の拡張であり、`SELECT`文を読みやすくします。例:

```sql
FROM table
SELECT *
```

## FINAL修飾子

`FINAL`が指定されると、ClickHouseは結果を返す前にデータを完全にマージします。これにより、特定のテーブルエンジンのマージ中に起こるすべてのデータ変換も実行されます。

以下のテーブルエンジンを使用しているテーブルからデータを選択する場合に適用されます:
- `ReplacingMergeTree`
- `SummingMergeTree`
- `AggregatingMergeTree`
- `CollapsingMergeTree`
- `VersionedCollapsingMergeTree`

`FINAL`付きの`SELECT`クエリは並列に実行されます。[max_final_threads](../../../operations/settings/settings.md#max-final-threads)設定は使用するスレッド数を制限します。

### 欠点

`FINAL`を使用するクエリは、`FINAL`を使用しない類似のクエリよりも若干遅く実行されます。理由は以下の通りです:

- クエリ実行中にデータがマージされる。
- `FINAL`付きのクエリは、クエリで指定したカラムに加えて主キーのカラムを読み取る場合があります。

`FINAL`は通常マージ時に行われる処理をクエリの実行時にメモリ内で行うため、追加の計算リソースとメモリリソースを要求します。しかし、(データがまだ完全にマージされていない可能性があるため)正確な結果を得るためにFINALを使用することが時々必要です。それはマージを強制する`OPTIMIZE`を実行するよりもコストが低いです。

`FINAL`を使用する代わりに、`MergeTree`エンジンのバックグラウンドプロセスがまだ発生していないと仮定し、集計を適用して(例えば、重複を破棄するなど)それに対処する異なるクエリを使用することが時々可能です。クエリで必要な結果を得るために`FINAL`を使用する必要がある場合、それを行うことは問題ありませんが、追加の処理が必要であることに注意してください。

すべてのテーブルにクエリを自動的に適用するために、セッションまたはユーザープロファイルで[FINAL](../../../operations/settings/settings.md#final)設定を使用して`FINAL`を適用することができます。

### 使用例

`FINAL`キーワードを使用する場合

```sql
SELECT x, y FROM mytable FINAL WHERE x > 1;
```

クエリレベルで`FINAL`を設定として使用する場合

```sql
SELECT x, y FROM mytable WHERE x > 1 SETTINGS final = 1;
```

セッションレベルで`FINAL`を設定として使用する場合

```sql
SET final = 1;
SELECT x, y FROM mytable WHERE x > 1;
```

## 実装の詳細

`FROM`句が省略された場合、`system.one`テーブルからデータが読み取られます。
`system.one`テーブルは正確に1行を含んでいます (このテーブルは他のDBMSsで見られるDUALテーブルと同じ目的を果たします)。

クエリを実行するために、クエリにリストされたすべてのカラムが適切なテーブルから抽出されます。外部クエリに必要のないカラムはサブクエリから捨てられます。
クエリにカラムがリストされていない場合 (`SELECT count() FROM t`など)、いずれかのカラムがテーブルから抽出されます (できるだけ小さいものが好まれます)、行数を計算するためです。
