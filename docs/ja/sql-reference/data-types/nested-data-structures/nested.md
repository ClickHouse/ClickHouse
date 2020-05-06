---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 57
toc_title: "\u30CD\u30B9\u30C8(Name1\u30BF\u30A4\u30D71,Name2\u30BF\u30A4\u30D72,...)"
---

# Nested(name1 Type1, Name2 Type2, …) {#nestedname1-type1-name2-type2}

A nested data structure is like a table inside a cell. The parameters of a nested data structure – the column names and types – are specified the same way as in a [CREATE TABLE](../../../sql-reference/statements/create.md) クエリ。 各テーブル行は、入れ子になったデータ構造内の任意の数の行に対応できます。

例えば:

``` sql
CREATE TABLE test.visits
(
    CounterID UInt32,
    StartDate Date,
    Sign Int8,
    IsNew UInt8,
    VisitID UInt64,
    UserID UInt64,
    ...
    Goals Nested
    (
        ID UInt32,
        Serial UInt32,
        EventTime DateTime,
        Price Int64,
        OrderID String,
        CurrencyID UInt32
    ),
    ...
) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192, Sign)
```

この例では、 `Goals` ネストされたデータ構造。 の各行 ‘visits’ 表は、ゼロまたは任意の数の変換に対応することができます。

単一の入れ子レベルのみがサポートされます。 配列を含むネストされた構造体の列は、多次元配列と同等であるため、サポートが制限されています（mergetreeエンジンを使用してこれらの列をテーブルに格

ほとんどの場合、入れ子になったデータ構造で作業する場合、その列はドットで区切られた列名で指定されます。 これらの列は、一致する型の配列を構成します。 単一の入れ子になったデータ構造のすべての列配列の長さは同じです。

例えば:

``` sql
SELECT
    Goals.ID,
    Goals.EventTime
FROM test.visits
WHERE CounterID = 101500 AND length(Goals.ID) < 5
LIMIT 10
```

``` text
┌─Goals.ID───────────────────────┬─Goals.EventTime───────────────────────────────────────────────────────────────────────────┐
│ [1073752,591325,591325]        │ ['2014-03-17 16:38:10','2014-03-17 16:38:48','2014-03-17 16:42:27']                       │
│ [1073752]                      │ ['2014-03-17 00:28:25']                                                                   │
│ [1073752]                      │ ['2014-03-17 10:46:20']                                                                   │
│ [1073752,591325,591325,591325] │ ['2014-03-17 13:59:20','2014-03-17 22:17:55','2014-03-17 22:18:07','2014-03-17 22:18:51'] │
│ []                             │ []                                                                                        │
│ [1073752,591325,591325]        │ ['2014-03-17 11:37:06','2014-03-17 14:07:47','2014-03-17 14:36:21']                       │
│ []                             │ []                                                                                        │
│ []                             │ []                                                                                        │
│ [591325,1073752]               │ ['2014-03-17 00:46:05','2014-03-17 00:46:05']                                             │
│ [1073752,591325,591325,591325] │ ['2014-03-17 13:28:33','2014-03-17 13:30:26','2014-03-17 18:51:21','2014-03-17 18:51:45'] │
└────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
```

ネストされたデータ構造を、同じ長さの複数の列配列のセットと考えるのが最も簡単です。

SELECTクエリで、個々の列ではなくネストされたデータ構造全体の名前を指定できる唯一の場所は、ARRAY JOIN句です。 詳細については、 “ARRAY JOIN clause”. 例えば:

``` sql
SELECT
    Goal.ID,
    Goal.EventTime
FROM test.visits
ARRAY JOIN Goals AS Goal
WHERE CounterID = 101500 AND length(Goals.ID) < 5
LIMIT 10
```

``` text
┌─Goal.ID─┬──────Goal.EventTime─┐
│ 1073752 │ 2014-03-17 16:38:10 │
│  591325 │ 2014-03-17 16:38:48 │
│  591325 │ 2014-03-17 16:42:27 │
│ 1073752 │ 2014-03-17 00:28:25 │
│ 1073752 │ 2014-03-17 10:46:20 │
│ 1073752 │ 2014-03-17 13:59:20 │
│  591325 │ 2014-03-17 22:17:55 │
│  591325 │ 2014-03-17 22:18:07 │
│  591325 │ 2014-03-17 22:18:51 │
│ 1073752 │ 2014-03-17 11:37:06 │
└─────────┴─────────────────────┘
```

ネストされたデータ構造全体のselectは実行できません。 明示的にリストできるのは、その一部である個々の列のみです。

挿入クエリでは、入れ子になったデータ構造のすべてのコンポーネント列配列を個別に渡す必要があります(個々の列配列と同様)。 挿入時に、システムは同じ長さを持つことをチェックします。

DESCRIBEクエリの場合、入れ子になったデータ構造内の列は、同じ方法で別々にリストされます。

入れ子になったデータ構造内の要素に対するalter queryには制限があります。

[元の記事](https://clickhouse.tech/docs/en/data_types/nested_data_structures/nested/) <!--hide-->
