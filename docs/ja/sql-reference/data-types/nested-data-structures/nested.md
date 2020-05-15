---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "\u30CD\u30B9\u30C8\u3055\u308C\u305F(Name1Type1,Name2Type2,...)"
---

# Nested(name1 Type1, Name2 Type2, …) {#nestedname1-type1-name2-type2}

A nested data structure is like a table inside a cell. The parameters of a nested data structure – the column names and types – are specified the same way as in a [CREATE TABLE](../../../sql-reference/statements/create.md) クエリ。 各テーブルの行は、入れ子になったデータ構造内の任意の数の行に対応できます。

例:

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

この例では、 `Goals` コンバージョンに関するデータを含む入れ子になったデータ構造。 の各行 ‘visits’ tableは、ゼロまたは任意の数の変換に対応できます。

単一の入れ子レベルのみがサポートされます。 配列を含む入れ子になった構造体の列は多次元配列と同等なので、サポートが限られています（MergeTreeエンジンでこれらの列をテーブルに格納するサポートは

ほとんどの場合、入れ子になったデータ構造を操作する場合、その列はドットで区切られた列名で指定されます。 これらの列は、一致する型の配列を構成します。 単一の入れ子になったデータ構造のすべての列配列の長さは同じです。

例:

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

ネストされたデータ構造は、同じ長さの複数の列配列のセットと考えるのが最も簡単です。

SELECTクエリで個々の列ではなく入れ子になったデータ構造全体の名前を指定できるのは、ARRAY JOIN句だけです。 詳細については、 “ARRAY JOIN clause”. 例:

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

入れ子になったデータ構造全体に対してSELECTは実行できません。 明示的にリストできるのは、その一部である個々の列のみです。

INSERTクエリでは、入れ子になったデータ構造のすべてのコンポーネント列配列を個別に渡す必要があります(個々の列配列と同様に)。 挿入時に、同じ長さであることがチェックされます。

DESCRIBEクエリの場合、入れ子になったデータ構造の列は、同じ方法で別々に表示されます。

入れ子になったデータ構造内の要素のALTERクエリには制限があります。

[元の記事](https://clickhouse.tech/docs/en/data_types/nested_data_structures/nested/) <!--hide-->
