---
description: 'NOT IN、GLOBAL IN、GLOBAL NOT IN演算子を除くIN演算子のドキュメント（これらは別途説明されています）'
slug: /sql-reference/operators/in
title: 'IN演算子'
doc_type: 'reference'
---

`IN`、`NOT IN`、`GLOBAL IN`、`GLOBAL NOT IN` の各演算子は機能が非常に豊富なため、個別に説明します。

演算子の左辺は、単一のカラムまたはTupleのいずれかです。

例：

```sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

左辺が索引に含まれる単一のカラムで、右辺が定数のセットである場合、システムはクエリの処理に索引を使用します。

値を明示的に大量に列挙しないでください (例：数百万件) 。データセットが大きい場合は、一時テーブルに格納してからサブクエリを使用してください (例については、セクション [クエリ処理のための外部データ](../../engines/table-engines/special/external-data.md) を参照) 。

演算子の右辺には、定数式の集合、定数式を含むTupleの集合 (上記の例を参照) 、またはデータベーステーブルの名前や括弧内の`SELECT`サブクエリを指定できます。

歴史的な互換性のため、右辺が単一の `tuple` 式である場合、`IN` 演算子の左辺に応じて、値の集合またはタプル値として解釈されます。左辺がスカラー値の場合、ClickHouse はこの単一の右辺 `tuple` 式の各要素を個別の `IN` の値として扱います。

```sql title="Query"
SELECT
    1 IN (tuple(1, 2)) AS one_in_tuple,
    2 IN (tuple(1, 2)) AS two_in_tuple,
    3 IN (tuple(1, 2)) AS three_in_tuple;
```

```text title="Response"
┌─one_in_tuple─┬─two_in_tuple─┬─three_in_tuple─┐
│            1 │            1 │              0 │
└──────────────┴──────────────┴────────────────┘
```

これは`SELECT 1 IN (1, 2)`と同様に動作します。左辺もタプルの場合、右辺はタプル値のSetとして解釈されます：

```sql title="Query"
SELECT tuple(1, 2) IN (tuple(1, 2)) AS tuple_in_tuple;
```

```text title="Response"
┌─tuple_in_tuple─┐
│              1 │
└────────────────┘
```

この特別な処理は、右辺が単一の`tuple`式である場合にのみ適用されます。スカラーの左辺は、複数のタプル値を含む右辺と照合できません：

```sql title="Query"
SELECT 1 IN (tuple(1, 2), tuple(3, 4));
```

```text title="Response"
Code: 43. DB::Exception: Unsupported types for IN. First argument type UInt8. Second argument type Tuple(Tuple(UInt8, UInt8), Tuple(UInt8, UInt8)). (ILLEGAL_TYPE_OF_ARGUMENT)
```

ClickHouse では、`IN` サブクエリの左辺と右辺で型が異なっていても構いません。
この場合、右辺の値は左辺の型に変換されます。これは、右辺に [accurateCastOrNull](/ja/sql-reference/functions/type-conversion-functions#accurateCastOrNull) 関数を適用した場合と同等の動作です。

これは、データ型が[Nullable](../../sql-reference/data-types/nullable.md)になることを意味し、変換を
実行できない場合は[NULL](/ja/operations/settings/formats#input_format_null_as_default)を返します。

**例**

```sql title="Query"
SELECT '1' IN (SELECT 1);
```

```text title="Response"
┌─in('1', _subquery49)─┐
│                    1 │
└──────────────────────┘
```

演算子の右辺がテーブル名である場合 (例：`UserID IN users`) 、これはサブクエリ `UserID IN (SELECT * FROM users)` と同等です。クエリと共に送信される外部データを扱う際に利用します。たとえば、フィルタリング対象のユーザーIDのセットを「users」一時テーブルに読み込んだうえで、クエリと一緒に送信することができます。

演算子の右辺が Set エンジンを持つテーブル名 (常に RAM 上に保持される準備済みのデータセット) である場合、クエリのたびにデータセットが再作成されることはありません。

サブクエリでは、タプルのフィルタリングに複数のカラムを指定できます。

例:

```sql title="Query"
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

`IN` 演算子の左側と右側のカラムは、同じ型である必要があります。

`IN` 演算子とサブクエリは、集約関数やラムダ関数を含め、クエリのどの部分でも使用できます。
例:

```sql title="Query"
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

```text title="Response"
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

3月17日以降の各日について、3月17日にサイトを訪問したユーザーによるページビューの割合を集計します。
`IN` 句内のサブクエリは、常に単一のサーバー上で1回だけ実行されます。依存サブクエリはありません。

<div id="null-processing">
  ## NULL の処理
</div>

リクエストの処理中、`IN` 演算子は、演算子の右辺・左辺のどちらに `NULL` があるかにかかわらず、[NULL](/ja/operations/settings/formats#input_format_null_as_default) を含む演算の結果は常に `0` に等しいものとみなします。[transform&#95;null&#95;in = 0](../../operations/settings/settings.md#transform_null_in) の場合、`NULL` 値はどのデータセットにも含まれず、互いに対応せず、比較することもできません。

以下は `t_null` テーブルを使った例です。

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

クエリ `SELECT x FROM t_null WHERE y IN (NULL,3)` を実行すると、次のような結果になります：

```text
┌─x─┐
│ 2 │
└───┘
```

`y = NULL` である行がクエリ結果から除外されていることがわかります。これは、ClickHouse では `NULL` が `(NULL,3)` の Set に含まれるかどうかを判定できないため、演算結果として `0` を返し、`SELECT` がこの行を最終出力から除外するためです。

```sql
SELECT y IN (NULL, 3)
FROM t_null
```

```text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

<div id="distributed-subqueries">
  ## 分散サブクエリ
</div>

サブクエリを伴う `IN` 演算子には (`JOIN` 演算子と同様に) 、通常の `IN` / `JOIN` と `GLOBAL IN` / `GLOBAL JOIN` の2つの方式があります。これらは分散クエリ処理での実行方法が異なります。

:::note
以下で説明するアルゴリズムは、[settings](../../operations/settings/settings.md) の `distributed_product_mode` 設定によって動作が異なる場合があります。
:::

通常の`IN`を使用する場合、クエリはリモートサーバーに送信され、各サーバーが`IN`または`JOIN`句のサブクエリを実行します。

`GLOBAL IN` / `GLOBAL JOIN` を使用する場合、まず `GLOBAL IN` / `GLOBAL JOIN` に対するすべてのサブクエリが実行され、その結果が一時テーブルに収集されます。次に、一時テーブルが各リモートサーバーに送信され、この一時データを使用してクエリが実行されます。

`GLOBAL ... JOIN` において、結合のどちら側がサブクエリとして計算されるかは結合の種類によって決まります。`LEFT` および `INNER` 結合では右テーブルが計算され、`RIGHT` 結合では右テーブルが保持側であるため分片から読み取る必要があり、代わりに左テーブルが計算されます。

非分散クエリの場合は、通常の `IN` / `JOIN` を使用してください。

分散クエリ処理では、`IN` / `JOIN` 句でサブクエリを使用する際に注意が必要です。

いくつかの例を見てみましょう。クラスター内の各サーバーには通常の **local&#95;table** があるものとします。また、各サーバーには、クラスター内のすべてのサーバーを参照する **Distributed** タイプの **distributed&#95;table** テーブルもあります。

**distributed&#95;table** へのクエリは、すべてのリモートサーバーに送信され、**local&#95;table** を使用して各サーバー上で実行されます。

例えば、次のクエリ

```sql
SELECT uniq(UserID) FROM distributed_table
```

すべてのリモートサーバーに次のように送信されます

```sql
SELECT uniq(UserID) FROM local_table
```

そして、中間結果を結合できる段階に達するまで、それぞれのサーバー上で並列に実行されます。その後、中間結果はリクエスト元のサーバーに返されてマージされ、最終結果がクライアントに送信されます。

次に、`IN`を使ったクエリを見てみましょう：

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

* 2 つのサイトのオーディエンスの積集合の計算。

このクエリはすべてのリモートサーバーに次の形式で送信されます

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

言い換えると、`IN` 句内のデータセットは、各サーバーにローカルに保存されているデータのみを対象に、各サーバーで独立して収集されます。

これは、あらかじめこのケースを想定してクラスターのサーバー間にデータを分散させ、単一の UserID のデータが必ず単一のサーバー上にまとまって存在するようにしている場合に限り、正しく最適に動作します。この場合、必要なデータはすべて各サーバーのローカルで参照できます。そうでない場合、結果は不正確になります。このクエリのバリエーションを &quot;local IN&quot; と呼びます。

データがクラスターのサーバー全体にランダムに分散されている場合にクエリの動作を修正するには、サブクエリ内で **distributed&#95;table** を指定します。クエリは次のようになります：

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

このクエリはすべてのリモートサーバーに次のように送信されます

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

サブクエリは各リモートサーバーで実行が開始されます。サブクエリが分散テーブルを使用しているため、各リモートサーバー上のサブクエリはすべてのリモートサーバーに以下のように再送信されます：

```sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

たとえば、100台のサーバーで構成されるクラスターがある場合、クエリ全体を実行するには10,000件の基本リクエストが必要となり、これは一般的に受け入れられないと考えられています。

このような場合は、`IN` の代わりに常に `GLOBAL IN` を使用してください。このクエリでの動作を確認してみましょう：

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

リクエスターサーバーはサブクエリを実行します：

```sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

結果は RAM 上の一時テーブルに格納されます。その後、リクエストは各リモートサーバーに次の形式で送信されます：

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

一時テーブル `_data1` は、クエリとともにすべてのリモートサーバーに送信されます (この一時テーブルの名前は実装依存です) 。

これは通常の `IN` を使うよりも効率的です。ただし、次の点に注意してください。

1. 一時テーブルの作成時には、データは一意化されません。ネットワーク経由で送信されるデータ量を減らすには、サブクエリで DISTINCT を指定してください。 (通常の `IN` ではこれを行う必要はありません。)
2. 一時テーブルはすべてのリモートサーバーに送信されます。送信時にネットワークトポロジーは考慮されません。たとえば、10 台のリモートサーバーが、リクエスト元サーバーから見て非常に離れたデータセンターにある場合、そのリモートデータセンターへの回線を通じてデータは 10 回送信されます。`GLOBAL IN` を使用する際は、大きなデータセットは避けるようにしてください。
3. データをリモートサーバーに送信する際、ネットワーク帯域幅の制限は設定できません。ネットワークに過負荷をかけるおそれがあります。
4. 常に `GLOBAL IN` を使わずに済むよう、データをサーバー間に分散させるようにしてください。
5. `GLOBAL IN` を頻繁に使う必要がある場合は、ClickHouse クラスターの配置を計画し、レプリカの 1 つのグループが 2 つ以上ではなく 1 つのデータセンターにのみ存在し、それらの間は高速なネットワークで接続されるようにしてください。そうすることで、クエリを単一のデータセンター内で完結して処理できます。

また、このローカルテーブルがリクエスト元サーバーでのみ利用可能で、そのデータをリモートサーバーで使いたい場合は、`GLOBAL IN` 句でローカルテーブルを指定するのも有効です。

<div id="distributed-subqueries-and-max_rows_in_set">
  ### 分散サブクエリ と max_rows_in_set
</div>

[`max_rows_in_set`](/ja/operations/settings/settings#max_rows_in_set) と [`max_bytes_in_set`](/ja/operations/settings/settings#max_bytes_in_set) を使うと、分散クエリ中に転送されるデータ量を制御できます。

これは、`GLOBAL IN` クエリが大量のデータを返す場合に特に重要です。次の SQL を見てみましょう。

```sql
SELECT * FROM table1 WHERE col1 GLOBAL IN (SELECT col1 FROM table2 WHERE <some_predicate>)
```

`some_predicate` の選択性が十分でない場合、大量のデータが返され、パフォーマンスの問題を引き起こす可能性があります。このような場合は、ネットワーク経由のデータ転送を制限するのが賢明です。なお、[`set_overflow_mode`](/ja/operations/settings/settings#set_overflow_mode) は (デフォルトで) `throw` に設定されているため、これらの閾値に達すると例外が発生することにも注意してください。

<div id="distributed-subqueries-and-max_parallel_replicas">
  ### 分散サブクエリと max_parallel_replicas
</div>

[max&#95;parallel&#95;replicas](#distributed-subqueries-and-max_parallel_replicas) が 1 より大きい場合、分散クエリはさらに変換されます。

たとえば、次のようになります。

```sql
SELECT CounterID, count() FROM distributed_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS max_parallel_replicas=3
```

各サーバーで次のように変換されます:

```sql
SELECT CounterID, count() FROM local_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS parallel_replicas_count=3, parallel_replicas_offset=M
```

ここで `M` は、ローカルクエリが実行されるレプリカに応じて、`1` から `3` までの値を取ります。

これらの設定は、クエリ内のすべての MergeTree ファミリーのテーブルに影響し、各テーブルに `SAMPLE 1/3 OFFSET (M-1)/3` を適用した場合と同じ効果があります。

したがって、[max&#95;parallel&#95;replicas](#distributed-subqueries-and-max_parallel_replicas) 設定を追加して正しい結果が得られるのは、両方のテーブルでレプリケーション方式が同一で、かつ UserID またはそのサブキーでサンプリングされている場合に限られます。特に、`local_table_2` にサンプリングキーがない場合、誤った結果になります。同じルールは `JOIN` にも当てはまります。

`local_table_2` が要件を満たさない場合の回避策の 1 つは、`GLOBAL IN` または `GLOBAL JOIN` を使用することです。

テーブルにサンプリングキーがない場合は、[parallel&#95;replicas&#95;custom&#95;key](/ja/operations/settings/settings#parallel_replicas_custom_key) のより柔軟なオプションを使用でき、異なる、より最適な動作を実現できることがあります。