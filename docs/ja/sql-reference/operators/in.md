---
slug: /ja/sql-reference/operators/in
---
# IN 演算子

`IN`、`NOT IN`、`GLOBAL IN`、`GLOBAL NOT IN` 演算子は、その機能が非常に豊富であるため、個別に説明されます。

演算子の左側は単一のカラムまたはタプルです。

例:

``` sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

左側がインデックスにある単一のカラムで、右側が定数の集合である場合、システムはクエリの処理にインデックスを使用します。

あまりにも多くの値を明示的にリストしないでください（例えば、数百万）。データセットが大きい場合は、一時テーブルに置きます（例えば、[クエリ処理のための外部データ](../../engines/table-engines/special/external-data.md)のセクションを参照）、そしてサブクエリを使用します。

演算子の右側は、定数式の集合、定数式を持つタプルの集合（上記の例に示される）、またはデータベースのテーブル名や括弧内の`SELECT`サブクエリであることができます。

ClickHouseは、`IN`サブクエリの左側と右側の型が異なることを許可します。この場合、左側の値を右側の型に変換します。[accurateCastOrNull](../functions/type-conversion-functions.md#type_conversion_function-accurate-cast_or_null)関数が適用されるかのようにです。つまり、データ型は[Nullable](../../sql-reference/data-types/nullable.md)となり、変換が実行できない場合は[NULL](../../sql-reference/syntax.md#null-literal)を返します。

**例**

クエリ:

``` sql
SELECT '1' IN (SELECT 1);
```

結果:

``` text
┌─in('1', _subquery49)─┐
│                    1 │
└──────────────────────┘
```

演算子の右側がテーブル名の場合（例えば、`UserID IN users`）、これはサブクエリ `UserID IN (SELECT * FROM users)` と同等です。クエリと一緒に送信される外部データを扱うときにこれを使用します。例えば、クエリは、ユーザーのIDセットを一時テーブルの「users」に読み込んでフィルタリングする必要がある場合に送信されます。

演算子の右側が、常にRAMに存在する準備済みデータセットを持つSetエンジンを持つテーブル名である場合、データセットは各クエリのために再作成されません。

サブクエリは、タプルをフィルタリングするために複数のカラムを指定することがあります。

例:

``` sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

`IN`演算子の左側と右側のカラムは同じ型を持つ必要があります。

`IN`演算子とサブクエリは、クエリの任意の部分（集約関数やラムダ関数も含む）に出現することができます。
例:

``` sql
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

``` text
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

3月17日以降の各日付について、その日にこのサイトを訪れたユーザーによるページビューの割合を計算します。
`IN`句内のサブクエリは、単一のサーバー上で一度だけ実行されます。依存サブクエリはありません。

## NULL 処理

リクエスト処理中、`IN`演算子は、[NULL](../../sql-reference/syntax.md#null-literal) との操作結果が常に `0` に等しいと仮定します。演算子の右側または左側に `NULL` があっても関連しません。`NULL` 値はデータセットに含まれず、相互に対応せず、[transform_null_in = 0](../../operations/settings/settings.md#transform_null_in) の場合に比較されません。

`t_null` テーブルを使用した例を示します:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

クエリ `SELECT x FROM t_null WHERE y IN (NULL,3)` を実行すると、次の結果が得られます:

``` text
┌─x─┐
│ 2 │
└───┘
```

`y = NULL` の行がクエリ結果から除外されていることがわかります。これは、ClickHouse が`(NULL,3)` 集合に `NULL` が含まれるかどうかを決定できず、操作の結果として `0` を返し、`SELECT` がこの行を最終出力から除外したためです。

``` sql
SELECT y IN (NULL, 3)
FROM t_null
```

``` text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

## 分散サブクエリ

`IN`演算子とサブクエリには、`JOIN`演算子に似た2つのオプションがあります。それは通常の`IN` / `JOIN` と `GLOBAL IN` / `GLOBAL JOIN` です。これらは、分散クエリ処理の方法が異なります。

:::note
以下に説明するアルゴリズムは、[設定](../../operations/settings/settings.md) `distributed_product_mode` に依存して、異なる動作をすることがあります。
:::

通常の`IN`を使用すると、クエリはリモートサーバーに送信され、それぞれが`IN`または`JOIN`句のサブクエリを実行します。

`GLOBAL IN` / `GLOBAL JOIN` を使用すると、まず全ての`GLOBAL IN` / `GLOBAL JOIN`用のサブクエリが実行され、その結果が一時テーブルに収集されます。その後、一時テーブルは各リモートサーバーに送信され、クエリはこの一時データを使用して実行されます。

分散クエリでない場合は、通常の`IN` / `JOIN`を使用します。

分散クエリ処理のために`IN` / `JOIN`句内のサブクエリを使用する際は注意が必要です。

例を見てみましょう。クラスター内の各サーバーには通常の **local_table** があると仮定します。各サーバーにもクラスター内の全サーバーを対象とした**Distributed**型の **distributed_table** テーブルがあります。

**distributed_table** に対するクエリの場合、クエリはすべてのリモートサーバーに送信され、それらで **local_table** を使用して実行されます。

例えば、クエリ

``` sql
SELECT uniq(UserID) FROM distributed_table
```

はすべてのリモートサーバーに

``` sql
SELECT uniq(UserID) FROM local_table
```

として送信され、それぞれで並行して実行され、中間結果が結合できるステージに達するまで実行されます。その後、中間結果は要求元サーバーに返され、そこでマージされ、クライアントに最終結果が送られます。

`IN` を使用したクエリを見てみましょう:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

- 2つのサイトのオーディエンスの交差の計算。

このクエリはすべてのリモートサーバーに次のように送信されます

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

つまり、`IN`句内のデータセットは、各サーバーに格納されているローカルデータのみを対象にして、各サーバーに独立して収集されます。

これは、クラスタサーバー全体でデータをユーザーIDごとに単一のサーバーに完全に存在するようにデータを分散した場合、正確かつ最適に機能します。この場合、必要なデータはすべてのサーバーでローカルに利用可能になります。そうでない場合、結果は不正確になります。このクエリの変形を「ローカルIN」と呼びます。

クラスタサーバー全体にデータがランダムに分散されている場合に、クエリの動作を修正するには、サブクエリ内に **distributed_table** を指定することができます。クエリは以下のようになります:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

このクエリはすべてのリモートサーバーに次のように送信されます

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

サブクエリは各リモートサーバー上で開始されます。サブクエリが分散テーブルを使用しているため、各リモートサーバー上のサブクエリは、次のように各リモートサーバーに再送信されます:

``` sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

例えば、100台のサーバーのクラスターがある場合、クエリ全体を実行するには10,000の基本的なリクエストが必要であり、これは一般には許容されません。

このような場合、通常の `IN` の代わりに常に `GLOBAL IN` を使用すべきです。クエリの動作を見てみましょう:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

要求元サーバーはサブクエリを実行します:

``` sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

その結果はRAMにある一時テーブルに格納されます。その後、クエリは各リモートサーバーに次のように送信されます:

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

一時テーブル `_data1` はクエリと共に各リモートサーバーに送られます（一時テーブルの名前は実装定義です）。

これは通常の `IN` を使用するよりも最適ですが、以下の点に注意してください:

1. 一時テーブルを作成する際には、データは一意にはされません。ネットワーク上で送信されるデータの量を減らすために、サブクエリでDISTINCTを指定してください。（通常の`IN`の場合はこれを行う必要はありません。）
2. 一時テーブルはすべてのリモートサーバーに送られます。ネットワークトポロジは考慮されません。例えば、10台のリモートサーバーが要求元サーバーに対して非常に遠くにあるデータセンターにある場合、そのデータはリモートデータセンターへのチャネルを10回送信されます。`GLOBAL IN`を使用する際は、大規模なデータセットを避けてください。
3. リモートサーバーへのデータ転送時に、ネットワークの帯域幅制限は構成可能ではありません。ネットワークを過負荷にする可能性があります。
4. `GLOBAL IN`を定期的に使用する必要がないように、サーバー間でデータを分散してください。
5. `GLOBAL IN`を頻繁に使用する必要がある場合、ClickHouseクラスターの場所を計画し、単一のデータセンター内で単一のレプリカグループが存在し、迅速なネットワークが確保され、クエリが単一のデータセンター内で完全に処理できるようにします。

要求元サーバーにのみ利用可能なローカルテーブルがあり、それをリモートサーバーで使用する必要がある場合には、`GLOBAL IN`句内でローカルテーブルを指定するのも意味があります。

### 分散サブクエリと max_rows_in_set

[`max_rows_in_set`](../../operations/settings/query-complexity.md#max-rows-in-set) と [`max_bytes_in_set`](../../operations/settings/query-complexity.md#max-rows-in-set) を使用して、分散クエリ中に転送されるデータ量を制御することができます。

特に、`GLOBAL IN` クエリが大量のデータを返す場合が重要です。次のSQLを考えてみましょう:

```sql
select * from table1 where col1 global in (select col1 from table2 where <some_predicate>)
```

`some_predicate` が十分に選択的でない場合、大量のデータを返してパフォーマンスの問題を引き起こす可能性があります。このような場合、ネットワーク越しのデータ転送を制限することが賢明です。また、[`set_overflow_mode`](../../operations/settings/query-complexity.md#set_overflow_mode) が `throw` に設定されているため（デフォルト）、これらのしきい値に達した時に例外が発生することに注意してください。

### 分散サブクエリと max_parallel_replicas

[max_parallel_replicas](#distributed-subqueries-and-max_parallel_replicas) が1より大きい場合、分散クエリはさらに変換されます。

例を示します:

```sql
SELECT CounterID, count() FROM distributed_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS max_parallel_replicas=3
```

このクエリは各サーバー上で以下のように変換されます:

```sql
SELECT CounterID, count() FROM local_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS parallel_replicas_count=3, parallel_replicas_offset=M
```

ここで、`M` はローカルクエリが実行されているレプリカに応じて `1` から `3` の間です。

これらの設定はクエリ内のすべてのMergeTreeファミリーのテーブルに影響を与え、各テーブルに`SAMPLE 1/3 OFFSET (M-1)/3`を適用するのと同じ効果があります。

そのため、[max_parallel_replicas](#distributed-subqueries-and-max_parallel_replicas)設定を追加することで、両方のテーブルが同じレプリケーションスキームを持ち、UserIDまたはその部分キーによってサンプリングされる場合のみ、正しい結果が得られます。特に、`local_table_2`にサンプリングキーがない場合、不正な結果が生成されます。同じルールが`JOIN`にも適用されます。

`local_table_2`が要件を満たさない場合の回避策として、`GLOBAL IN`または`GLOBAL JOIN`を使用することができます。

テーブルにサンプリングキーがない場合、[parallel_replicas_custom_key](#settings-parallel_replicas_custom_key)のより柔軟なオプションを使用することができます。これにより、異なる、より最適な動作を生成する可能性があります。
