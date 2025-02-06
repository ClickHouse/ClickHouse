---
slug: /ja/sql-reference/aggregate-functions/parametric-functions
sidebar_position: 38
sidebar_label: Parametric
---

# パラメトリック集計関数

いくつかの集計関数は、引数カラム（圧縮のために使用）だけでなく、初期化のためのパラメータのセットを受け入れることができます。構文は、1組ではなく2組の括弧を使用します。1つはパラメータ用で、もう1つは引数用です。

## histogram

順応型ヒストグラムを計算します。正確な結果は保証されません。

``` sql
histogram(number_of_bins)(values)
```

この関数は、[A Streaming Parallel Decision Tree Algorithm](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf)を使用します。ヒストグラムのビンの境界は新しいデータが関数に入ると調整されます。一般的な場合、ビンの幅は等しくありません。

**引数**

`values` — 入力値を結果する[式](../../sql-reference/syntax.md#syntax-expressions)。

**パラメータ**

`number_of_bins` — ヒストグラム内のビンの数の上限。関数は自動的にビンの数を計算します。指定されたビンの数に達しようとしますが、失敗した場合は少ないビンを使用します。

**返される値**

- 次の形式の[タプル](../../sql-reference/data-types/tuple.md)の[配列](../../sql-reference/data-types/array.md)：

    ```
    [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
    ```

    - `lower` — ビンの下限。
    - `upper` — ビンの上限。
    - `height` — ビンの計算された高さ。

**例**

``` sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

[bar](../../sql-reference/functions/other-functions.md#function-bar)関数を使用してヒストグラムを視覚化することができます。例えば：

``` sql
WITH histogram(5)(rand() % 100) AS hist
SELECT
    arrayJoin(hist).3 AS height,
    bar(height, 0, 6, 5) AS bar
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

この場合、ヒストグラムビンの境界を知らないことを覚えておく必要があります。

## sequenceMatch

シーケンスにパターンに一致するイベントチェーンが含まれているかどうかをチェックします。

**構文**

``` sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

:::note
同じ秒に発生するイベントは、結果に影響を与える不明な順序でシーケンス内に存在する可能性があります。
:::

**引数**

- `timestamp` — 時間データを含んでいると見なされるカラム。一般的なデータ型は`Date`と`DateTime`です。また、サポートされている[UInt](../../sql-reference/data-types/int-uint.md)データ型のいずれかを使用することもできます。

- `cond1`, `cond2` — イベントチェーンを記述する条件。データ型：`UInt8`。最大32の条件引数を渡すことができます。関数は、これらの条件で説明されているイベントのみ考慮します。シーケンスに条件で説明されていないデータが含まれている場合、関数はそれらをスキップします。

**パラメータ**

- `pattern` — パターン文字列。[パターン構文](#pattern-syntax)を参照してください。

**返される値**

- パターンが一致する場合は1。
- パターンが一致しない場合は0。

型：`UInt8`。

#### パターン構文

- `(?N)` — 位置`N`の条件引数に一致します。条件は`[1, 32]`の範囲で番号が付けられています。たとえば、`(?1)`は`cond1`パラメータに渡された引数に一致します。

- `.*` — 任意の数のイベントに一致します。このパターン要素に一致するために条件引数を必要としません。

- `(?t operator value)` — 2つのイベントを隔てる秒数を設定します。たとえば、パターン`(?1)(?t>1800)(?2)`は1800秒以上離れて発生するイベントに一致します。これらのイベントの間に任意の数のイベントが存在することができます。`>=`, `>`, `<`, `<=`, `==`演算子を使用できます。

**例**

`t`テーブル内のデータを考えます：

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

クエリを実行します：

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

関数は、2番目のイベントが1に続くイベントチェーンを発見しました。その間の3番はイベントとして記述されていないため、スキップされました。例で示されたイベントチェーンを検索するときにこの番号を考慮に入れたい場合は、そのための条件を設定する必要があります。

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

この場合、関数はパターンに一致するイベントチェーンを見つけられませんでした。なぜなら、3のイベントが1と2の間に発生したからです。同じ場合に4の番号が条件としてチェックされた場合、シーケンスはパターンに一致します。

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**参照**

- [sequenceCount](#sequencecount)

## sequenceCount

パターンに一致したイベントチェーンの数をカウントします。関数は重ならないイベントチェーンを検索します。現在のチェーンが一致した後に次のチェーンの検索を開始します。

:::note
同じ秒に発生するイベントは、結果に影響を与える不明な順序でシーケンス内に存在する可能性があります。
:::

**構文**

``` sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**引数**

- `timestamp` — 時間データを含んでいると見なされるカラム。一般的なデータ型は`Date`と`DateTime`です。また、サポートされている[UInt](../../sql-reference/data-types/int-uint.md)データ型のいずれかを使用することもできます。

- `cond1`, `cond2` — イベントチェーンを記述する条件。データ型：`UInt8`。最大32の条件引数を渡すことができます。関数は、これらの条件で説明されているイベントのみ考慮します。シーケンスに条件で説明されていないデータが含まれている場合、関数はそれらをスキップします。

**パラメータ**

- `pattern` — パターン文字列。[パターン構文](#pattern-syntax)を参照してください。

**返される値**

- 一致した重ならないイベントチェーンの数。

型：`UInt64`

**例**

`t`テーブル内のデータを考えます：

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

任意の他の番号がそれらの間に存在しても、番号1の後に番号2が何回発生するかを数えます：

``` sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

**参照**

- [sequenceMatch](#sequencematch)

## windowFunnel

スライディングタイムウィンドウでイベントチェーンを検索し、チェーンから発生したイベントの最大数を計算します。

関数は次のアルゴリズムに従って動作します：

- 関数はチェーンの最初の条件をトリガーするデータを検索し、イベントカウンターを1に設定します。これはスライディングウィンドウが始まる瞬間です。

- ウィンドウ内でチェーンのイベントが順次発生する場合、カウンターはインクリメントされます。イベントのシーケンスが中断された場合、カウンターはインクリメントされません。

- データに異なる完了点を持つ複数のイベントチェーンが存在する場合、関数は最長のチェーンのサイズのみ出力します。

**構文**

``` sql
windowFunnel(window, [mode, [mode, ... ]])(timestamp, cond1, cond2, ..., condN)
```

**引数**

- `timestamp` — タイムスタンプを含むカラムの名前。サポートされているデータ型：[Date](../../sql-reference/data-types/date.md)、[DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime)およびその他の符号なし整数型（タイムスタンプは`UInt64`型をサポートしていますが、その値はInt64の最大値（2^63 - 1）を超えない必要があります）
- `cond` — イベントチェーンを記述する条件またはデータ。[UInt8](../../sql-reference/data-types/int-uint.md)。

**パラメータ**

- `window` — スライディングウィンドウの長さ、これは最初と最後の条件の間の時間間隔です。`window`の単位は`timestamp`自体に依存し、異なります。式`timestamp of cond1 <= timestamp of cond2 <= ... <= timestamp of condN <= timestamp of cond1 + window`を使用して決定されます。
- `mode` — これはオプションの引数です。1つ以上のモードを設定できます。
    - `'strict_deduplication'` — イベントのシーケンスで同じ条件が成立している場合、そのような繰り返しイベントはさらに処理を中断します。注：複数の条件が同じイベントに対して成立した場合、予期しない動作が発生する可能性があります。
    - `'strict_order'` — 他のイベントの介入を許可しません。例：`A->B->D->C`の場合、`A->B->C`を見つけるのを`D`で止め、最大イベントレベルは2です。
    - `'strict_increase'` — 厳密に増加するタイムスタンプのイベントにのみ条件を適用します。
    - `'strict_once'` — 条件に複数回一致しても、チェーン内で各イベントを一度だけカウントします。

**返される値**

スライディングタイムウィンドウ内のチェーンから連続してトリガーされた最大条件数。
選択内のすべてのチェーンが分析されます。

型：`Integer`

**例**

オンラインストアでユーザーが電話を2回選択して購入するのに十分な期間があるかどうかを判断します。

次のイベントチェーンを設定します：

1. ユーザーがストアのアカウントにログインしました（`eventID = 1003`）。
2. ユーザーが電話を検索しました（`eventID = 1007, product = 'phone'`）。
3. ユーザーが注文をしました（`eventID = 1009`）。
4. ユーザーが再度注文をしました（`eventID = 1010`）。

入力テーブル：

``` text
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-28 │       1 │ 2019-01-29 10:00:00 │    1003 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-31 │       1 │ 2019-01-31 09:00:00 │    1007 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-30 │       1 │ 2019-01-30 08:00:00 │    1009 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-02-01 │       1 │ 2019-02-01 08:00:00 │    1010 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
```

2019年1月〜2月の期間に、ユーザー`user_id`がどこまでチェーンを進むことができたかを調べます。

クエリ：

``` sql
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1003, eventID = 1009, eventID = 1007, eventID = 1010) AS level
    FROM trend
    WHERE (event_date >= '2019-01-01') AND (event_date <= '2019-02-02')
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;
```

結果：

``` text
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

## retention

この関数は、イベントに対して特定の条件が満たされたかどうかを示す`UInt8`型の1から32の引数からなる条件のセットを引数として受け取ります。
どんな条件でも引数として指定できます（[WHERE](../../sql-reference/statements/select/where.md#select-where)のように）。

最初の条件を除いて、条件はペアで適用されます：2番目の結果が真になるのは、最初と2番目がともに真の場合、3番目の場合は最初と3番目がともに真の場合などです。

**構文**

``` sql
retention(cond1, cond2, ..., cond32);
```

**引数**

- `cond` — `UInt8`の結果（1または0）を返す式。

**返される値**

1または0の配列。

- 1 — イベントに対して条件が満たされた。
- 0 — イベントに対して条件が満たされなかった。

型：`UInt8`。

**例**

サイトトラフィックを判断するための`retention`関数の計算例を考えてみましょう。

**1.** 例を示すためにテーブルを作成します。

``` sql
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

入力テーブル：

クエリ：

``` sql
SELECT * FROM retention_test
```

結果：

``` text
┌───────date─┬─uid─┐
│ 2020-01-01 │   0 │
│ 2020-01-01 │   1 │
│ 2020-01-01 │   2 │
│ 2020-01-01 │   3 │
│ 2020-01-01 │   4 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-02 │   0 │
│ 2020-01-02 │   1 │
│ 2020-01-02 │   2 │
│ 2020-01-02 │   3 │
│ 2020-01-02 │   4 │
│ 2020-01-02 │   5 │
│ 2020-01-02 │   6 │
│ 2020-01-02 │   7 │
│ 2020-01-02 │   8 │
│ 2020-01-02 │   9 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-03 │   0 │
│ 2020-01-03 │   1 │
│ 2020-01-03 │   2 │
│ 2020-01-03 │   3 │
│ 2020-01-03 │   4 │
│ 2020-01-03 │   5 │
│ 2020-01-03 │   6 │
│ 2020-01-03 │   7 │
│ 2020-01-03 │   8 │
│ 2020-01-03 │   9 │
│ 2020-01-03 │  10 │
│ 2020-01-03 │  11 │
│ 2020-01-03 │  12 │
│ 2020-01-03 │  13 │
│ 2020-01-03 │  14 │
└────────────┴─────┘
```

**2.** `retention`関数を使用して`uid`をユニークなユーザーIDでグループ化します。

クエリ：

``` sql
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

結果：

``` text
┌─uid─┬─r───────┐
│   0 │ [1,1,1] │
│   1 │ [1,1,1] │
│   2 │ [1,1,1] │
│   3 │ [1,1,1] │
│   4 │ [1,1,1] │
│   5 │ [0,0,0] │
│   6 │ [0,0,0] │
│   7 │ [0,0,0] │
│   8 │ [0,0,0] │
│   9 │ [0,0,0] │
│  10 │ [0,0,0] │
│  11 │ [0,0,0] │
│  12 │ [0,0,0] │
│  13 │ [0,0,0] │
│  14 │ [0,0,0] │
└─────┴─────────┘
```

**3.** 日ごとのサイト訪問数の合計を計算します。

クエリ：

``` sql
SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
    FROM retention_test
    WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
    GROUP BY uid
)
```

結果：

``` text
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

それぞれ：

- `r1` - 2020-01-01にサイトを訪れたユニークビジターの数（`cond1`条件）。
- `r2` - 特定の期間2020-01-01から2020-01-02の間にサイトを訪れたユニークビジターの数（`cond1`および`cond2`条件）。
- `r3` - 特定の期間2020-01-01と2020-01-03にサイトを訪れたユニークビジターの数（`cond1`および`cond3`条件）。

## uniqUpTo(N)(x)

引数の異なる値の数を指定された制限`N`まで計算します。引数の異なる値の数が`N`を超える場合、この関数は`N`+1を返し、それ以外の場合は正確な値を計算します。

小さい`N`（最大10まで）での使用を推奨します。`N`の最大値は100です。

集計関数の状態のため、この関数は1 + `N` \* バイトの値の一つのサイズのメモリを使用します。
文字列を扱う場合、この関数は8バイトの非暗号化ハッシュを保存します。文字列の計算は近似的になります。

たとえば、あなたのウェブサイトでユーザーによって行われたすべての検索クエリを記録するテーブルがあったとします。テーブルの各行は単一の検索クエリを表し、ユーザーID、検索クエリ、クエリのタイムスタンプのカラムがあります。このデータを使用して、少なくとも5人のユニークユーザーが使用したキーワードのみを表示するレポートを生成するために`uniqUpTo`を使用できます。

```sql
SELECT SearchPhrase
FROM SearchLog
GROUP BY SearchPhrase
HAVING uniqUpTo(4)(UserID) >= 5
```

`uniqUpTo(4)(UserID)`は各`SearchPhrase`に対するユニークな`UserID`値の数を計算しますが、最大4つのユニークな値だけを数えます。もしある`SearchPhrase`に対するユニークな`UserID`値が4を超える場合、この関数は5（4 + 1）を返します。その後、`HAVING`句はユニークな`UserID`値が5未満の`SearchPhrase`値をフィルタリングします。これにより、少なくとも5人のユニークユーザーが使用した検索キーワードのリストを取得することができます。

## sumMapFiltered

この関数は、[sumMap](../../sql-reference/aggregate-functions/reference/summap.md#agg_functions-summap)と同じ動作をしますが、フィルタリングに使用するキーの配列をパラメータとして受け取ることができます。これは、高いキーのカーディナリティを扱う際に特に役立ちます。

**構文**

`sumMapFiltered(keys_to_keep)(keys, values)`

**パラメータ**

- `keys_to_keep`: フィルタするための[配列](../data-types/array.md)。
- `keys`: [配列](../data-types/array.md)。
- `values`: [配列](../data-types/array.md)。

**返される値**

- ソートされた順序のキーと、対応するキーに対して合計された値を持つ2つの配列のタプルを返します。

**例**

クエリ：

```sql
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt16, requests UInt64)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) FROM sum_map;
```

結果：

```response
   ┌─sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests)─┐
1. │ ([1,4,8],[10,20,10])                                            │
   └─────────────────────────────────────────────────────────────────┘
```

## sumMapFilteredWithOverflow

この関数は、[sumMap](../../sql-reference/aggregate-functions/reference/summap.md#agg_functions-summap)と同じ動作をしますが、フィルタリングに使用するキーの配列をパラメータとして受け取ることができます。これは、高いキーのカーディナリティを扱う際に特に役立ちます。[sumMapFiltered](#summapfiltered)関数とは異なり、オーバーフローを含む加算を行います - つまり、加算の結果のデータ型として引数のデータ型を返します。

**構文**

`sumMapFilteredWithOverflow(keys_to_keep)(keys, values)`

**パラメータ**

- `keys_to_keep`: フィルタするための[配列](../data-types/array.md)。
- `keys`: [配列](../data-types/array.md)。
- `values`: [配列](../data-types/array.md)。

**返される値**

- ソートされた順序のキーと、対応するキーに対して合計された値を持つ2つの配列のタプルを返します。

**例**

この例では、テーブル`sum_map`を作成し、データを挿入してから`sumMapFilteredWithOverflow`と`sumMapFiltered`を`toTypeName`関数と共に比較に使用します。作成したテーブルで`requests`は`UInt8`型であった場合、`sumMapFiltered`はオーバーフローを避けるために合計された値の型を`UInt64`に昇格させたのに対し、`sumMapFilteredWithOverflow`は型を引き続き`UInt8`としており、これは結果を格納するには十分ではありません - つまり、オーバーフローが発生しています。

クエリ：

```sql
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt8, requests UInt8)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql
SELECT sumMapFilteredWithOverflow([1, 4, 8])(statusMap.status, statusMap.requests) as summap_overflow, toTypeName(summap_overflow) FROM sum_map;
```

```sql
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) as summap, toTypeName(summap) FROM sum_map;
```

結果：

```response
   ┌─sum──────────────────┬─toTypeName(sum)───────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt8)) │
   └──────────────────────┴───────────────────────────────────┘
```

```response
   ┌─summap───────────────┬─toTypeName(summap)─────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt64)) │
   └──────────────────────┴────────────────────────────────────┘
```

## sequenceNextNode

一致したイベントチェーンの次のイベントの値を返します。

_エクスペリメンタルな関数、`SET allow_experimental_funnel_functions = 1`で有効にします。_

**構文**

``` sql
sequenceNextNode(direction, base)(timestamp, event_column, base_condition, event1, event2, event3, ...)
```

**パラメータ**

- `direction` — 移動の方向を指定します。
    - forward — 前進。
    - backward — 後退。

- `base` — 基準点を設定するために使用します。
    - head — 基準点を最初のイベントに設定します。
    - tail — 基準点を最後のイベントに設定します。
    - first_match — 基準点を最初に一致した`event1`に設定します。
    - last_match — 基準点を最後の一致した`event1`に設定します。

**引数**

- `timestamp` — タイムスタンプを含むカラムの名前。サポートされているデータ型：[Date](../../sql-reference/data-types/date.md)、[DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) およびその他の符号なし整数型。
- `event_column` — 次に返されるイベントの値を含むカラムの名前。サポートされているデータ型：[String](../../sql-reference/data-types/string.md) と [Nullable(String)](../../sql-reference/data-types/nullable.md)。
- `base_condition` — 基準点が満たすべき条件。
- `event1`, `event2`, ... — イベントチェーンを記述する条件。[UInt8](../../sql-reference/data-types/int-uint.md)。

**返される値**

- `event_column[next_index]` — パターンが一致し、次の値が存在する場合。
- `NULL` - パターンが一致しないか次の値が存在しない場合。

型：[Nullable(String)](../../sql-reference/data-types/nullable.md)。

**例**

イベントがA->B->C->D->Eである場合、B->Cに続くイベントDを知りたいときに使用できます。

A->Bの後のイベントを検索するクエリステートメント：

``` sql
CREATE TABLE test_flow (
    dt DateTime,
    id int,
    page String)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow VALUES (1, 1, 'A') (2, 1, 'B') (3, 1, 'C') (4, 1, 'D') (5, 1, 'E');

SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'A', page = 'A', page = 'B') as next_flow FROM test_flow GROUP BY id;
```

結果：

``` text
┌─id─┬─next_flow─┐
│  1 │ C         │
└────┴───────────┘
```

**`forward`と`head`の動作**

``` sql
ALTER TABLE test_flow DELETE WHERE 1 = 1 settings mutations_sync = 1;

INSERT INTO test_flow VALUES (1, 1, 'Home') (2, 1, 'Gift') (3, 1, 'Exit');
INSERT INTO test_flow VALUES (1, 2, 'Home') (2, 2, 'Home') (3, 2, 'Gift') (4, 2, 'Basket');
INSERT INTO test_flow VALUES (1, 3, 'Gift') (2, 3, 'Home') (3, 3, 'Gift') (4, 3, 'Basket');
```

``` sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'Home', page = 'Home', page = 'Gift') FROM test_flow GROUP BY id;

                  dt   id   page
 1970-01-01 09:00:01    1   Home // 基準点、一致したHome
 1970-01-01 09:00:02    1   Gift // Giftと一致
 1970-01-01 09:00:03    1   Exit // 結果

 1970-01-01 09:00:01    2   Home // 基準点、一致したHome
 1970-01-01 09:00:02    2   Home // Giftと一致しない
 1970-01-01 09:00:03    2   Gift
 1970-01-01 09:00:04    2   Basket

 1970-01-01 09:00:01    3   Gift // 基準点、Homeと一致しない
 1970-01-01 09:00:02    3   Home
 1970-01-01 09:00:03    3   Gift
 1970-01-01 09:00:04    3   Basket
```

**`backward`と`tail`の動作**

``` sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, page = 'Basket', page = 'Basket', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift
1970-01-01 09:00:03    1   Exit // 基準点、Basketと一致しない

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // 結果
1970-01-01 09:00:03    2   Gift // Giftと一致
1970-01-01 09:00:04    2   Basket // 基準点、Basketと一致

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // 結果
1970-01-01 09:00:03    3   Gift // 基準点、Giftと一致
1970-01-01 09:00:04    3   Basket // 基準点、Basketと一致
```

**`forward`と`first_match`の動作**

``` sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // 基準点
1970-01-01 09:00:03    1   Exit // 結果

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // 基準点
1970-01-01 09:00:04    2   Basket // 結果

1970-01-01 09:00:01    3   Gift // 基準点
1970-01-01 09:00:02    3   Home // 結果
1970-01-01 09:00:03    3   Gift
1970-01-01 09:00:04    3   Basket
```

``` sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // 基準点
1970-01-01 09:00:03    1   Exit // Homeと一致しない

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // 基準点
1970-01-01 09:00:04    2   Basket // Homeと一致しない

1970-01-01 09:00:01    3   Gift // 基準点
1970-01-01 09:00:02    3   Home // Homeと一致
1970-01-01 09:00:03    3   Gift // 結果
1970-01-01 09:00:04    3   Basket
```

**`backward`と`last_match`の動作**

``` sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // 結果
1970-01-01 09:00:02    1   Gift // 基準点
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // 結果
1970-01-01 09:00:03    2   Gift // 基準点
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // 結果
1970-01-01 09:00:03    3   Gift // 基準点
1970-01-01 09:00:04    3   Basket
```

``` sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // Homeと一致、結果はnull
1970-01-01 09:00:02    1   Gift // 基準点
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home // 結果
1970-01-01 09:00:02    2   Home // Homeと一致
1970-01-01 09:00:03    2   Gift // 基準点
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift // 結果
1970-01-01 09:00:02    3   Home // Homeと一致
1970-01-01 09:00:03    3   Gift // 基準点
1970-01-01 09:00:04    3   Basket
```

**`base_condition`の動作**

``` sql
CREATE TABLE test_flow_basecond
(
    `dt` DateTime,
    `id` int,
    `page` String,
    `ref` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow_basecond VALUES (1, 1, 'A', 'ref4') (2, 1, 'A', 'ref3') (3, 1, 'B', 'ref2') (4, 1, 'B', 'ref1');
```

``` sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, ref = 'ref1', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // 基準点をヘッドにすることはできません。refカラムが'ref1'と一致しないため。
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1
 ```

``` sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, ref = 'ref4', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1 // テールを基準点にすることはできません。refカラムが'ref4'と一致しないため。
```

``` sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, ref = 'ref3', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // この行は基準点にはなりません。refカラムが'ref3'と一致しないため。
 1970-01-01 09:00:02    1   A      ref3 // 基準点
 1970-01-01 09:00:03    1   B      ref2 // 結果
 1970-01-01 09:00:04    1   B      ref1
```

``` sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, ref = 'ref2', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3 // 結果
 1970-01-01 09:00:03    1   B      ref2 // 基準点
 1970-01-01 09:00:04    1   B      ref1 // この行は基準点にはなりません。refカラムが'ref2'と一致しないため。
```
