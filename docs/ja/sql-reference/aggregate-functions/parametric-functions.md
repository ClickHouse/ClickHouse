---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: "\u30D1\u30E9\u30E1\u30C8\u30EA\u30C3\u30AF"
---

# パラメトリック集計関数 {#aggregate_functions_parametric}

Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters – constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

## ヒス {#histogram}

適応ヒストグラムを計算します。 正確な結果を保証するものではありません。

``` sql
histogram(number_of_bins)(values)
```

関数は以下を使用します [ストリーミン](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). ヒストグラムビンの境界は、新しいデータが関数に入ると調整されます。 一般的なケースでは、ビンの幅は等しくありません。

**パラメータ**

`number_of_bins` — Upper limit for the number of bins in the histogram. The function automatically calculates the number of bins. It tries to reach the specified number of bins, but if it fails, it uses fewer bins.
`values` — [式](../syntax.md#syntax-expressions) 結果として入力値が生成されます。

**戻り値**

-   [配列](../../sql-reference/data-types/array.md) の [タプル](../../sql-reference/data-types/tuple.md) 次の形式の:

        ```
        [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
        ```

        - `lower` — Lower bound of the bin.
        - `upper` — Upper bound of the bin.
        - `height` — Calculated height of the bin.

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

ヒストグラムを視覚化するには [バー](../../sql-reference/functions/other-functions.md#function-bar) 関数、例えば:

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

この場合、ヒストグラムビンの境界線がわからないことを覚えておく必要があります。

## sequenceMatch(pattern)(timestamp, cond1, cond2, …) {#function-sequencematch}

かどうかをチェックします配列を含むイベントのチェーンに一致するパターンです。

``` sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

!!! warning "警告"
    同じ秒で発生するイベントは、結果に影響を与える未定義の順序でシーケンス内に存在する可能性があります。

**パラメータ**

-   `pattern` — Pattern string. See [パターン構文](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` と `DateTime`. も利用できますの対応 [UInt](../../sql-reference/data-types/int-uint.md) データ型。

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. 最大32個の条件引数を渡すことができます。 この関数は、これらの条件で説明されているイベントのみを考慮します。 シーケンスに条件で記述されていないデータが含まれている場合、関数はそれらをスキップします。

**戻り値**

-   パターンが一致する場合は、1。
-   パターンが一致しない場合は0。

タイプ: `UInt8`.

<a name="sequence-function-pattern-syntax"></a>
**パターン構文**

-   `(?N)` — Matches the condition argument at position `N`. 条件は、 `[1, 32]` 範囲 例えば, `(?1)` に渡された引数と一致します。 `cond1` パラメータ。

-   `.*` — Matches any number of events. You don't need conditional arguments to match this element of the pattern.

-   `(?t operator value)` — Sets the time in seconds that should separate two events. For example, pattern `(?1)(?t>1800)(?2)` お互いから1800秒以上発生するイベントと一致します。 これらのイベントの間に任意の数のイベントを配置できます。 を使用することができます `>=`, `>`, `<`, `<=` 演算子。

**例**

のデータを考慮する `t` テーブル:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

クエリの実行:

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

関数は、番号2が番号1に続くイベントチェーンを見つけました。 番号はイベントとして記述されていないため、それらの間の番号3をスキップしました。 例で与えられたイベントチェーンを検索するときにこの番号を考慮に入れたい場合は、それに対する条件を作成する必要があります。

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

この場合、番号3のイベントが1と2の間で発生したため、関数はパターンに一致するイベントチェーンを見つけることができませんでした。 同じケースで4番の条件をチェックした場合、シーケンスはパターンと一致します。

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**も参照。**

-   [シーケンスカウント](#function-sequencecount)

## sequenceCount(pattern)(time, cond1, cond2, …) {#function-sequencecount}

パターンに一致したイベントチェーンの数を数えます。 この関数は、重複しないイベントチェーンを検索します。 現在のチェーンが一致した後、次のチェーンの検索を開始します。

!!! warning "警告"
    同じ秒で発生するイベントは、結果に影響を与える未定義の順序でシーケンス内に存在する可能性があります。

``` sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**パラメータ**

-   `pattern` — Pattern string. See [パターン構文](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` と `DateTime`. も利用できますの対応 [UInt](../../sql-reference/data-types/int-uint.md) データ型。

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. 最大32個の条件引数を渡すことができます。 この関数は、これらの条件で説明されているイベントのみを考慮します。 シーケンスに条件で記述されていないデータが含まれている場合、関数はそれらをスキップします。

**戻り値**

-   一致する重複しないイベントチェーンの数。

タイプ: `UInt64`.

**例**

のデータを考慮する `t` テーブル:

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

数2が数1の後に何回発生するかを数えます。:

``` sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

**も参照。**

-   [シーケンスマッチ](#function-sequencematch)

## ウィンドウファンネル {#windowfunnel}

スライドタイムウィンドウ内のイベントチェーンを検索し、チェーンから発生したイベントの最大数を計算します。

機能の動作に応じてアルゴリズム:

-   この関数は、チェーン内の最初の条件をトリガーし、イベントカウンターを1に設定するデータを検索します。 これはスライディングウィンドウが始まる瞬間です。

-   だから、チェーンが順次内のウインドウのカウンタを増加されます。 シーケンスのイベントに障害が発生、カウンターな増加されます。

-   さまざまな完了ポイントでデータに複数のイベントチェーンがある場合、関数は最も長いチェーンのサイズのみを出力します。

**構文**

``` sql
windowFunnel(window, [mode])(timestamp, cond1, cond2, ..., condN)
```

**パラメータ**

-   `window` — Length of the sliding window in seconds.
-   `mode` -これはオプションの引数です。
    -   `'strict'` -とき `'strict'` 設定されている場合、windowFunnel()は一意の値に対してのみ条件を適用します。
-   `timestamp` — Name of the column containing the timestamp. Data types supported: [日付](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) その他の符号なし整数型（timestampがサポートしているにもかかわらず `UInt64` 値はInt64最大値を超えることはできません（2^63-1）。
-   `cond` — Conditions or data describing the chain of events. [UInt8](../../sql-reference/data-types/int-uint.md).

**戻り値**

スライディングタイムウィンドウ内のチェーンからの連続したトリガ条件の最大数。
選択内のすべてのチェーンが分析されます。

タイプ: `Integer`.

**例**

ユーザーが電話を選択してオンラインストアで二度購入するのに十分な時間があるかどうかを判断します。

次の一連のイベントを設定します:

1.  ユーザーがストアのアカウントにログインした (`eventID = 1003`).
2.  ユーザーが電話を検索する (`eventID = 1007, product = 'phone'`).
3.  ユーザーが注文した (`eventID = 1009`).
4.  ユーザーが再び注文しました (`eventID = 1010`).

入力テーブル:

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

ユーザーの距離を調べる `user_id` 2019年にチェーンを抜けることができた。

クエリ:

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
ORDER BY level ASC
```

結果:

``` text
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

## 保持 {#retention}

この関数は、1から32までの型の引数の条件のセットを引数として受け取ります `UInt8` るかどうかを示す一定の条件を満ためのイベントです。
任意の条件を引数として指定することができます [WHERE](../../sql-reference/statements/select/where.md#select-where)).

最初の条件を除く条件は、ペアで適用されます：最初と第二が真であれば第二の結果は真になり、最初とfirdが真であれば第三の結果は真になります。

**構文**

``` sql
retention(cond1, cond2, ..., cond32);
```

**パラメータ**

-   `cond` — an expression that returns a `UInt8` 結果(1または0)。

**戻り値**

1または0の配列。

-   1 — condition was met for the event.
-   0 — condition wasn't met for the event.

タイプ: `UInt8`.

**例**

のは、計算の例を考えてみましょう `retention` サイトトラフィックを決定する機能。

**1.** Сreate a table to illustrate an example.

``` sql
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

入力テーブル:

クエリ:

``` sql
SELECT * FROM retention_test
```

結果:

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

**2.** ユーザを一意のIDでグループ化 `uid` を使用して `retention` 機能。

クエリ:

``` sql
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

結果:

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

**3.** 一日あたりのサイト訪問の合計数を計算します。

クエリ:

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

結果:

``` text
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

どこに:

-   `r1`-2020-01-01中にサイトを訪問したユニークな訪問者の数（ `cond1` 条件）。
-   `r2`-2020-01-01から2020-01-02の間の特定の期間にサイトを訪問したユニーク訪問者の数 (`cond1` と `cond2` 条件）。
-   `r3`-2020-01-01から2020-01-03の間の特定の期間にサイトを訪問したユニーク訪問者の数 (`cond1` と `cond3` 条件）。

## uniqUpTo(N)(x) {#uniquptonx}

Calculates the number of different argument values ​​if it is less than or equal to N. If the number of different argument values is greater than N, it returns N + 1.

小さいNsの使用のために推薦される、10まで。 Nの最大値は100です。

集計関数の状態については、1+N\*バイトの値のサイズに等しいメモリ量を使用します。
文字列の場合、8バイトの非暗号化ハッシュを格納します。 つまり、計算は文字列に対して近似されます。

この関数は、いくつかの引数でも機能します。

大きなN値が使用され、一意の値の数がNよりわずかに小さい場合を除いて、できるだけ高速に動作します。

使用例:

``` text
Problem: Generate a report that shows only keywords that produced at least 5 unique users.
Solution: Write in the GROUP BY query SearchPhrase HAVING uniqUpTo(4)(UserID) >= 5
```

[元の記事](https://clickhouse.tech/docs/en/query_language/agg_functions/parametric_functions/) <!--hide-->

## sumMapFiltered(keys\_to\_keep)(キー,値) {#summapfilteredkeys-to-keepkeys-values}

と同じ動作 [サマップ](reference.md#agg_functions-summap) ただし、キーの配列はパラメータとして渡されます。 これは、キーの基数が高い場合に特に便利です。
