---
description: 'パラメトリック集計関数のドキュメント'
sidebar_label: 'パラメトリック'
sidebar_position: 38
slug: /sql-reference/aggregate-functions/parametric-functions
title: 'パラメトリック集計関数'
doc_type: 'reference'
---

一部のaggregate functionsは、引数のカラム (圧縮に使用) だけでなく、初期化に使う定数のパラメーター群も受け取ることができます。構文では、括弧を1組ではなく2組使用します。最初の括弧はパラメーター用、2番目の括弧は引数用です。

<div id="histogram">
  ## histogram
</div>

適応型ヒストグラムを計算します。結果の正確性は保証されません。

```sql
histogram(number_of_bins)(values)
```

この関数では [A Streaming Parallel Decision Tree Algorithm](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf) を使用します。関数に新しいデータが入力されると、ヒストグラムのビンの境界が調整されます。通常、ビンの幅は等しくありません。

**引数**

`values` — 入力値となる [式](/ja/sql-reference/syntax#expressions)。

**パラメータ**

`number_of_bins` — ヒストグラム内のビン数の上限。関数はビン数を自動的に計算します。指定されたビン数に近づくようにしますが、できない場合はそれより少ないビン数を使用します。

**戻り値**

* 次の形式の [Tuple](../../sql-reference/data-types/tuple.md) の [Array](../../sql-reference/data-types/array.md):

  ```
  [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
  ```

  * `lower` — ビンの下限。
  * `upper` — ビンの上限。
  * `height` — 計算されたビンの高さ。

**例**

```sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

```text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

ヒストグラムは、たとえば [bar](/ja/sql-reference/functions/other-functions#bar) 関数を使って可視化できます。

```sql
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

```text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

この場合、ヒストグラムのビンの境界がわからないことに注意してください。

<div id="sequencematch">
  ## sequenceMatch
</div>

数列に、パターンに一致するイベントチェーンが含まれているかを判定します。

**構文**

```sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

:::note
同じ秒に発生したイベントは、シーケンス内で未定義の順序に並ぶ場合があり、その結果に影響することがあります。
:::

**引数**

* `timestamp` — 時刻データを含むと見なされるカラム。一般的なデータ型は `Date` と `DateTime` です。サポートされている [UInt](../../sql-reference/data-types/int-uint.md) のデータ型も使用できます。

* `cond1`, `cond2` — イベントチェーンを記述する条件。データ型: `UInt8`。条件引数は最大 32 個まで渡せます。この関数は、これらの条件で記述されたイベントのみを考慮します。シーケンスに条件で記述されていないデータが含まれている場合、関数はそれらをスキップします。

**パラメータ**

* `pattern` — パターン文字列。[パターン構文](#pattern-syntax) を参照してください。

**戻り値**

* パターンに一致した場合は 1。
* パターンに一致しなかった場合は 0。

型: `UInt8`。

<div id="pattern-syntax">
  #### パターン構文
</div>

* `(?N)` — 位置 `N` の条件引数に一致します。条件には `[1, 32]` の範囲で番号が付けられます。たとえば、`(?1)` は `cond1` パラメータに渡された引数に一致します。

* `.*` — 任意の数のイベントに一致します。パターンのこの要素に一致させるのに、条件引数は必要ありません。

* `(?t operator value)` — 2 つのイベントの間隔となる時間を秒単位で設定します。たとえば、パターン `(?1)(?t>1800)(?2)` は、1800 秒より長い間隔で発生するイベントに一致します。これらのイベントの間には、任意の数のどのようなイベントがあってもかまいません。`>=`、`>`、`<`、`<=`、`==` の演算子を使用できます。

**例**

`t` テーブル内のデータを考えます。

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

次のクエリを実行します。

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

この関数は、番号2が番号1に続くイベントチェーンを見つけました。2つの間にある番号3はイベントとして記述されていないため、スキップされました。例に示したイベントチェーンを検索する際にこの番号も考慮したい場合は、そのための条件を設定する必要があります。

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

この場合、3番のイベントが 1 と 2 の間に発生しているため、関数はパターンに一致するイベントチェーンを見つけられませんでした。同じケースで 4 番の条件を確認した場合は、数列はそのパターンに一致します。

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**関連項目**

* [sequenceCount](#sequencecount)

<div id="sequencecount">
  ## sequenceCount
</div>

パターンに一致したイベントチェーンの数を返します。この関数は、互いに重複しないイベントチェーンを検索します。現在のチェーンが一致すると、その後から次のチェーンの検索を開始します。

:::note
同じ秒に発生したイベントは、シーケンス内での順序が未定義になる場合があり、結果に影響することがあります。
:::

**構文**

```sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**引数**

* `timestamp` — 時刻データを含むと見なされるカラムです。一般的なデータ型は `Date` と `DateTime` です。また、サポートされている任意の [UInt](../../sql-reference/data-types/int-uint.md) データ型も使用できます。

* `cond1`, `cond2` — イベントチェーンを表す条件です。データ型: `UInt8`。条件引数は最大 32 個まで渡せます。関数は、これらの条件で指定されたイベントだけを対象にします。シーケンスに条件で指定されていないデータが含まれている場合、関数はそれらをスキップします。

**パラメータ**

* `pattern` — パターン文字列。[パターン構文](#pattern-syntax)を参照してください。

**戻り値**

* 一致した、重複しないイベントチェーンの数。

型: `UInt64`。

**例**

`t` テーブルのデータを考えてみましょう。

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

数値 1 の後に、間に任意の個数の数値を挟んで数値 2 が現れる回数を数えます:

```sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

<div id="sequencematchevents">
  ## sequenceMatchEvents
</div>

パターンに一致した最長のイベントチェーンに含まれるイベントのタイムスタンプを返します。

:::note
同じ秒に発生したイベントは、結果に影響を与える未定義の順序でシーケンス内に並ぶ場合があります。
:::

**構文**

```sql
sequenceMatchEvents(pattern)(timestamp, cond1, cond2, ...)
```

**引数**

* `timestamp` — 時刻データを含むと見なされるカラム。一般的なデータ型は `Date` と `DateTime` です。また、サポートされている [UInt](../../sql-reference/data-types/int-uint.md) データ型も使用できます。

* `cond1`, `cond2` — イベントチェーンを表す条件。データ型: `UInt8`。条件引数は最大 32 個まで渡せます。関数は、これらの条件で記述されたイベントのみを考慮します。シーケンスに条件で記述されていないデータが含まれている場合、関数はそれらをスキップします。

**パラメータ**

* `pattern` — パターン文字列。[パターン構文](#pattern-syntax) を参照してください。

**戻り値**

* イベントチェーン内で一致した条件引数 (?N) に対応する timestamp の Array。Array 内の位置は、パターン内の条件引数の位置に対応します。

型: Array。

**例**

`t` テーブルのデータを考えてみましょう:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

最長のチェーン内のイベントのタイムスタンプを返します

```sql
SELECT sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│ [1,3,4]                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**関連項目**

* [sequenceMatch](#sequencematch)

<div id="windowfunnel">
  ## windowFunnel
</div>

スライディング時間ウィンドウ内でイベントチェーンを検索し、そのチェーン内で発生したイベント数の最大値を計算します。

この関数は、次のアルゴリズムに従って動作します。

* 関数は、チェーン内の最初の条件をトリガーするデータを検索し、イベントカウンターを 1 に設定します。これがスライディングウィンドウの開始時点です。

* チェーン内のイベントがウィンドウ内で順番どおりに発生した場合、カウンターが増加します。イベントの並びが途中で途切れると、カウンターは増加しません。

* データに完了度の異なる複数のイベントチェーンがある場合、この関数が出力するのは最長のチェーンの長さだけです。

**構文**

```sql
windowFunnel(window, [mode, [mode, ... ]])(timestamp, cond1, cond2, ..., condN)
```

**引数**

* `timestamp` — タイムスタンプを含むカラム名。サポートされるデータ型: [Date](../../sql-reference/data-types/date.md)、[DateTime](/ja/sql-reference/data-types/datetime)、およびその他の符号なし整数型 (`時刻データ` は `UInt64` 型をサポートしますが、その値は Int64 の最大値である 2^63 - 1 を超えることはできません) 。
* `cond` — イベントチェーンを表す条件またはデータ。[UInt8](../../sql-reference/data-types/int-uint.md)。

**パラメータ**

* `window` — スライディングウィンドウの長さで、最初の条件と最後の条件の間の時間間隔です。`window` の単位は `時刻データ` 自体に依存し、状況によって異なります。これは `timestamp of cond1 <= timestamp of cond2 <= ... <= timestamp of condN <= timestamp of cond1 + window` という式で定義されます。
* `mode` — オプションの引数です。1 つ以上のモードを設定できます。
  * `'strict_deduplication'` — イベントの並びの中で同じ条件が成立した場合、そのような重複イベントが以降の処理を中断します。注意: 同じイベントに対して複数の条件が成立する場合、予期しない動作をすることがあります。
  * `'strict_order'` — 他のイベントの介在を許可しません。たとえば、`A->B->D->C` の場合、`D` の時点で `A->B->C` の検出を停止し、最大イベントレベルは 2 になります。
  * `'strict_increase'` — タイムスタンプが厳密に増加するイベントにのみ条件を適用します。
  * `'strict_once'` — 条件を複数回満たしていても、チェーン内では各イベントを 1 回だけカウントします。
  * `'allow_reentry'` — strict order に違反するイベントを無視します。たとえば、A-&gt;A-&gt;B-&gt;C の場合、余分な A を無視して A-&gt;B-&gt;C を検出し、最大イベントレベルは 3 になります。

**戻り値**

スライディング時間枠内で、チェーン内の連続してトリガーされた条件の最大数。
選択範囲内のすべてのチェーンが解析されます。

型: `Integer`。

**例**

オンラインストアで、ユーザーがスマートフォンを選んで 2 回購入するのに、一定の期間で十分かどうかを判定します。

次のイベントチェーンを設定します。

1. ユーザーがストアのアカウントにログインした (`eventID = 1003`)。
2. ユーザーがスマートフォンを検索した (`eventID = 1007, product = 'phone'`)。
3. ユーザーが注文した (`eventID = 1009`)。
4. ユーザーが再度注文した (`eventID = 1010`)。

入力テーブル:

```text
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

2019年1月〜2月の期間に、ユーザー `user_id` がチェーンのどこまで到達できたかを調べます。

```sql title="Query"
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

```text title="Response"
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

**allow&#95;reentry モードの例**

この例では、`allow_reentry` モードがユーザーの再訪パターンでどのように機能するかを示します。

```sql
-- Sample data: user visits checkout -> product detail -> checkout again -> payment
-- Without allow_reentry: stops at level 2 (product detail page)
-- With allow_reentry: reaches level 4 (payment completion)

SELECT
    level,
    count() AS users
FROM
(
    SELECT
        user_id,
        windowFunnel(3600, 'strict_order', 'allow_reentry')(
            timestamp,
            action = 'begin_checkout',      -- Step 1: Begin checkout
            action = 'view_product_detail', -- Step 2: View product detail  
            action = 'begin_checkout',      -- Step 3: Begin checkout again (reentry)
            action = 'complete_payment'     -- Step 4: Complete payment
        ) AS level
    FROM user_events
    WHERE event_date = today()
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;
```

<div id="retention">
  ## retention
</div>

この関数は引数として、イベントで特定の条件が満たされたかどうかを示す `UInt8` 型の条件を 1 個から 32 個まで受け取ります。
引数には任意の条件を指定できます ([WHERE](/ja/sql-reference/statements/select/where) と同様です) 。

最初の条件を除き、各条件は 1 番目の条件との組み合わせで適用されます。2 番目の結果は 1 番目と 2 番目が `true` の場合に `true` となり、3 番目の結果は 1 番目と 3 番目が `true` の場合に `true` となります。以降も同様です。

**構文**

```sql
retention(cond1, cond2, ..., cond32);
```

**引数**

* `cond` — `UInt8` の結果 (1 または 0) を返す式。

**戻り値**

1 または 0 の配列。

* 1 — そのイベントで条件が満たされたことを示します。
* 0 — そのイベントで条件が満たされなかったことを示します。

型: `UInt8`。

**例**

サイトトラフィックを把握するために、`retention` 関数を計算する例を見てみましょう。

**1.** 例を示すためのテーブルを作成します。

```sql title="Query"
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

入力テーブル:

```sql title="Query"
SELECT * FROM retention_test
```

```text title="Response"
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

**2.** `retention` 関数を使用して、ユーザーを一意の ID `uid` でグループ化します。

```sql title="Query"
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

```text title="Response"
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

**3.** 1日ごとのサイト訪問数の合計を計算します。

```sql title="Query"
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

```text title="Response"
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

ここで:

* `r1` - 2020-01-01 にサイトを訪問したユニーク訪問者数 (`cond1` 条件) 。
* `r2` - 2020-01-01 から 2020-01-02 までの特定の期間にサイトを訪問したユニーク訪問者数 (`cond1` 条件と `cond2` 条件) 。
* `r3` - 2020-01-01 と 2020-01-03 の特定の期間にサイトを訪問したユニーク訪問者数 (`cond1` 条件と `cond3` 条件) 。

<div id="uniquptonx">
  ## uniqUpTo(N)(x)
</div>

指定した上限 `N` までの、引数の異なる値の数を計算します。異なる引数値の数が `N` を超える場合、この関数は `N` + 1 を返し、それ以外の場合は正確な値を計算します。

`N` が 10 以下の小さい値での使用を推奨します。`N` の最大値は 100 です。

集約関数の状態には、1 + `N` * 1 つの値のバイトサイズに相当するメモリを使用します。
文字列を扱う場合、この関数は 8 バイトの非暗号学的ハッシュを保存するため、文字列に対する計算は近似になります。

たとえば、Web サイトでユーザーが実行したすべての検索クエリを記録するテーブルがあるとします。テーブル内の各行は 1 件の検索クエリを表し、ユーザー ID、検索クエリ、クエリのタイムスタンプを格納するカラムがあります。`uniqUpTo` を使用すると、少なくとも 5 人のユニークユーザーを生み出したキーワードのみを表示するレポートを生成できます。

```sql
SELECT SearchPhrase
FROM SearchLog
GROUP BY SearchPhrase
HAVING uniqUpTo(4)(UserID) >= 5
```

`uniqUpTo(4)(UserID)` は、各 `SearchPhrase` に含まれる一意の `UserID` 値の数を計算しますが、数えるのは最大 4 個までです。ある `SearchPhrase` に対して一意の `UserID` 値が 4 個を超える場合、この関数は 5 (4 + 1) を返します。次に、`HAVING` 句によって、一意の `UserID` 値の数が 5 未満の `SearchPhrase` 値が除外されます。これにより、少なくとも 5 人の異なるユーザーが使用した検索キーワードの一覧を取得できます。

<div id="summapfiltered">
  ## sumMapFiltered
</div>

この関数は、フィルタに使用するキーの配列をパラメータとして受け取れる点を除き、[sumMap](/ja/sql-reference/aggregate-functions/reference/summap) と同じように動作します。これは、キーのカーディナリティが高い場合に特に便利です。

**構文**

`sumMapFiltered(keys_to_keep)(keys, values)`

**パラメータ**

* `keys_to_keep`: フィルタに使用するキーの [Array](../data-types/array.md)。
* `keys`: キーの [Array](../data-types/array.md)。
* `values`: 値の [Array](../data-types/array.md)。

**戻り値**

* 2 つの配列からなるタプルを返します。1 つはソート済みのキー、もう 1 つは対応するキーごとに合計された値です。

**例**

```sql title="Query"
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

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) FROM sum_map;
```

```response title="Response"
   ┌─sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests)─┐
1. │ ([1,4,8],[10,20,10])                                            │
   └─────────────────────────────────────────────────────────────────┘
```

<div id="summapfilteredwithoverflow">
  ## sumMapFilteredWithOverflow
</div>

この関数は、フィルタに使用するキーの配列をパラメータとして受け取る点を除き、[sumMap](/ja/sql-reference/aggregate-functions/reference/summap) と同様に動作します。これは、特にキーのカーディナリティが高い場合に便利です。[sumMapFiltered](#summapfiltered) 関数との違いは、オーバーフローありで合計を行うことです。つまり、合計結果も引数と同じデータ型で返します。

**構文**

`sumMapFilteredWithOverflow(keys_to_keep)(keys, values)`

**パラメータ**

* `keys_to_keep`: フィルタに使用するキーの [Array](../data-types/array.md)。
* `keys`: キーの [Array](../data-types/array.md)。
* `values`: 値の [Array](../data-types/array.md)。

**戻り値**

* 2 つの配列からなるタプルを返します。キーはソート順で、値は対応するキーごとに合計されます。

**例**

この例では、`sum_map` テーブルを作成してデータをいくつか insert し、その後 `sumMapFilteredWithOverflow` と `sumMapFiltered`、および `toTypeName` 関数を使って結果を比較します。作成したテーブルでは `requests` は `UInt8` 型ですが、`sumMapFiltered` はオーバーフローを避けるため、合計された値の型を `UInt64` に拡張します。一方、`sumMapFilteredWithOverflow` は型を `UInt8` のまま保持するため、結果を格納するには十分な大きさではなく、つまりオーバーフローが発生します。

```sql title="Query"
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

```sql title="Query"
SELECT sumMapFilteredWithOverflow([1, 4, 8])(statusMap.status, statusMap.requests) as summap_overflow, toTypeName(summap_overflow) FROM sum_map;
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) as summap, toTypeName(summap) FROM sum_map;
```

```response title="Response"
   ┌─sum──────────────────┬─toTypeName(sum)───────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt8)) │
   └──────────────────────┴───────────────────────────────────┘
```

```response title="Response"
   ┌─summap───────────────┬─toTypeName(summap)─────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt64)) │
   └──────────────────────┴────────────────────────────────────┘
```

<div id="sequencenextnode">
  ## sequenceNextNode
</div>

イベントチェーンに一致する次のイベントの値を返します。

*実験的な関数です。有効にするには `SET allow_experimental_funnel_functions = 1` を設定します。*

**構文**

```sql
sequenceNextNode(direction, base)(timestamp, event_column, base_condition, event1, event2, event3, ...)
```

**パラメータ**

* `direction` — 方向を指定するために使用します。
  * forward — 前方に移動します。
  * backward — 後方に移動します。

* `base` — 基点を設定するために使用します。
  * head — 基点を最初のイベントに設定します。
  * tail — 基点を最後のイベントに設定します。
  * first&#95;match — 基点を最初に一致した `event1` に設定します。
  * last&#95;match — 基点を最後に一致した `event1` に設定します。

**引数**

* `timestamp` — timestamp を含むカラム名。サポートされるデータ型: [Date](../../sql-reference/data-types/date.md)、[DateTime](/ja/sql-reference/data-types/datetime)、およびその他の符号なし整数型。
* `event_column` — 戻り値として返される次のイベントの値を含むカラム名。サポートされるデータ型: [String](../../sql-reference/data-types/string.md) および [Nullable(String)](../../sql-reference/data-types/nullable.md)。
* `base_condition` — 基点が満たす必要のある条件。
* `event1`, `event2`, ... — イベントチェーンを記述する条件。[UInt8](../../sql-reference/data-types/int-uint.md)。

**戻り値**

* `event_column[next_index]` — pattern が一致し、かつ次の値が存在する場合。
* `NULL` - pattern が一致しないか、次の値が存在しない場合。

Type: [Nullable(String)](../../sql-reference/data-types/nullable.md).

**例**

イベントが A-&gt;B-&gt;C-&gt;D-&gt;E で、B-&gt;C の次のイベントである D を知りたい場合に使用できます。

A-&gt;B の次のイベントを検索するクエリステートメント:

```sql title="Query"
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

```text title="Response"
┌─id─┬─next_flow─┐
│  1 │ C         │
└────┴───────────┘
```

**`forward` と `head` の動作**

```sql
ALTER TABLE test_flow DELETE WHERE 1 = 1 settings mutations_sync = 1;

INSERT INTO test_flow VALUES (1, 1, 'Home') (2, 1, 'Gift') (3, 1, 'Exit');
INSERT INTO test_flow VALUES (1, 2, 'Home') (2, 2, 'Home') (3, 2, 'Gift') (4, 2, 'Basket');
INSERT INTO test_flow VALUES (1, 3, 'Gift') (2, 3, 'Home') (3, 3, 'Gift') (4, 3, 'Basket');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'Home', page = 'Home', page = 'Gift') FROM test_flow GROUP BY id;

                  dt   id   page
 1970-01-01 09:00:01    1   Home // 基点, Matched with Home
 1970-01-01 09:00:02    1   Gift // Matched with Gift
 1970-01-01 09:00:03    1   Exit // The result

 1970-01-01 09:00:01    2   Home // 基点, Matched with Home
 1970-01-01 09:00:02    2   Home // Unmatched with Gift
 1970-01-01 09:00:03    2   Gift
 1970-01-01 09:00:04    2   Basket

 1970-01-01 09:00:01    3   Gift // 基点, Unmatched with Home
 1970-01-01 09:00:02    3   Home
 1970-01-01 09:00:03    3   Gift
 1970-01-01 09:00:04    3   Basket
```

**`backward` と `tail` の挙動**

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, page = 'Basket', page = 'Basket', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift
1970-01-01 09:00:03    1   Exit // 基点, Unmatched with Basket

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // Matched with Gift
1970-01-01 09:00:04    2   Basket // 基点, Matched with Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // 基点, Matched with Gift
1970-01-01 09:00:04    3   Basket // 基点, Matched with Basket
```

**`forward` と `first_match` の動作**

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // 基点
1970-01-01 09:00:03    1   Exit // The result

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // 基点
1970-01-01 09:00:04    2   Basket  The result

1970-01-01 09:00:01    3   Gift // 基点
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // 基点
1970-01-01 09:00:03    1   Exit // Unmatched with Home

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // 基点
1970-01-01 09:00:04    2   Basket // Unmatched with Home

1970-01-01 09:00:01    3   Gift // 基点
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // The result
1970-01-01 09:00:04    3   Basket
```

**`backward` と `last_match` の動作**

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // The result
1970-01-01 09:00:02    1   Gift // 基点
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // 基点
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // 基点
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // Matched with Home, the result is null
1970-01-01 09:00:02    1   Gift // 基点
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home // The result
1970-01-01 09:00:02    2   Home // Matched with Home
1970-01-01 09:00:03    2   Gift // 基点
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift // The result
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // 基点
1970-01-01 09:00:04    3   Basket
```

**`base_condition` の動作**

```sql
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

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, ref = 'ref1', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // The head can not be 基点 because the ref column of the head unmatched with 'ref1'.
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, ref = 'ref4', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1 // The tail can not be 基点 because the ref column of the tail unmatched with 'ref4'.
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, ref = 'ref3', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // This row can not be 基点 because the ref column unmatched with 'ref3'.
 1970-01-01 09:00:02    1   A      ref3 // 基点
 1970-01-01 09:00:03    1   B      ref2 // The result
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, ref = 'ref2', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3 // The result
 1970-01-01 09:00:03    1   B      ref2 // 基点
 1970-01-01 09:00:04    1   B      ref1 // This row can not be 基点 because the ref column unmatched with 'ref2'.
```