---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "\u9593\u9694"
---

# 間隔 {#data-type-interval}

時間と日付間隔を表すデータ型のファミリ。 の結果の型 [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) オペレーター

!!! warning "警告"
    `Interval` データ型の値はテーブルに格納できません。

構造:

-   符号なし整数値としての時間間隔。
-   間隔のタイプ。

サポートさ:

-   `SECOND`
-   `MINUTE`
-   `HOUR`
-   `DAY`
-   `WEEK`
-   `MONTH`
-   `QUARTER`
-   `YEAR`

間隔タイプごとに、個別のデータタイプがあります。 例えば、 `DAY` 区間は `IntervalDay` データ型:

``` sql
SELECT toTypeName(INTERVAL 4 DAY)
```

``` text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## 使用上の注意 {#data-type-interval-usage-remarks}

以下を使用できます `Interval`-算術演算の値を [日付](../../../sql-reference/data-types/date.md) と [DateTime](../../../sql-reference/data-types/datetime.md)-値を入力します。 たとえば、現在の時刻に4日を追加できます:

``` sql
SELECT now() as current_date_time, current_date_time + INTERVAL 4 DAY
```

``` text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

異なるタイプの間隔は結合できません。 次のような間隔は使用できません `4 DAY 1 HOUR`. 間隔は、間隔など、間隔の最小単位より小さい単位または等しい単位で指定します `1 day and an hour` 間隔は次のように表現できます `25 HOUR` または `90000 SECOND`.

次の操作で算術演算を実行することはできません `Interval`-値を入力しますが、結果として異なるタイプの間隔を値に追加することができます。 `Date` または `DateTime` データ型。 例えば:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

次のクエリでは、例外が発生します:

``` sql
select now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
Received exception from server (version 19.14.1):
Code: 43. DB::Exception: Received from localhost:9000. DB::Exception: Wrong argument types for function plus: if one argument is Interval, then another must be Date or DateTime..
```

## も参照。 {#see-also}

-   [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) 演算子
-   [toInterval](../../../sql-reference/functions/type-conversion-functions.md#function-tointerval) 型変換関数
