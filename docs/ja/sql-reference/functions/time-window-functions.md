---
slug: /ja/sql-reference/functions/time-window-functions
sidebar_position: 175
sidebar_label: タイムウィンドウ
---

# Time Window Functions

タイムウィンドウ関数は、対応するウィンドウの排他的上限と包括的下限を返します。[WindowView](../statements/create/view.md/#window-view-experimental)で作業するための関数は以下に示されます。

## tumble

タンブルタイムウィンドウは、固定された期間（`interval`）で非重複かつ連続するウィンドウにレコードを割り当てます。

**構文**

``` sql
tumble(time_attr, interval [, timezone])
```

**引数**
- `time_attr` — 日付と時間。[DateTime](../data-types/datetime.md)。
- `interval` — ウィンドウの間隔 [Interval](../data-types/special-data-types/interval.md)。
- `timezone` — [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。

**返される値**

- 対応するタンブルウィンドウの包括的下限と排他的上限。[Tuple](../data-types/tuple.md)([DateTime](../data-types/datetime.md), [DateTime](../data-types/datetime.md))。

**例**

クエリ:

``` sql
SELECT tumble(now(), toIntervalDay('1'));
```

結果:

``` text
┌─tumble(now(), toIntervalDay('1'))─────────────┐
│ ('2024-07-04 00:00:00','2024-07-05 00:00:00') │
└───────────────────────────────────────────────┘
```

## tumbleStart

対応する[タンブルウィンドウ](#tumble)の包括的下限を返します。

**構文**

``` sql
tumbleStart(time_attr, interval [, timezone]);
```

**引数**

- `time_attr` — 日付と時間。[DateTime](../data-types/datetime.md)。
- `interval` — ウィンドウの間隔 [Interval](../data-types/special-data-types/interval.md)。
- `timezone` — [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。

上記のパラメータは[tuple](../data-types/tuple.md)としても関数に渡すことができます。

**返される値**

- 対応するタンブルウィンドウの包括的下限。[DateTime](../data-types/datetime.md), [Tuple](../data-types/tuple.md)または[UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT tumbleStart(now(), toIntervalDay('1'));
```

結果:

```response
┌─tumbleStart(now(), toIntervalDay('1'))─┐
│                    2024-07-04 00:00:00 │
└────────────────────────────────────────┘
```

## tumbleEnd

対応する[タンブルウィンドウ](#tumble)の排他的上限を返します。

**構文**

``` sql
tumbleEnd(time_attr, interval [, timezone]);
```

**引数**

- `time_attr` — 日付と時間。[DateTime](../data-types/datetime.md)。
- `interval` — ウィンドウの間隔 [Interval](../data-types/special-data-types/interval.md)。
- `timezone` — [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。

上記のパラメータは[tuple](../data-types/tuple.md)としても関数に渡すことができます。

**返される値**

- 対応するタンブルウィンドウの排他的上限。[DateTime](../data-types/datetime.md), [Tuple](../data-types/tuple.md)または[UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT tumbleEnd(now(), toIntervalDay('1'));
```

結果:

```response
┌─tumbleEnd(now(), toIntervalDay('1'))─┐
│                  2024-07-05 00:00:00 │
└──────────────────────────────────────┘
```

## hop

ホッピングタイムウィンドウは固定された期間（`window_interval`）を持ち、指定されたホップ間隔（`hop_interval`）で移動します。`hop_interval`が`window_interval`より小さい場合、ホッピングウィンドウは重複しています。したがって、レコードは複数のウィンドウに割り当てることができます。

``` sql
hop(time_attr, hop_interval, window_interval [, timezone])
```

**引数**

- `time_attr` — 日付と時間。[DateTime](../data-types/datetime.md)。
- `hop_interval` — 正のホップ間隔。[Interval](../data-types/special-data-types/interval.md)。
- `window_interval` — 正のウィンドウ間隔。[Interval](../data-types/special-data-types/interval.md)。
- `timezone` — [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。

**返される値**

- 対応するホッピングウィンドウの包括的下限と排他的上限。[Tuple](../data-types/tuple.md)([DateTime](../data-types/datetime.md), [DateTime](../data-types/datetime.md))。

:::note
一つのレコードが複数のホップウィンドウに割り当てられるため、hop関数が`WINDOW VIEW`なしで使用されるとき、関数は**最初の**ウィンドウの境界のみを返します。
:::

**例**

クエリ:

``` sql
SELECT hop(now(), INTERVAL '1' DAY, INTERVAL '2' DAY);
```

結果:

``` text
┌─hop(now(), toIntervalDay('1'), toIntervalDay('2'))─┐
│ ('2024-07-03 00:00:00','2024-07-05 00:00:00')      │
└────────────────────────────────────────────────────┘
```

## hopStart

対応する[ホッピングウィンドウ](#hop)の包括的下限を返します。

**構文**

``` sql
hopStart(time_attr, hop_interval, window_interval [, timezone]);
```
**引数**

- `time_attr` — 日付と時間。[DateTime](../data-types/datetime.md)。
- `hop_interval` — 正のホップ間隔。[Interval](../data-types/special-data-types/interval.md)。
- `window_interval` — 正のウィンドウ間隔。[Interval](../data-types/special-data-types/interval.md)。
- `timezone` — [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。

上記のパラメータは[tuple](../data-types/tuple.md)としても関数に渡すことができます。

**返される値**

- 対応するホッピングウィンドウの包括的下限。[DateTime](../data-types/datetime.md), [Tuple](../data-types/tuple.md)または[UInt32](../data-types/int-uint.md)。

:::note
一つのレコードが複数のホップウィンドウに割り当てられるため、hop関数が`WINDOW VIEW`なしで使用されるとき、関数は**最初の**ウィンドウの境界のみを返します。
:::

**例**

クエリ:

``` sql
SELECT hopStart(now(), INTERVAL '1' DAY, INTERVAL '2' DAY);
```

結果:

``` text
┌─hopStart(now(), toIntervalDay('1'), toIntervalDay('2'))─┐
│                                     2024-07-03 00:00:00 │
└─────────────────────────────────────────────────────────┘
```

## hopEnd

対応する[ホッピングウィンドウ](#hop)の排他的上限を返します。

**構文**

``` sql
hopEnd(time_attr, hop_interval, window_interval [, timezone]);
```
**引数**

- `time_attr` — 日付と時間。[DateTime](../data-types/datetime.md)。
- `hop_interval` — 正のホップ間隔。[Interval](../data-types/special-data-types/interval.md)。
- `window_interval` — 正のウィンドウ間隔。[Interval](../data-types/special-data-types/interval.md)。
- `timezone` — [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。

上記のパラメータは[tuple](../data-types/tuple.md)としても関数に渡すことができます。

**返される値**

- 対応するホッピングウィンドウの排他的上限。[DateTime](../data-types/datetime.md), [Tuple](../data-types/tuple.md)または[UInt32](../data-types/int-uint.md)。

:::note
一つのレコードが複数のホップウィンドウに割り当てられるため、hop関数が`WINDOW VIEW`なしで使用されるとき、関数は**最初の**ウィンドウの境界のみを返します。
:::

**例**

クエリ:

``` sql
SELECT hopEnd(now(), INTERVAL '1' DAY, INTERVAL '2' DAY);
```

結果:

``` text
┌─hopEnd(now(), toIntervalDay('1'), toIntervalDay('2'))─┐
│                                   2024-07-05 00:00:00 │
└───────────────────────────────────────────────────────┘

```

## 関連コンテンツ

- ブログ: [ClickHouseでの時系列データの処理](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
