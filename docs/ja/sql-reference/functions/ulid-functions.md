---
slug: /ja/sql-reference/functions/ulid-functions
sidebar_position: 190
sidebar_label: ULID
---

# ULIDを扱うための関数

## generateULID

[ULID](https://github.com/ulid/spec)を生成します。

**構文**

``` sql
generateULID([x])
```

**引数**

- `x` — [サポートされるデータ型](../data-types/index.md#data_types)のいずれかを結果とする[式](../../sql-reference/syntax.md#syntax-expressions)。結果の値は破棄されますが、関数が1つのクエリ内で複数回呼び出されたときに[共通部分式除去 (CSE)](../../sql-reference/functions/index.md#common-subexpression-elimination)をバイパスするために使用されます。オプションの引数です。

**戻り値**

[FixedString](../data-types/fixedstring.md)型の値。

**使用例**

``` sql
SELECT generateULID()
```

``` text
┌─generateULID()─────────────┐
│ 01GNB2S2FGN2P93QPXDNB4EN2R │
└────────────────────────────┘
```

**1行に複数の値を生成する必要がある場合の使用例**

```sql
SELECT generateULID(1), generateULID(2)
```

``` text
┌─generateULID(1)────────────┬─generateULID(2)────────────┐
│ 01GNB2SGG4RHKVNT9ZGA4FFMNP │ 01GNB2SGG4V0HMQVH4VBVPSSRB │
└────────────────────────────┴────────────────────────────┘
```

## ULIDStringToDateTime

この関数はULIDからタイムスタンプを抽出します。

**構文**

``` sql
ULIDStringToDateTime(ulid[, timezone])
```

**引数**

- `ulid` — 入力ULID。[String](../data-types/string.md)または[FixedString(26)](../data-types/fixedstring.md)。
- `timezone` — 返される値のための[タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。[String](../data-types/string.md)。

**戻り値**

- ミリ秒精度のタイムスタンプ。[DateTime64(3)](../data-types/datetime64.md)。

**使用例**

``` sql
SELECT ULIDStringToDateTime('01GNB2S2FGN2P93QPXDNB4EN2R')
```

``` text
┌─ULIDStringToDateTime('01GNB2S2FGN2P93QPXDNB4EN2R')─┐
│                            2022-12-28 00:40:37.616 │
└────────────────────────────────────────────────────┘
```

## 関連項目

- [UUID](../../sql-reference/functions/uuid-functions.md)
