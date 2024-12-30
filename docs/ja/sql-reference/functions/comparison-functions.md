---
slug: /ja/sql-reference/functions/comparison-functions
sidebar_position: 35
sidebar_label: 比較
---

# 比較関数

以下の比較関数は Uint8 として 0 または 1 を返します。

次のタイプが比較できます:
- 数値
- 文字列と固定長文字列
- 日付
- 時間を含む日付

同じグループ内の値のみが比較可能です（例：UInt16 と UInt64）。しかし、異なるグループ間では比較できません（例：UInt16 と DateTime）。

文字列はバイト単位で比較されます。これは、一方の文字列がUTF-8エンコードされたマルチバイト文字を含む場合、予期しない結果を招くことがあります。

文字列 S1 が別の文字列 S2 をプレフィックスとして持つ場合、S1 は S2 よりも長いと見なされます。

## equals, `=`, `==` 演算子 {#equals}

**構文**

```sql
equals(a, b)
```

エイリアス:
- `a = b` (演算子)
- `a == b` (演算子)

## notEquals, `!=`, `<>` 演算子 {#notequals}

**構文**

```sql
notEquals(a, b)
```

エイリアス:
- `a != b` (演算子)
- `a <> b` (演算子)

## less, `<` 演算子 {#less}

**構文**

```sql
less(a, b)
```

エイリアス:
- `a < b` (演算子)

## greater, `>` 演算子 {#greater}

**構文**

```sql
greater(a, b)
```

エイリアス:
- `a > b` (演算子)

## lessOrEquals, `<=` 演算子 {#lessorequals}

**構文**

```sql
lessOrEquals(a, b)
```

エイリアス:
- `a <= b` (演算子)

## greaterOrEquals, `>=` 演算子 {#greaterorequals}

**構文**

```sql
greaterOrEquals(a, b)
```

エイリアス:
- `a >= b` (演算子)
