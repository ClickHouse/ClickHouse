---
slug: /ja/sql-reference/aggregate-functions/reference/groupconcat
sidebar_position: 363
sidebar_label: groupConcat
title: groupConcat
---

文字列のグループから連結された文字列を計算します。区切り文字で区切ることも可能で、要素の最大数を制限することもできます。

**構文**

``` sql
groupConcat[(delimiter [, limit])](expression);
```

**引数**

- `expression` — 連結される文字列を出力する式またはカラム名。
- `delimiter` — 連結される値を区切るために使われる[文字列](../../../sql-reference/data-types/string.md)。このパラメータはオプションで、指定しない場合は空の文字列がデフォルトとなります。
- `limit` — 連結する要素の最大数を指定する正の[整数](../../../sql-reference/data-types/int-uint.md)。もし要素がそれ以上ある場合、超過した要素は無視されます。このパラメータもオプションです。

:::note
区切り文字が指定され、limitが指定されていない場合、区切り文字は最初のパラメータでなければなりません。区切り文字とlimitの両方が指定されている場合、区切り文字はlimitよりも前に指定する必要があります。
:::

**返される値**

- カラムまたは式の連結された値からなる[文字列](../../../sql-reference/data-types/string.md)を返します。グループに要素がない、またはすべての要素がnullの場合、かつ関数がnull値のみの処理を指定していない場合、結果はnull値を持つnullableな文字列となります。

**例**

入力テーブル:

``` text
┌─id─┬─name─┐
│ 1  │  John│
│ 2  │  Jane│
│ 3  │   Bob│
└────┴──────┘
```

1. 区切り文字なしの基本的な使用法:

クエリ:

``` sql
SELECT groupConcat(Name) FROM Employees;
```

結果:

``` text
JohnJaneBob
```

すべての名前を区切りなしで一つの連続した文字列に連結します。


2. 区切り文字としてカンマを使用する場合:

クエリ:

``` sql
SELECT groupConcat(', ')(Name)  FROM Employees;
```

結果:

``` text
John, Jane, Bob
```

この出力では、カンマとスペースで名前が区切られています。


3. 連結する要素数の制限

クエリ:

``` sql
SELECT groupConcat(', ', 2)(Name) FROM Employees;
```

結果:

``` text
John, Jane
```

このクエリでは、テーブルにもっと名前があっても、最初の2つの名前だけが結果として表示されます。
