---
description: 'ClickHouse の String データ型に関するドキュメント'
sidebar_label: 'String'
sidebar_position: 8
slug: /sql-reference/data-types/string
title: 'String'
doc_type: 'reference'
---

任意の長さの文字列を扱えます。長さに制限はありません。値には、ヌルバイトを含む任意のバイト列を格納できます。
String 型は、他の DBMSs の VARCHAR、BLOB、CLOB などの型を置き換えるものです。

テーブル作成時には、文字列フィールドに数値パラメータを指定できます (例: `VARCHAR(255)`) が、ClickHouse はそれらを無視します。

別名:

* `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

<div id="encodings">
  ## エンコーディング
</div>

ClickHouse には、エンコーディングという概念がありません。文字列には任意のバイト列を含めることができ、それらはそのまま保存され、そのまま出力されます。
テキストを保存する必要がある場合は、UTF-8 エンコーディングを使用することを推奨します。少なくとも、端末が UTF-8 を使用していれば (推奨) 、変換せずに値を読み書きできます。
同様に、文字列を扱う一部の関数には、文字列が UTF-8 でエンコードされたテキストを表すバイト列を含んでいることを前提とした別バージョンがあります。
たとえば、[length](/ja/sql-reference/functions/array-functions#length) 関数は文字列の長さをバイト単位で計算します。一方、[lengthUTF8](../functions/string-functions.md#lengthUTF8) 関数は、値が UTF-8 でエンコードされていることを前提に、文字列の長さを Unicode のコードポイント単位で計算します。