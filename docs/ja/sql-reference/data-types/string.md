---
slug: /ja/sql-reference/data-types/string
sidebar_position: 8
sidebar_label: String
---

# String

任意の長さの文字列です。長さに制限はありません。値は、ヌルバイトを含む任意のバイトセットを含むことができます。String型は、他のDBMSのVARCHAR、BLOB、CLOBなどの型を置き換えます。

テーブルを作成する際に、文字列フィールド用に数値パラメータを設定することができます（例: `VARCHAR(255)`）。ただし、ClickHouseはこれを無視します。

エイリアス：

- `String` — `LONGTEXT`, `MEDIUMTEXT`, `TINYTEXT`, `TEXT`, `LONGBLOB`, `MEDIUMBLOB`, `TINYBLOB`, `BLOB`, `VARCHAR`, `CHAR`, `CHAR LARGE OBJECT`, `CHAR VARYING`, `CHARACTER LARGE OBJECT`, `CHARACTER VARYING`, `NCHAR LARGE OBJECT`, `NCHAR VARYING`, `NATIONAL CHARACTER LARGE OBJECT`, `NATIONAL CHARACTER VARYING`, `NATIONAL CHAR VARYING`, `NATIONAL CHARACTER`, `NATIONAL CHAR`, `BINARY LARGE OBJECT`, `BINARY VARYING`,

## エンコーディング

ClickHouseにはエンコーディングの概念がありません。文字列は任意のバイトセットを含むことができ、それらはそのまま保存および出力されます。テキストを保存する必要がある場合は、UTF-8エンコーディングの使用を推奨します。少なくとも、あなたの端末がUTF-8を使用しているのであれば（推奨されるとおり）、値を変換せずに読み書きできます。

同様に、文字列操作のための特定の関数には、文字列がUTF-8でエンコードされたテキストを表すバイトセットであると仮定して動作する別のバリエーションがあります。たとえば、[length](../functions/string-functions.md#length)関数は、文字列の長さをバイト単位で計算しますが、[lengthUTF8](../functions/string-functions.md#lengthutf8)関数は、値がUTF-8でエンコードされていると仮定してUnicodeコードポイント単位で文字列の長さを計算します。
