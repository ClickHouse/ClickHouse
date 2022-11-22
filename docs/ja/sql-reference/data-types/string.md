---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: "\u6587\u5B57\u5217"
---

# 文字列 {#string}

任意の長さの文字列。 長さは限定されない。 値には、nullバイトを含む任意のバイトセットを含めることができます。
文字列型は、他のDbmsのVARCHAR型、BLOB型、CLOB型などを置き換えます。

## エンコード {#encodings}

ClickHouseにはエンコーディングの概念はありません。 文字列には、任意のバイトセットを含めることができます。
が必要な場合は店舗テキストの使用をお勧めしまUTF-8エンコーディングです。 少なくとも、端末がUTF-8（推奨）を使用している場合は、変換を行わずに値を読み書きできます。
同様に、文字列を操作するための特定の関数には、UTF-8でエンコードされたテキストを表すバイトセットが文字列に含まれているという前提の下で
例えば、 ‘length’ 関数は、文字列の長さをバイト単位で計算します。 ‘lengthUTF8’ 関数は、値がUTF-8でエンコードされていると仮定して、Unicodeコードポイントで文字列の長さを計算します。

[元の記事](https://clickhouse.com/docs/en/data_types/string/) <!--hide-->
