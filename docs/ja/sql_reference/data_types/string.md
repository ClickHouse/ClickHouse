---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 44
toc_title: "\u6587\u5B57\u5217"
---

# 文字列 {#string}

任意の長さの文字列。 長さは限定されない。 値には、nullバイトを含む任意のバイトセットを含めることができます。
文字列型は、他のdbmsのvarchar、blob、clobなどの型を置き換えます。

## エンコード {#encodings}

ClickHouseにはエンコーディングの概念はありません。 文字列には、任意のバイトセットを含めることができます。
が必要な場合は店舗テキストの使用をお勧めしまutf-8エンコーディングです。 少なくとも、端末がutf-8を使用している場合（推奨）、変換を行わずに値を読み書きできます。
同様に、文字列を操作するための特定の関数には、文字列にutf-8でエンコードされたテキストを表すバイトのセットが含まれているという前提の下
たとえば、 ‘length’ 関数は文字列の長さをバイト単位で計算します。 ‘lengthUTF8’ 関数を計算し、文字列の長さはUnicodeコードポイント価値をとした場合は、UTF-8エンコードされます。

[元の記事](https://clickhouse.tech/docs/en/data_types/string/) <!--hide-->
