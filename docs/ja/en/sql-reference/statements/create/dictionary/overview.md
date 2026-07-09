---
description: 'Dictionary の作成と設定に関するドキュメント'
sidebar_label: '概要'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary
title: 'CREATE DICTIONARY'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import CloudSupportedBadge from '@theme/badges/CloudSupportedBadge';

<div id="create-dictionary">
  # CREATE DICTIONARY
</div>

Dictionary は、さまざまな種類の参照リストで利用しやすいマッピング (`key -> attributes`) です。
ClickHouse は、クエリ内で使用できる、Dictionary を操作するための特別な関数をサポートしています。参照テーブルと `JOIN` するよりも、関数と組み合わせて Dictionary を使用するほうが、簡単で効率的です。

Dictionary は次の 2 つの方法で作成できます。

* [DDL クエリを使用](#creating-a-dictionary-with-a-ddl-query) (推奨)
* [設定ファイルを使用](#creating-a-dictionary-with-a-configuration-file)

<div id="creating-a-dictionary-with-a-ddl-query">
  ## DDLクエリを使ったDictionaryの作成
</div>

<CloudSupportedBadge />

DictionaryはDDLクエリで作成できます。
これは推奨される方法です。DDLで作成したDictionaryには、次のような利点があるためです。

* サーバー設定ファイルに追加のレコードを記述する必要がありません。
* Dictionaryは、テーブルやビューと同様に第一級のエンティティとして使用できます。
* Dictionaryテーブル関数ではなく、使い慣れた`SELECT`構文を使ってデータを直接読み取れます。なお、`SELECT`ステートメントでDictionaryに直接アクセスする場合、キャッシュされたDictionaryではキャッシュされているデータのみが返され、キャッシュされていないDictionaryでは保持しているすべてのデータが返されます。
* Dictionaryは簡単にリネームできます。

<div id="syntax">
  ### 構文
</div>

```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1  type1  [DEFAULT | EXPRESSION expr1] [IS_OBJECT_ID],
    key2  type2  [DEFAULT | EXPRESSION expr2],
    attr1 type2  [DEFAULT | EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2  [DEFAULT | EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

| 句                                           | 説明                                                                     |
| ------------------------------------------- | ---------------------------------------------------------------------- |
| [属性](./attributes.md)                       | Dictionary の属性は、テーブルのカラムと同様に指定します。必須のプロパティは型のみで、それ以外はすべてデフォルト値を設定できます。 |
| PRIMARY KEY                                 | Dictionary のルックアップに使用するキーカラムを定義します。レイアウトに応じて、1 つ以上の属性をキーとして指定できます。     |
| [`SOURCE`](./sources/overview.md)           | Dictionary のデータソースを定義します (例: ClickHouse table、HTTP、PostgreSQL) 。       |
| [`LAYOUT`](./layouts/overview.md)           | Dictionary をメモリ内にどのように格納するかを制御します (例: `FLAT`、`HASHED`、`CACHE`) 。       |
| [`LIFETIME`](./lifetime.md)                 | Dictionary の更新間隔を設定します。                                                |
| [`ON CLUSTER`](../../../distributed-ddl.md) | クラスター上に Dictionary を作成します。省略可能です。                                      |
| `SETTINGS`                                  | Dictionary の追加設定です。省略可能です。                                             |
| `COMMENT`                                   | Dictionary にテキストコメントを追加します。省略可能です。                                     |

<div id="creating-a-dictionary-with-a-configuration-file">
  ## 設定ファイルを使用した Dictionary の作成
</div>

<CloudNotSupportedBadge />

:::note
設定ファイルを使用した Dictionary の作成は ClickHouse Cloud ではサポートされていません。DDL (上記を参照) を使用し、`default` ユーザーとして Dictionary を作成してください。
:::

Dictionary の設定ファイルは次の形式です。

```xml
<clickhouse>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of dictionary sections in a configuration file. -->
    </dictionary>

</clickhouse>
```

同じファイルに、いくつでも辞書を設定できます。

<div id="related-content">
  ## 関連コンテンツ
</div>

* [レイアウト](/ja/sql-reference/statements/create/dictionary/layouts) — Dictionary がメモリ内でどのように格納されるか
* [SOURCES](/ja/sql-reference/statements/create/dictionary/sources) — データソースへの接続
* [ライフタイム](./lifetime.md) — 自動リフレッシュ設定
* [属性](./attributes.md) — キーと属性の設定
* [埋め込み Dictionaries](./embedded.md) — 組み込みの geobase Dictionary
* [system.dictionaries](../../../../operations/system-tables/dictionaries.md) — Dictionary の情報を含むシステムテーブル