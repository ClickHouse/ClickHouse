---
description: 'メモリ内に Dictionary を格納するためのレイアウトの種類'
sidebar_label: '概要'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary/layouts
title: 'Dictionary のレイアウト'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="storing-dictionaries-in-memory">
  ## Dictionary レイアウトの種類
</div>

Dictionary をメモリ内に格納する方法にはさまざまなものがあり、それぞれ CPU と RAM 使用量のトレードオフがあります。

| Layout                                                                                                     | Description                                                                               |
| ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| [flat](./flat.md)                                                                                          | キーで索引付けされたフラットな配列にデータを格納します。最も高速なレイアウトですが、キーは `UInt64` で、`max_array_size` の範囲内である必要があります。 |
| [hashed](./hashed.md)                                                                                      | データをハッシュテーブルに格納します。キーサイズの制限はなく、任意の数の要素をサポートします。                                           |
| [sparse&#95;hashed](./hashed.md#sparse_hashed)                                                             | `hashed` と同様ですが、CPU と引き換えにメモリ使用量を削減します。                                                   |
| [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed)                                               | `hashed` と同様で、複合キー向けです。                                                                   |
| [complex&#95;key&#95;sparse&#95;hashed](./hashed.md#complex_key_sparse_hashed)                             | `sparse_hashed` と同様で、複合キー向けです。                                                            |
| [hashed&#95;array](./hashed-array.md)                                                                      | 属性は配列に格納され、ハッシュテーブルでキーを配列インデックスに対応付けます。属性が多い場合にメモリ効率に優れます。                                |
| [complex&#95;key&#95;hashed&#95;array](./hashed-array.md#complex_key_hashed_array)                         | `hashed_array` と同様で、複合キー向けです。                                                             |
| [range&#95;hashed](./range-hashed.md)                                                                      | 順序付きの範囲を持つハッシュテーブルです。キー + 日付/時刻範囲によるルックアップをサポートします。                                       |
| [complex&#95;key&#95;range&#95;hashed](./range-hashed.md#complex_key_range_hashed)                         | `range_hashed` と同様で、複合キー向けです。                                                             |
| [cache](./cache.md)                                                                                        | 固定サイズのインメモリ cache です。頻繁にアクセスされるキーのみを格納します。                                                |
| [complex&#95;key&#95;cache](/ja/sql-reference/statements/create/dictionary/layouts/hashed#complex_key_hashed) | `cache` と同様で、複合キー向けです。                                                                    |
| [ssd&#95;cache](./ssd-cache.md)                                                                            | `cache` と同様ですが、データを SSD に格納し、インメモリの索引を使用します。                                              |
| [complex&#95;key&#95;ssd&#95;cache](./ssd-cache.md#complex_key_ssd_cache)                                  | `ssd_cache` と同様で、複合キー向けです。                                                                |
| [direct](./direct.md)                                                                                      | メモリ内には格納せず、各リクエストごとにログソースへ直接問い合わせます。                                                      |
| [complex&#95;key&#95;direct](./direct.md#complex_key_direct)                                               | `direct` と同様で、複合キー向けです。                                                                   |
| [ip&#95;trie](./ip-trie.md)                                                                                | IP プレフィックスを高速にルックアップするための trie 構造です (CIDR ベース) 。                                          |

:::tip 推奨レイアウト
[flat](./flat.md)、[hashed](./hashed.md)、および [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed) は、最高のクエリパフォーマンスを提供します。
Caching レイアウトは、パフォーマンスが低下する可能性があり、パラメータ調整も難しいため推奨されません。詳細は [cache](./cache.md) を参照してください。
:::

<div id="specify-dictionary-layout">
  ## Dictionary レイアウトを指定する
</div>

<CloudDetails />

Dictionary レイアウトは、`LAYOUT` 句 (DDL の場合) または設定ファイル定義の `layout` 設定を使用して指定できます。

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY (...)
    ...
    LAYOUT(LAYOUT_TYPE(param value)) -- レイアウト設定
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <clickhouse>
        <dictionary>
            ...
            <layout>
                <layout_type>
                    <!-- レイアウト設定 -->
                </layout_type>
            </layout>
            ...
        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

DDL 構文の詳細については、[CREATE DICTIONARY](../overview.md) も参照してください。

レイアウトに `complex-key*` を含まない Dictionary は [UInt64](/ja/sql-reference/data-types/int-uint.md) 型のキーを持ち、`complex-key*` Dictionary は複合キー (任意の型で構成されるキー) を持ちます。

**数値キーの例** (カラム `key_column` は [UInt64](/ja/sql-reference/data-types/int-uint.md) 型です) :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        key_column UInt64,
        ...
    )
    PRIMARY KEY key_column
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <structure>
        <id>
            <name>key_column</name>
        </id>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<br />

**複合キーの例** (キーは [String](/ja/sql-reference/data-types/string.md) 型の要素を 1 つ持ちます) :

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        country_code String,
        ...
    )
    PRIMARY KEY country_code
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <structure>
        <key>
            <attribute>
                <name>country_code</name>
                <type>String</type>
            </attribute>
        </key>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<div id="improve-performance">
  ## Dictionaryのパフォーマンスを改善する
</div>

Dictionaryのパフォーマンスを改善する方法はいくつかあります。

* Dictionaryを扱う関数は、`GROUP BY` の後で呼び出します。
* 取得する属性を injective としてマークします。
  異なるキーに異なる属性値が対応している属性は、injective と呼ばれます。
  そのため、`GROUP BY` でキーから属性値を取得する関数を使用している場合、この関数は自動的に `GROUP BY` の外に出されます。

ClickHouse は、Dictionaryに関するエラー時に例外を生成します。
エラーの例としては、次のようなものがあります。

* アクセス対象のDictionaryを読み込めませんでした。
* `cached` Dictionaryのクエリ時のエラー。

Dictionaryの一覧とそのステータスは、[system.dictionaries](/ja/operations/system-tables/dictionaries.md) テーブルで確認できます。