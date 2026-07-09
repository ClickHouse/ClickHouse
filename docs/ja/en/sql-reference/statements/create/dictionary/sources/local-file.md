---
slug: /sql-reference/statements/create/dictionary/sources/local-file
title: 'ローカルファイル Dictionary ソース'
sidebar_position: 2
sidebar_label: 'ローカルファイル'
description: 'ローカルファイルを ClickHouse の Dictionary ソースとして設定します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

ローカルファイルソースは、ローカルfilesystem上のファイルからDictionaryデータを読み込みます。これは、TSV、CSV、またはその他の[対応フォーマット](/ja/sql-reference/formats)のフラットファイルとして保存できる、小規模で静的なルックアップテーブルに適しています。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <source>
      <file>
        <path>/opt/dictionaries/os.tsv</path>
        <format>TabSeparated</format>
      </file>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

設定フィールド:

| 設定       | 説明                                                                           |
| -------- | ---------------------------------------------------------------------------- |
| `path`   | ファイルの絶対パスです。                                                                 |
| `format` | ファイルのフォーマットです。[Formats](/ja/sql-reference/formats)で説明されているすべてのフォーマットをサポートしています。 |

ソース`FILE`を持つDictionaryをDDLコマンド (`CREATE DICTIONARY ...`) で作成する場合、DBユーザーがClickHouse node上の任意のファイルにアクセスできないようにするため、ソースファイルは`user_files`ディレクトリ内に配置する必要があります。

**関連項目**

* [Dictionary function](/ja/sql-reference/table-functions/dictionary)