---
slug: /sql-reference/statements/create/dictionary/sources/executable-file
title: '実行可能ファイルの Dictionary ソース'
sidebar_position: 3
sidebar_label: '実行可能ファイル'
description: 'ClickHouse で実行可能ファイルを Dictionary ソースとして設定します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

実行可能ファイルの扱いは、[Dictionary がメモリにどのように格納されるか](../layouts/)によって異なります。Dictionary が `cache` および `complex_key_cache` を使用して格納されている場合、ClickHouse は実行可能ファイルの STDIN にリクエストを送信して必要なキーを要求します。それ以外の場合、ClickHouse は実行可能ファイルを起動し、その出力を Dictionary データとして扱います。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE(
        command 'cat /opt/dictionaries/os.tsv'
        format 'TabSeparated'
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <source>
        <executable>
            <command>cat /opt/dictionaries/os.tsv</command>
            <format>TabSeparated</format>
            <implicit_key>false</implicit_key>
        </executable>
    </source>
    ```
  </TabItem>
</Tabs>

設定項目:

| Setting                       | Description                                                                                                                                                                                                                                                                                                              |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `command`                     | 実行可能ファイルの絶対パス、またはファイル名 (コマンドのディレクトリが `PATH` に含まれている場合) 。                                                                                                                                                                                                                                                                 |
| `format`                      | ファイルのフォーマットです。[Formats](/ja/sql-reference/formats) で説明されているすべてのフォーマットをサポートしています。                                                                                                                                                                                                                                            |
| `command_termination_timeout` | 実行可能スクリプトには、メインの read-write ループが含まれている必要があります。Dictionary が破棄されるとパイプが閉じられ、ClickHouse が子プロセスに SIGTERM シグナルを送信するまで、実行可能ファイルは `command_termination_timeout` 秒以内にシャットダウンする必要があります。秒単位で指定します。デフォルト値は `10` です。任意です。                                                                                                             |
| `command_read_timeout`        | command の stdout からデータを読み取る際のタイムアウト (ミリ秒単位) 。デフォルト値は `10000` です。任意です。                                                                                                                                                                                                                                                    |
| `command_write_timeout`       | command の stdin にデータを書き込む際のタイムアウト (ミリ秒単位) 。デフォルト値は `10000` です。任意です。                                                                                                                                                                                                                                                      |
| `implicit_key`                | 実行可能ファイルのソースは値のみを返すことができ、要求されたキーとの対応関係は結果内の行の順序によって暗黙的に決定されます。デフォルト値は `false` です。                                                                                                                                                                                                                                        |
| `execute_direct`              | `execute_direct` = `1` の場合、`command` は [user&#95;scripts&#95;path](/ja/operations/server-configuration-parameters/settings#user_scripts_path) で指定された user&#95;scripts フォルダー内から検索されます。追加のスクリプト引数は空白区切りで指定できます。例: `script_name arg1 arg2`。`execute_direct` = `0` の場合、`command` は `bin/sh -c` の引数として渡されます。デフォルト値は `0` です。任意です。 |
| `send_chunk_header`           | データの chunk をプロセスに送信する前に行数を送信するかどうかを制御します。デフォルト値は `false` です。任意です。                                                                                                                                                                                                                                                        |

この Dictionary ソースは XML 設定でのみ構成できます。実行可能ソースを使用する Dictionary の DDL による作成は無効化されています。そうしないと、DB ユーザーが ClickHouse ノード上で任意のバイナリを実行できてしまうためです。