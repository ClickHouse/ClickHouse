---
slug: /sql-reference/statements/create/dictionary/sources/executable-pool
title: '実行可能プールの Dictionary ソース'
sidebar_position: 4
sidebar_label: '実行可能プール'
description: 'ClickHouse で実行可能プールを Dictionary ソースとして設定します。'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

実行可能プールでは、プロセスプールからデータを読み込めます。
この辞書ソースは、ソースからすべてのデータを読み込む必要がある dictionary layout では動作しません。

辞書が次のいずれかのレイアウトを使って[保存されている](../layouts/#storing-dictionaries-in-memory)場合、実行可能プールを使用できます。

* `cache`
* `complex_key_cache`
* `ssd_cache`
* `complex_key_ssd_cache`
* `direct`
* `complex_key_direct`

実行可能プールは、指定されたコマンドでプロセスプールを起動し、それらが終了するまで実行し続けます。プログラムは、利用可能な間は STDIN からデータを読み取り、結果を STDOUT に出力する必要があります。STDIN 上の次のデータブロックを待機することもできます。ClickHouse はデータブロックの処理後に STDIN を閉じません。必要に応じて、別の chunk のデータをパイプで渡します。実行可能スクリプトはこのデータ処理方式に対応している必要があります。つまり、STDIN をポーリングし、STDOUT には早めにデータをフラッシュする必要があります。

設定例:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE_POOL(
        command 'while read key; do printf "$key\tData for key $key\n"; done'
        format 'TabSeparated'
        pool_size 10
        max_command_execution_time 10
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="設定ファイル">
    ```xml
    <source>
        <executable_pool>
            <command><command>while read key; do printf "$key\tData for key $key\n"; done</command</command>
            <format>TabSeparated</format>
            <pool_size>10</pool_size>
            <max_command_execution_time>10<max_command_execution_time>
            <implicit_key>false</implicit_key>
        </executable_pool>
    </source>
    ```
  </TabItem>
</Tabs>

設定項目:

| Setting                       | Description                                                                                                                                                                                                                                                                                                             |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                     | 実行可能ファイルの絶対パス、またはファイル名です (プログラムのディレクトリが `PATH` に含まれている場合) 。                                                                                                                                                                                                                                                             |
| `format`                      | ファイルフォーマットです。[フォーマット](/ja/sql-reference/formats) に記載されているすべてのフォーマットをサポートします。                                                                                                                                                                                                                                               |
| `pool_size`                   | プールのサイズです。`pool_size` に `0` を指定すると、プールサイズの制限はありません。デフォルト値は `16` です。                                                                                                                                                                                                                                                     |
| `command_termination_timeout` | 実行可能スクリプトには、メインの read-write ループを含める必要があります。辞書が破棄されるとパイプが閉じられ、実行可能ファイルは ClickHouse が子プロセスに SIGTERM シグナルを送信する前に、`command_termination_timeout` 秒以内に終了する必要があります。秒単位で指定します。デフォルト値は `10` です。任意です。                                                                                                                             |
| `max_command_execution_time`  | データブロックを処理する際の、実行可能スクリプトコマンドの最大実行時間です。秒単位で指定します。デフォルト値は `10` です。任意です。                                                                                                                                                                                                                                                   |
| `command_read_timeout`        | コマンドの stdout からデータを読み取る際のタイムアウトです。Milliseconds 単位で指定します。デフォルト値は `10000` です。任意です。                                                                                                                                                                                                                                        |
| `command_write_timeout`       | コマンドの stdin にデータを書き込む際のタイムアウトです。Milliseconds 単位で指定します。デフォルト値は `10000` です。任意です。                                                                                                                                                                                                                                          |
| `implicit_key`                | 実行可能ソースファイルは値だけを返すことができ、要求されたキーとの対応は、結果内の行の順序によって暗黙的に決定されます。デフォルト値は `false` です。任意です。                                                                                                                                                                                                                                    |
| `execute_direct`              | `execute_direct` = `1` の場合、`command` は [user&#95;scripts&#95;path](/ja/operations/server-configuration-parameters/settings#user_scripts_path) で指定された user&#95;scripts フォルダー内で検索されます。追加のスクリプト引数は空白区切りで指定できます。例: `script_name arg1 arg2`。`execute_direct` = `0` の場合、`command` は `bin/sh -c` の引数として渡されます。デフォルト値は `1` です。任意です。 |
| `send_chunk_header`           | プロセスに chunk のデータを送信する前に、行数を送るかどうかを制御します。デフォルト値は `false` です。任意です。                                                                                                                                                                                                                                                        |

この辞書ソースは、XML 設定でのみ構成できます。DDL 経由で実行可能ソースを使う辞書の作成は無効化されています。そうしないと、DB ユーザーが ClickHouse ノード上で任意のバイナリを実行できてしまうためです。