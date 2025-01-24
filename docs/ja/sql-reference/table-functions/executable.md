---
slug: /ja/engines/table-functions/executable
sidebar_position: 50
sidebar_label:  executable
keywords: [udf, user defined function, clickhouse, executable, table, function]
---

# UDF用の executable テーブル関数

`executable` テーブル関数は、ユーザ定義関数（UDF）の出力に基づいてテーブルを作成します。この関数は、**stdout** に行を出力するスクリプト内で定義します。実行可能なスクリプトは `users_scripts` ディレクトリに保存され、任意のソースからデータを読み取ることができます。ClickHouse サーバーに、実行可能スクリプトを動作させるために必要なパッケージがインストールされていることを確認してください。例えば、Python スクリプトの場合、必要な Python パッケージがサーバーにインストールされている必要があります。

入力クエリを一つ以上オプションで含め、結果を **stdin** にストリームしてスクリプトが読み取れるようにすることができます。

:::note
通常の UDF 関数と `executable` テーブル関数および `Executable` テーブルエンジンの大きな利点は、通常の UDF 関数は行数を変更できない点です。例えば、入力が100行であれば、結果も100行でなければなりません。しかし、`executable` テーブル関数または `Executable` テーブルエンジンを使用することで、スクリプトは複雑な集計など、任意のデータ変換を行うことができます。
:::

## 構文

`executable` テーブル関数は3つのパラメータを要求し、入力クエリのオプションリストを受け付けます:

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

- `script_name`: スクリプトのファイル名。`user_scripts` フォルダに保存されます（`user_scripts_path` 設定のデフォルトフォルダ）
- `format`: 生成されるテーブルのフォーマット
- `structure`: 生成されるテーブルのスキーマ
- `input_query`: スクリプトに **stdin** 経由で結果が渡されるオプションのクエリ（または集合、クエリ）

:::note
同じ入力クエリで同じスクリプトを繰り返し呼び出す予定がある場合、[`Executable` テーブルエンジン](../../engines/table-engines/special/executable.md)を使用することを検討してください。
:::

以下の Python スクリプトは `generate_random.py` という名前で、`user_scripts` フォルダに保存されています。このスクリプトは数値 `i` を読み取り、タブで区切られた番号が前につく `i` 個のランダムな文字列を出力します。

```python
#!/usr/local/bin/python3.9

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

このスクリプトを呼び出し、ランダム文字列を10個生成してみましょう。

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

レスポンスは次のようになります。

```response
┌─id─┬─random─────┐
│  0 │ xheXXCiSkH │
│  1 │ AqxvHAoTrl │
│  2 │ JYvPCEbIkY │
│  3 │ sWgnqJwGRm │
│  4 │ fTZGrjcLon │
│  5 │ ZQINGktPnd │
│  6 │ YFSvGGoezb │
│  7 │ QyMJJZOOia │
│  8 │ NfiyDDhmcI │
│  9 │ REJRdJpWrg │
└────┴────────────┘
```

## 設定

- `send_chunk_header` - データ処理のチャンクを送信する前に行数を送信するかどうかを制御します。デフォルト値は `false` です。
- `pool_size` — プールのサイズ。`pool_size` に `0` が指定された場合、プールサイズの制限はありません。デフォルト値は `16` です。
- `max_command_execution_time` — データブロックを処理するための実行可能スクリプトコマンドの最大実行時間。秒単位で指定。デフォルト値は 10。
- `command_termination_timeout` — 実行可能スクリプトはメインの読み書きループを含んでいる必要があります。テーブル関数が破棄されるとパイプが閉じ、実行可能ファイルは終了するまで `command_termination_timeout` 秒を持ち、ClickHouse が子プロセスに SIGTERM シグナルを送る前に終了します。秒単位で指定。デフォルト値は 10。
- `command_read_timeout` - コマンドの stdout からデータを読み取るタイムアウトをミリ秒で指定。デフォルト値は 10000。
- `command_write_timeout` - コマンドの stdin へデータを書き込むタイムアウトをミリ秒で指定。デフォルト値は 10000。

## クエリ結果をスクリプトに渡す

クエリ結果をスクリプトに渡す方法については、[`Executable` テーブルエンジン](../../engines/table-engines/special/executable.md#passing-query-results-to-a-script)の例も参照してください。この例のスクリプトを`executable` テーブル関数で実行する方法は次の通りです：

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```
