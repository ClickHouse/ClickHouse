---
description: '`executable` テーブル関数は、**stdout** に行を出力するスクリプト内で定義したユーザー定義関数 (UDF) の出力に基づいてテーブルを作成します。'
keywords: ['udf', 'user defined function', 'ClickHouse', '実行可能', 'テーブル', '関数']
sidebar_label: 'executable'
sidebar_position: 50
slug: /engines/table-functions/executable
title: 'executable'
doc_type: 'リファレンス'
---

`executable` テーブル関数は、**stdout** に行を出力するスクリプト内で定義したユーザー定義関数 (UDF) の出力に基づいてテーブルを作成します。実行可能スクリプトは `users_scripts` ディレクトリに保存され、任意のソースからデータを読み取ることができます。実行可能スクリプトの実行に必要なパッケージが ClickHouse server にすべて揃っていることを確認してください。たとえば、Python スクリプトであれば、必要な Python パッケージが server にインストールされていることを確認してください。

必要に応じて、スクリプトが読み取れるように、その結果を **stdin** にストリーミングする 1 つ以上の入力クエリを含めることもできます。

:::note
通常の UDF 関数と `executable` テーブル関数および `Executable` テーブルエンジンとの大きな違いは、通常の UDF 関数では行数を変更できないことです。たとえば、入力が 100 行であれば、結果も 100 行を返さなければなりません。一方、`executable` テーブル関数または `Executable` テーブルエンジンを使用すると、複雑な集計を含め、スクリプトで任意のデータ変換を行えます。
:::

<div id="syntax">
  ## 構文
</div>

`executable` テーブル関数には3つのパラメータが必要で、オプションで入力クエリのリストも指定できます。

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

* `script_name`: スクリプトのファイル名。`user_scripts` フォルダ (`user_scripts_path` 設定のデフォルトのフォルダ) に保存されます
* `format`: 生成されるテーブルのフォーマット
* `structure`: 生成されるテーブルのスキーマ
* `input_query`: 結果が **stdin** 経由でスクリプトに渡される、省略可能なクエリ (またはコレクション、あるいは複数のクエリ) 

:::note
同じスクリプトを同じ入力クエリで繰り返し呼び出す場合は、[`Executable` テーブルエンジン](../../engines/table-engines/special/executable.md)の使用を検討してください。
:::

以下の Python スクリプトは `generate_random.py` という名前で、`user_scripts` フォルダに保存されます。これは数値 `i` を読み取り、`i` 個のランダムな文字列を出力します。各文字列の前には、タブで区切られた数値が付きます。

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

スクリプトを実行し、10個のランダムな文字列を生成してみましょう：

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

レスポンスは次のようになります：

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

<div id="settings">
  ## 設定
</div>

* `send_chunk_header` - 処理対象のデータ chunk を送信する前に、行数を送信するかどうかを制御します。デフォルト値は `false` です。
* `pool_size` — pool のサイズです。`pool_size` に 0 を指定すると、pool サイズの制限はなくなります。デフォルト値は `16` です。
* `max_command_execution_time` — データ block を処理する実行可能スクリプト コマンドの最大 execution time です。秒単位で指定します。デフォルト値は 10 です。
* `command_termination_timeout` — 実行可能スクリプト には、メインの read-write ループが含まれている必要があります。テーブル関数 が破棄されるとパイプは閉じられ、実行可能 file はシャットダウンまでに `command_termination_timeout` 秒の猶予が与えられます。それを過ぎると、ClickHouse は child process に SIGTERM シグナルを送信します。秒単位で指定します。デフォルト値は 10 です。
* `command_read_timeout` - command の stdout からデータを読み取るための timeout です (ミリ秒単位) 。デフォルト値は 10000 です。
* `command_write_timeout` - command の stdin にデータを書き込むための timeout です (ミリ秒単位) 。デフォルト値は 10000 です。

<div id="passing-query-results-to-a-script">
  ## クエリ結果をスクリプトに渡す
</div>

クエリ結果をスクリプトに渡す方法については、[`Executable` テーブルエンジンの例](../../engines/table-engines/special/executable.md#passing-query-results-to-a-script)も参照してください。以下では、その例と同じスクリプトを `executable` テーブル関数を使って実行する方法を示します。

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```