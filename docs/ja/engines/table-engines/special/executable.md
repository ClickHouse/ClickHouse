---
slug: /ja/engines/table-engines/special/executable
sidebar_position: 40
sidebar_label:  Executable
---

# Executable および ExecutablePool テーブルエンジン

`Executable` および `ExecutablePool` テーブルエンジンを使用すると、**stdout** に行を書き込むことによって定義したスクリプトから行を生成するテーブルを定義できます。実行スクリプトは `users_scripts` ディレクトリに保存され、任意のソースからデータを読み取ることができます。

- `Executable` テーブル: スクリプトは各クエリで実行されます
- `ExecutablePool` テーブル: プール内で永続的なプロセスを維持し、読み取りのためにプールからプロセスを取得します

任意で 1 つ以上の入力クエリを含め、それらの結果を **stdin** にストリームしてスクリプトが読み取れるようにすることができます。

## Executable テーブルの作成

`Executable` テーブルエンジンは、スクリプトの名前と受信データのフォーマットの 2 つのパラメータを必要とします。任意で 1 つ以上の入力クエリを渡すことができます:

```sql
Executable(script_name, format, [input_query...])
```

以下は、`Executable` テーブルに関する設定です：

- `send_chunk_header`
    - 説明: チャンク処理の前に各チャンクの行数を送信します。この設定によってリソースの一部を事前に割り当てる脚本を書くのに役立ちます
    - デフォルト値: false
- `command_termination_timeout`
    - 説明: コマンド終了タイムアウト（秒単位）
    - デフォルト値: 10
- `command_read_timeout`
    - 説明: コマンド stdout からデータを読み取るためのタイムアウト（ミリ秒単位）
    - デフォルト値: 10000
- `command_write_timeout`
    - 説明: コマンド stdin にデータを書き込むためのタイムアウト（ミリ秒単位）
    - デフォルト値: 10000

例を見てみましょう。この Python スクリプトは `my_script.py` と名付けられ、`user_scripts` フォルダに保存されています。それは数値 `i` を読み取り、各文字列がタブで区切られた番号とともに、`i` 個のランダムな文字列を出力します：

```python
#!/usr/bin/python3

import sys
import string
import random

def main():

    # 入力値を読み取る
    for number in sys.stdin:
        i = int(number)

        # ランダムな行を生成する
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # 結果を stdout にフラッシュする
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

次の `my_executable_table` は `my_script.py` の出力から構築されており、`my_executable_table` から `SELECT` を実行するたびに 10 個のランダムな文字列を生成します：

```sql
CREATE TABLE my_executable_table (
   x UInt32,
   y String
)
ENGINE = Executable('my_script.py', TabSeparated, (SELECT 10))
```

テーブルを作成しても即座にスクリプトを実行することはなく、`my_executable_table` をクエリするとスクリプトが実行されます：

```sql
SELECT * FROM my_executable_table
```

```response
┌─x─┬─y──────────┐
│ 0 │ BsnKBsNGNH │
│ 1 │ mgHfBCUrWM │
│ 2 │ iDQAVhlygr │
│ 3 │ uNGwDuXyCk │
│ 4 │ GcFdQWvoLB │
│ 5 │ UkciuuOTVO │
│ 6 │ HoKeCdHkbs │
│ 7 │ xRvySxqAcR │
│ 8 │ LKbXPHpyDI │
│ 9 │ zxogHTzEVV │
└───┴────────────┘
```

## クエリ結果をスクリプトに渡す

Hacker News のユーザーはコメントを残します。Python には自然言語処理のツールキット（`nltk`）があり、コメントがポジティブ、ネガティブ、あるいは中立であるかを判定する `SentimentIntensityAnalyzer` が存在します。Hacker News のコメントの感情を `nltk` を使用して計算する `Executable` テーブルを作成してみましょう。

この例では、[このページ](https://clickhouse.com/docs/ja/engines/table-engines/mergetree-family/invertedindexes/#full-text-search-of-the-hacker-news-dataset)で説明されている `hackernews` テーブルを使用します。`hackernews` テーブルには `UInt64` タイプの `id` カラムと `comment` という名前の `String` カラムがあります。まずは `Executable` テーブルを定義してみましょう：

```sql
CREATE TABLE sentiment (
   id UInt64,
   sentiment Float32
)
ENGINE = Executable(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```

`sentiment` テーブルについていくつかのコメント：

- ファイル `sentiment.py` は `user_scripts` フォルダに保存されています（`user_scripts_path` 設定のデフォルトフォルダ）
- `TabSeparated` フォーマットは、Python スクリプトがタブ区切りの値を含む生データ行を生成する必要があることを意味します
- クエリは `hackernews` から 2 つのカラムを選択しています。Python スクリプトはそれらのカラム値を入力行から解析する必要があります

`sentiment.py` の定義はこちらです：

```python
#!/usr/local/bin/python3.9

import sys
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

def main():
    sentiment_analyzer = SentimentIntensityAnalyzer()

    while True:
        try:
            row = sys.stdin.readline()
            if row == '':
                break

            split_line = row.split("\t")

            id = str(split_line[0])
            comment = split_line[1]

            score = sentiment_analyzer.polarity_scores(comment)['compound']
            print(id + '\t' + str(score) + '\n', end='')
            sys.stdout.flush()
        except BaseException as x:
            break

if __name__ == "__main__":
    main()
```

Python スクリプトに関するコメント：

- 機能させるためには `nltk.downloader.download('vader_lexicon')` を実行する必要があります。これはスクリプト内に配置することも可能ですが、そうすると `sentiment` テーブルの各クエリ実行ごとにダウンロードされるため非効率です
- `row` の各値は `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20` の結果セット内の行です
- 入力行はタブ区切りなので、Python の `split` 関数を使って `id` と `comment` を解析します
- `polarity_scores` の結果は JSON オブジェクトで、いくつかの値を持ちます。ここではその JSON オブジェクトの `compound` 値だけを取得することにしました
- ClickHouse の `sentiment` テーブルは `TabSeparated` フォーマットを使用し、2 つのカラムを含むので、`print` 関数でそれらのカラムをタブで区切っています

`sentiment` テーブルから行を選択するクエリを書くたびに、`SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20` クエリが実行され、その結果が `sentiment.py` に渡されます。試してみましょう：

```sql
SELECT *
FROM sentiment
```

レスポンスは次のようになります：

```response
┌───────id─┬─sentiment─┐
│  7398199 │    0.4404 │
│ 21640317 │    0.1779 │
│ 21462000 │         0 │
│ 25168863 │         0 │
│ 25168978 │   -0.1531 │
│ 25169359 │         0 │
│ 25169394 │   -0.9231 │
│ 25169766 │    0.4137 │
│ 25172570 │    0.7469 │
│ 25173687 │    0.6249 │
│ 28291534 │         0 │
│ 28291669 │   -0.4767 │
│ 28291731 │         0 │
│ 28291949 │   -0.4767 │
│ 28292004 │    0.3612 │
│ 28292050 │    -0.296 │
│ 28292322 │         0 │
│ 28295172 │    0.7717 │
│ 28295288 │    0.4404 │
│ 21465723 │   -0.6956 │
└──────────┴───────────┘
```

## ExecutablePool テーブルの作成

`ExecutablePool` の文法は `Executable` と似ていますが、`ExecutablePool` テーブルにはいくつかの特有の設定があります：

- `pool_size`
    - 説明: プロセスプールのサイズ。サイズが0の場合、サイズ制限はありません
    - デフォルト値: 16
- `max_command_execution_time`
    - 説明: コマンドの最大実行時間（秒単位）
    - デフォルト値: 10

上記の `sentiment` テーブルを `Executable` ではなく `ExecutablePool` を使用するように簡単に変換できます：

```sql
CREATE TABLE sentiment_pooled (
   id UInt64,
   sentiment Float32
)
ENGINE = ExecutablePool(
	'sentiment.py',
	TabSeparated,
	(SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20000)
)
SETTINGS
	pool_size = 4;
```

クライアントが `sentiment_pooled` テーブルをクエリすると、ClickHouse はオンデマンドで 4 つのプロセスを維持します。
