---
title: clickhouse-localデータベースの使用方法
sidebar_label: clickhouse-localデータベースの使用方法
slug: /ja/chdb/guides/clickhouse-local
description: chDBでclickhouse-localデータベースを使用する方法を学びます。
keywords: [chdb, clickhouse-local]
---

[clickhouse-local](/ja/operations/utilities/clickhouse-local) は、ClickHouseの埋め込みバージョンを使用するCLIであり、サーバーをインストールすることなくClickHouseの機能を利用できるようにします。このガイドでは、chDBからclickhouse-localデータベースを使用する方法を学びます。

## セットアップ

最初に仮想環境を作成しましょう：

```bash
python -m venv .venv
source .venv/bin/activate
```

次に、chDBをインストールします。
バージョン2.0.2以上であることを確認してください：

```bash
pip install "chdb>=2.0.2"
```

続いて[ipython](https://ipython.org/)をインストールします：

```bash
pip install ipython
```

このガイドの残りのコマンドを実行するために`ipython`を使用します。以下のコマンドで起動できます：

```bash
ipython
```

## clickhouse-localのインストール

clickhouse-localのダウンロードとインストールは、[ClickHouseのダウンロードとインストール](https://clickhouse.com/docs/ja/install)と同じです。以下のコマンドを実行します：

```bash
curl https://clickhouse.com/ | sh
```

データをディレクトリに保存してclickhouse-localを起動するには、`--path`を指定する必要があります：

```bash
./clickhouse -m --path demo.chdb
```

## clickhouse-localにデータを取り込む

デフォルトのデータベースはメモリにデータを保存するだけなので、ディスクにデータを保持するには名前付きデータベースを作成する必要があります。

```sql
CREATE DATABASE foo;
```

次にテーブルを作成し、ランダムな数値を挿入します：

```sql
CREATE TABLE foo.randomNumbers
ORDER BY number AS
SELECT rand() AS number
FROM numbers(10_000_000);
```

データを確認するためにクエリを実行してみましょう：

```sql
SELECT quantilesExact(0, 0.5, 0.75, 0.99)(number) AS quants
FROM foo.randomNumbers

┌─quants────────────────────────────────┐
│ [69,2147776478,3221525118,4252096960] │
└───────────────────────────────────────┘
```

その後、CLIから`exit;`することを忘れないでください。ディレクトリに対するロックを保持できるのは一つのプロセスだけです。この操作を怠ると、chDBからデータベースに接続しようとしたときに次のエラーが発生します：

```text
ChdbError: Code: 76. DB::Exception: Cannot lock file demo.chdb/status. Another server instance in same directory is already running. (CANNOT_OPEN_FILE)
```

## clickhouse-localデータベースへの接続

`ipython`シェルに戻り、chDBから`session`モジュールをインポートします：

```python
from chdb import session as chs
```

`demo.chdb`を指すセッションを初期化します：

```
sess = chs.Session("demo.chdb")
```

次に、数値の分位を返す同じクエリを実行できます：

```python
sess.query("""
SELECT quantilesExact(0, 0.5, 0.75, 0.99)(number) AS quants
FROM foo.randomNumbers
""", "Vertical")

Row 1:
──────
quants: [0,9976599,2147776478,4209286886]
```

chDBからこのデータベースにデータを挿入することもできます：

```python
sess.query("""
INSERT INTO foo.randomNumbers
SELECT rand() AS number FROM numbers(10_000_000)
""")

Row 1:
──────
quants: [0,9976599,2147776478,4209286886]
```

その後、chDBまたはclickhouse-localから分位数クエリを再実行できます。
