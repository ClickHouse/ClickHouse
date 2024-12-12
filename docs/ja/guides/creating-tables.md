---
sidebar_position: 1
sidebar_label: テーブルの作成
---

# ClickHouseにおけるテーブルの作成

ほとんどのデータベースと同様に、ClickHouseはテーブルを**データベース**に論理的にグループ化します。ClickHouseで新しいデータベースを作成するには、`CREATE DATABASE`コマンドを使用します。

```sql
CREATE DATABASE IF NOT EXISTS helloworld
```

同様に、`CREATE TABLE`を使用して新しいテーブルを定義します。（データベース名を指定しない場合、テーブルは`default`データベースに作成されます。）次のテーブルは`helloworld`データベース内に`my_first_table`という名前で作成されます。

```sql
CREATE TABLE helloworld.my_first_table
(
    user_id UInt32,
    message String,
    timestamp DateTime,
    metric Float32
)
ENGINE = MergeTree()
PRIMARY KEY (user_id, timestamp)
```

上記の例では、`my_first_table`は四つのカラムを持つ`MergeTree`テーブルです。

- `user_id`:  32ビットの符号なし整数
- `message`: `String`データ型で、他のデータベースシステムの`VARCHAR`、`BLOB`、`CLOB`などの型の代替
- `timestamp`: 時間の瞬間を表す`DateTime`値
- `metric`: 32ビットの浮動小数点数

:::note
テーブルエンジンは以下を決定します。
- データの保存方法と保存場所
- サポートされているクエリ
- データがレプリケートされるかどうか

選択できるエンジンは多くありますが、単一ノードのClickHouseサーバーでの単純なテーブルには、[MergeTree](/ja/engines/table-engines/mergetree-family/mergetree.md)が適しています。
:::

## 主キーの簡単な紹介

先に進む前に、ClickHouseにおける主キーの働きを理解することが重要です（主キーの実装は意外に感じるかもしれません！）。

- ClickHouseでは、主キーは各行に対して**一意ではありません**

ClickHouseテーブルの主キーは、データがディスクに書き込まれるときのソート方法を決定します。8,192行または10MBのデータごと（**インデックスの粒度**と呼ばれる）に主キーインデックスファイルにエントリが作成されます。この粒度の概念により、メモリに簡単に収まる**スパースインデックス**が作成され、グラニュールは`SELECT`クエリの処理中に最小のカラムデータ量のストライプを表します。

主キーは`PRIMARY KEY`パラメータを使用して定義できます。`PRIMARY KEY`を指定せずにテーブルを定義すると、キーは`ORDER BY`句で指定されたタプルになります。`PRIMARY KEY`と`ORDER BY`の両方を指定した場合、主キーはソート順序の接頭辞でなければなりません。

主キーはまたソートキーでもあり、`(user_id, timestamp)`のタプルです。 したがって、各カラムファイルに保存されるデータは、`user_id`、次に`timestamp`の順にソートされます。

:::tip
詳細については、ClickHouse Academyの[データモデリングトレーニングモジュール](https://learn.clickhouse.com/visitor_catalog_class/show/1328860/?utm_source=clickhouse&utm_medium=docs)をチェックしてください。
:::
