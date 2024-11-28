---
sidebar_position: 1
slug: /ja/cloud-quick-start
sidebar_label: Cloudクイックスタート
keywords: [clickhouse, install, getting started, quick start]
pagination_next: en/get-started/sql-console
---
import SignUp from '@site/docs/ja/_snippets/_sign_in_or_trial.md';
import SQLConsoleDetail from '@site/docs/ja/_snippets/_launch_sql_console.md';
import CheckIPAccess from '@site/docs/ja/_snippets/_check_ip_access_list_detail.md';

# ClickHouse Cloud クイックスタート

ClickHouse をすぐにセットアップして利用を開始する最速かつ最も簡単な方法は、[ClickHouse Cloud](https://clickhouse.cloud)で新しいサービスを作成することです。

## 1: ClickHouse を入手する

[ClickHouse Cloud](https://clickhouse.cloud)で無料の ClickHouse サービスを作成するには、次のステップを完了するだけでサインアップできます：

  - [サインアップページ](https://clickhouse.cloud/signUp)でアカウントを作成
  - 受信したメール内のリンクをクリックしてメールアドレスを確認
  - 作成したユーザー名とパスワードでログイン

ログインすると、ClickHouse Cloud のオンボーディングウィザードが開始され、新しい ClickHouse サービスの作成をガイドしてくれます。サービスの展開先のリージョンを選択し、新しいサービスに名前を付けます：

<div class="eighty-percent">

![New ClickHouse Service](@site/docs/ja/_snippets/images/createservice1.png)
</div>

<br/>

ClickHouse Cloud は IP フィルタリングを使用してサービスへのアクセスを制限します。ローカル IP アドレスが既に追加されていることに注意してください。サービスが起動してから追加することもできます。

<div class="eighty-percent">

![IP Filtering](@site/docs/ja/_snippets/images/createservice2.png)
</div>

<br/>

ClickHouse Cloud は `default` ユーザーのためにパスワードを生成します。資格情報を必ず保存してください。（後で変更可能です。）

<div class="eighty-percent">

![Download Credentials](@site/docs/ja/_snippets/images/createservice3.png)
</div>

新しいサービスがプロビジョニングされ、ClickHouse Cloud ダッシュボードに表示されるはずです：

<div class="eighty-percent">

![Download Credentials](@site/docs/ja/_snippets/images/createservice4.png)
</div>

<br/>

おめでとうございます！ClickHouse Cloud サービスが起動しました。接続方法やデータのインジェストを開始する方法について、引き続きお読みください。

## 2: ClickHouse に接続

素早く始めるために、ClickHouse ではウェブベースの SQL コンソールが提供されています。

<SQLConsoleDetail />

:::note
ClickHouse はデータのセキュリティを非常に重視しているため、サービス作成時に IP アクセスリストの設定が求められました。これをスキップしたり誤って閉じたりした場合、サービスに接続できません。

ローカル IP アドレスの追加方法については、[IP アクセスリスト](/docs/ja/cloud/security/setting-ip-filters)のドキュメントページを参照してください。
:::

1. 接続が正常に動作するか確認するために、簡単なクエリを入力しましょう：

  ```sql
  SHOW databases
  ```

   リストには 4 つのデータベースが表示され、追加したものがあればそれも含まれます。

  これで、新しい ClickHouse サービスを使用する準備が整いました！

## 3: データベースとテーブルを作成

1. ほとんどのデータベース管理システムと同様に、ClickHouse はテーブルを論理的に**データベース**にグループ化します。新しいデータベースを ClickHouse に作成するには `CREATE DATABASE` コマンドを使用します：
  ```sql
  CREATE DATABASE IF NOT EXISTS helloworld
  ```

1. `helloworld` データベースに `my_first_table` という名前のテーブルを作成するには次のコマンドを実行します：
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

  上記の例では、`my_first_table` は4つのカラムを持つ MergeTree テーブルです：

    - `user_id`:  32ビットの符号なし整数
    - `message`: 他のデータベースシステムでの VARCHAR, BLOB, CLOB などに置き換わる String データ型
    - `timestamp`: 時間を表す DateTime 値
    - `metric`: 32ビットの浮動小数点数

  :::note テーブルエンジン
  テーブルエンジンは以下を決定します：
   - データがどのように、どこに保存されるか
   - どのクエリがサポートされるか
   - データがレプリケーションされるかどうか

  選択可能なエンジンは多数ありますが、単一ノードの ClickHouse サーバー上のシンプルなテーブルには [MergeTree](/ja/engines/table-engines/mergetree-family/mergetree.md) が一般的な選択です。
  :::

  ### 主キーの簡単な紹介

  先に進む前に、ClickHouse における主キーの働きについて理解することが重要です（主キーの実装が予想外に思えるかもしれません！）：

    - ClickHouse の主キーはテーブルの各行に対して**一意ではありません**

  ClickHouse テーブルの主キーはデータがディスクに書き込まれる際の並び順を決定します。8,192 行または 10MB のデータごとに（**インデックス粒度**として参照される）主キーインデックスファイルにエントリが作成されます。この粒度の概念により、**スパースインデックス**がメモリに簡単に適合し、粒度は `SELECT` クエリ処理時に処理される最小のカラムデータのストライプを表します。

  主キーは `PRIMARY KEY` パラメータを使用して定義できます。`PRIMARY KEY`を指定しないでテーブルを定義すると、キーは `ORDER BY` 句に指定されたタプルになります。`PRIMARY KEY` と `ORDER BY` の両方を指定した場合、主キーはソート順のサブセットでなければなりません。

  主キーはまたソートキーであり `(user_id, timestamp)` のタプルです。したがって、各カラムファイルに格納されるデータは `user_id`、次に `timestamp` の順にソートされます。

## 4: データを挿入

ClickHouse ではおなじみの `INSERT INTO TABLE` コマンドを使用できますが、`MergeTree` テーブルへの各挿入がストレージに**パート**を作成することを理解することが重要です。

:::tip ClickHouse ベストプラクティス
バッチごとに大量（数万または数百万）の行を挿入してください。心配いりません - ClickHouse はそうしたボリュームを容易に処理でき、それが [コスト削減](/docs/ja/cloud/bestpractices/bulkinserts.md) にも繋がります。
:::

1. 簡単な例であっても、複数の行を同時に挿入しましょう：
  ```sql
  INSERT INTO helloworld.my_first_table (user_id, message, timestamp, metric) VALUES
      (101, 'Hello, ClickHouse!',                                 now(),       -1.0    ),
      (102, 'Insert a lot of rows per batch',                     yesterday(), 1.41421 ),
      (102, 'Sort your data based on your commonly-used queries', today(),     2.718   ),
      (101, 'Granules are the smallest chunks of data read',      now() + 5,   3.14159 )
  ```

  :::note
  `timestamp` カラムがさまざまな **Date** および **DateTime** 関数を使用して埋められていることに注意してください。ClickHouse には多くの便利な関数があります。詳細は[**関数**セクション](/docs/ja/sql-reference/functions/index.md)を参照してください。
  :::

1. 挿入が成功したか確認しましょう：
  ```sql
  SELECT * FROM helloworld.my_first_table
  ```
  挿入された4つの行が表示されるはずです：

## 5: ClickHouse クライアントを使用する

コマンドラインツール **clickhouse client** を使用して ClickHouse Cloud サービスに接続することもできます。接続詳細はサービスの **Native** タブに記載されています：

  ![clickhouse client connection details](@site/docs/ja/images/quickstart/CloudClickhouseClientDetails.png)

1. [ClickHouse](/docs/ja/integrations/clickhouse-client-local.md) をインストールします。

2. ホスト名、ユーザー名、パスワードを差し替えてコマンドを実行します：
  ```bash
  ./clickhouse client --host HOSTNAME.REGION.CSP.clickhouse.cloud \
  --secure --port 9440 \
  --user default \
  --password <password>
  ```
  スマイリーフェイスのプロンプトが表示されたら、クエリを実行する準備が整いました！
  ```response
  :)
  ```

3. 次のクエリを実行してみましょう：
  ```sql
  SELECT *
  FROM helloworld.my_first_table
  ORDER BY timestamp
  ```
  応答が整ったテーブル形式で返ってくることに注目してください：
   ```response
   ┌─user_id─┬─message────────────────────────────────────────────┬───────────timestamp─┬──metric─┐
   │     102 │ Insert a lot of rows per batch                     │ 2022-03-21 00:00:00 │ 1.41421 │
   │     102 │ Sort your data based on your commonly-used queries │ 2022-03-22 00:00:00 │   2.718 │
   │     101 │ Hello, ClickHouse!                                 │ 2022-03-22 14:04:09 │      -1 │
   │     101 │ Granules are the smallest chunks of data read      │ 2022-03-22 14:04:14 │ 3.14159 │
   └─────────┴────────────────────────────────────────────────────┴─────────────────────┴─────────┘

   4 rows in set. Elapsed: 0.008 sec.
   ```

5. `FORMAT`句を追加してその一つを指定すべき [ClickHouse の多くのサポートされている出力形式](/ja/interfaces/formats/) のいずれかを指定してください：
  ```sql
  SELECT *
  FROM helloworld.my_first_table
  ORDER BY timestamp
  FORMAT TabSeparated
  ```
  上記のクエリでは、出力はタブ区切りで返されます：
  ```response
  Query id: 3604df1c-acfd-4117-9c56-f86c69721121

  102 Insert a lot of rows per batch	2022-03-21 00:00:00	1.41421
  102 Sort your data based on your commonly-used queries	2022-03-22 00:00:00	2.718
  101 Hello, ClickHouse!	2022-03-22 14:04:09	-1
  101 Granules are the smallest chunks of data read	2022-03-22 14:04:14	3.14159

  4 rows in set. Elapsed: 0.005 sec.
  ```

6. `clickhouse client` を終了するには、**exit** コマンドを入力します：
  ```bash
  exit
  ```

## 6: CSV ファイルを挿入する

データベースを始める際に一般的なタスクは、既にファイルにあるデータを挿入することです。ユーザーID、訪問した URL、イベントのタイムスタンプを含むクリックストリームデータを表すサンプルデータをオンラインで提供しています。

`data.csv` という名前の CSV ファイルに次のテキストがあるとします：

  ```bash
  102,This is data in a file,2022-02-22 10:43:28,123.45
  101,It is comma-separated,2022-02-23 00:00:00,456.78
  103,Use FORMAT to specify the format,2022-02-21 10:43:30,678.90
  ```

1. 次のコマンドは `my_first_table` にデータを挿入します：
  ```bash
  ./clickhouse client --host HOSTNAME.REGION.CSP.clickhouse.cloud \
  --secure --port 9440 \
  --user default \
  --password <password> \
  --query='INSERT INTO helloworld.my_first_table FORMAT CSV' < data.csv
  ```

2. テーブルに新しい行が表示されることに注目してください：

  ![New rows from CSV file](@site/docs/ja/images/quickstart_04.png)

## 次は何をすべきか？

- [チュートリアル](/docs/ja/tutorial.md) では200万行のデータをテーブルに挿入し、分析クエリを書く体験を提供します
- [例データセット](/docs/ja/getting-started/index.md) のリストと、それらを挿入する手順があります
- [ClickHouse の始め方](https://clickhouse.com/company/events/getting-started-with-clickhouse/)に関する25分のビデオをご覧ください
- 外部ソースからデータを取得する場合、メッセージキュー、データベース、パイプラインなどとの接続方法についての[統合ガイドのコレクション](/docs/ja/integrations/index.mdx)を参照してください
- UI/BI 可視化ツールを使用している場合、[UI を ClickHouse に接続するためのユーザーガイド](/docs/ja/integrations/data-visualization.md) を参照してください
- 主キーに関するすべてのことを知るためには、[主キーに関するユーザーガイド](/docs/ja/guides/best-practices/sparse-primary-indexes.md)を参照してください
