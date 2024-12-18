---
sidebar_label: Vector
sidebar_position: 220
slug: /ja/integrations/vector
description: Vectorを使用してログファイルをClickHouseに送信する方法
---

# VectorをClickHouseと統合する

生産アプリケーションにおけるリアルタイムなログ分析は非常に重要です。ClickHouseがログデータの保存と分析に優れているかどうか考えたことはありませんか？ClickHouseにログインフラストラクチャを移行した<a href="https://eng.uber.com/logging/" target="_blank">Uberの経験</a>をぜひご確認ください。

このガイドでは、人気のデータパイプライン<a href="https://vector.dev/docs/about/what-is-vector/" target="_blank">Vector</a>を使用して、Nginxログファイルを監視し、それをClickHouseに送信する方法を説明します。以下の手順は、任意のタイプのログファイルを監視する際にも同様です。ClickHouseが稼働しており、Vectorがインストールされていることを前提とします（ただし、まだ起動する必要はありません）。

## 1. データベースとテーブルを作成する

ログイベントを保存するためのテーブルを定義しましょう。

1. 新しいデータベース**nginxdb**を作成します:
    ```sql
    CREATE DATABASE IF NOT EXISTS nginxdb
    ```

2. 初めに、ログイベント全体を1つの文字列として挿入します。これは分析するには最適な形式ではありませんが、***Materialized View***を使用してこの部分を以下で改善します。
    ```sql
    CREATE TABLE IF NOT EXISTS  nginxdb.access_logs (
        message String
    )
    ENGINE = MergeTree()
    ORDER BY tuple()
    ```
    :::note
    **主キー**はまだ必要ないため、**ORDER BY**は**tuple()**に設定しています。
    :::

## 2. Nginxを設定する

Nginxについて詳しく説明することは避けたいところですが、詳細を省略したくもないので、Nginxのログ設定に必要な詳細を提供します。

1. 以下の`access_log`プロパティは、**combined**形式でログを**/var/log/nginx/my_access.log**に送信します。この設定は**nginx.conf**ファイルの`http`セクションに入力します:
    ```bash
    http {
        include       /etc/nginx/mime.types;
        default_type  application/octet-stream;
        access_log  /var/log/nginx/my_access.log combined;
        sendfile        on;
        keepalive_timeout  65;
        include /etc/nginx/conf.d/*.conf;
    }
    ```

2. **nginx.conf**を修正した場合は、Nginxを再起動してください。

3. ウェブサーバーでページを訪問して、アクセスログにいくつかのログイベントを生成します。**combined**形式のログは次の形式を持ちます:
    ```bash
    192.168.208.1 - - [12/Oct/2021:03:31:44 +0000] "GET / HTTP/1.1" 200 615 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    192.168.208.1 - - [12/Oct/2021:03:31:44 +0000] "GET /favicon.ico HTTP/1.1" 404 555 "http://localhost/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    192.168.208.1 - - [12/Oct/2021:03:31:49 +0000] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    ```

## 3. Vectorを設定する

Vectorは、ログ、メトリクス、トレース（**ソース**と呼ばれます）を収集、変換、およびルーティングし、多くの異なるベンダー（**シンク**と呼ばれます）に送信します。ClickHouseとの即時対応性も組み込まれています。ソースとシンクは、**vector.toml**という名前の設定ファイルで定義されます。

1. 以下の**vector.toml**は、**my_access.log**の末尾を監視するタイプが**file**の**source**を定義し、上記で定義された**access_logs**テーブルを**sink**としても定義しています:
    ```bash
    [sources.nginx_logs]
    type = "file"
    include = [ "/var/log/nginx/my_access.log" ]
    read_from = "end"

    [sinks.clickhouse]
    type = "clickhouse"
    inputs = ["nginx_logs"]
    endpoint = "http://clickhouse-server:8123"
    database = "nginxdb"
    table = "access_logs"
    skip_unknown_fields = true
    ```

2. 上記の設定を使用してVectorを起動します。ソースとシンクの定義についてさらに詳しい情報を知りたい場合は、<a href="https://vector.dev/docs/" target="_blank">Vectorのドキュメント</a>をご覧ください。

3. アクセスログがClickHouseに挿入されていることを確認します。以下のクエリを実行し、テーブルにアクセスログが表示されるはずです:
    ```sql
    SELECT * FROM nginxdb.access_logs
    ```
    <img src={require('./images/vector_01.png').default} class="image" alt="ログを表示" />

## 4. ログを解析する

ClickHouseにログが保存されているのは素晴らしいことですが、各イベントを単独の文字列として保存していると、データの分析があまりできません。Materialized Viewを利用してログイベントを解析する方法を見てみましょう。

1. **Materialized View**（MVと略します）は、既存のテーブルに基づいた新しいテーブルで、既存のテーブルに挿入されたデータがMaterialized Viewにも自動的に反映されます。**access_logs**内のログイベントの解析された表現を含むMVを定義する方法を見てみましょう。次のように表現します:
    ```bash
    192.168.208.1 - - [12/Oct/2021:15:32:43 +0000] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
    ```

    ClickHouseには文字列を解析するためのさまざまな関数が用意されていますが、まずは**splitByWhitespace**を見てみましょう。これは、空白によって文字列を解析し、各トークンを配列で返します。次のコマンドを実行してみてください:
    ```sql
    SELECT splitByWhitespace('192.168.208.1 - - [12/Oct/2021:15:32:43 +0000] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"')
    ```

    レスポンスがかなり理想に近いことに気づくでしょう！一部の文字列には余分な文字が含まれており、ユーザーエージェント（ブラウザの詳細）は解析されていませんが、それは次のステップで解決します:
    ```
    ["192.168.208.1","-","-","[12/Oct/2021:15:32:43","+0000]","\"GET","/","HTTP/1.1\"","304","0","\"-\"","\"Mozilla/5.0","(Macintosh;","Intel","Mac","OS","X","10_15_7)","AppleWebKit/537.36","(KHTML,","like","Gecko)","Chrome/93.0.4577.63","Safari/537.36\""]
    ```

2. **splitByWhitespace**に似て、**splitByRegexp**関数は正規表現に基づいて文字列を配列に分割します。以下のコマンドを実行してみてください。2つの文字列が返されます。
    ```sql
    SELECT splitByRegexp('\S \d+ "([^"]*)"', '192.168.208.1 - - [12/Oct/2021:15:32:43 +0000] "GET / HTTP/1.1" 304 0 "-" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"')
    ```

    ログから正常に解析されたユーザーエージェントが2番目の文字列として返されます:
    ```
    ["192.168.208.1 - - [12/Oct/2021:15:32:43 +0000] \"GET / HTTP/1.1\" 30"," \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36\""]
    ```

3. 最終的な**CREATE MATERIALIZED VIEW**コマンドを見る前に、データをクリーンアップするために使用されるいくつかの関数を見てみましょう。例えば、**RequestMethod**は不要なダブルクォート付きの**"GET**となっています。以下の**trim**関数を実行してください。これによりダブルクォートが削除されます:
    ```sql
    SELECT trim(LEADING '"' FROM '"GET')
    ```

4. 時間文字列には先頭に角括弧が付いており、ClickHouseが日付に変換できる形式にはなっていません。しかし、コロン(**:**)をカンマ(**,**)に変更すると、きちんと解析されます:
    ```sql
    SELECT parseDateTimeBestEffort(replaceOne(trim(LEADING '[' FROM '[12/Oct/2021:15:32:43'), ':', ' '))
    ```

5. これでMaterialized Viewを定義する準備が整いました。定義には**POPULATE**が含まれており、既存の**access_logs**内の行がすぐに処理され挿入されます。以下のSQL文を実行してください:
    ```sql
    CREATE MATERIALIZED VIEW nginxdb.access_logs_view
    (
        RemoteAddr String,
        Client String,
        RemoteUser String,
        TimeLocal DateTime,
        RequestMethod String,
        Request String,
        HttpVersion String,
        Status Int32,
        BytesSent Int64,
        UserAgent String
    )
    ENGINE = MergeTree()
    ORDER BY RemoteAddr
    POPULATE AS
    WITH
        splitByWhitespace(message) as split,
        splitByRegexp('\S \d+ "([^"]*)"', message) as referer
    SELECT
        split[1] AS RemoteAddr,
        split[2] AS Client,
        split[3] AS RemoteUser,
        parseDateTimeBestEffort(replaceOne(trim(LEADING '[' FROM split[4]), ':', ' ')) AS TimeLocal,
        trim(LEADING '"' FROM split[6]) AS RequestMethod,
        split[7] AS Request,
        trim(TRAILING '"' FROM split[8]) AS HttpVersion,
        split[9] AS Status,
        split[10] AS BytesSent,
        trim(BOTH '"' from referer[2]) AS UserAgent
    FROM
        (SELECT message FROM nginxdb.access_logs)
    ```

6. これがうまくいったかどうか確認しましょう。アクセスログがきちんとカラムに解析されているはずです:
    ```sql
    SELECT * FROM nginxdb.access_logs_view
    ```
    <img src={require('./images/vector_02.png').default} class="image" alt="ログを表示" />

    :::note
    上記のレッスンではデータを2つのテーブルに保存しましたが、最初の**nginxdb.access_logs**テーブルを**Null**テーブルエンジンに変更することで、解析されたデータは**nginxdb.access_logs_view**テーブルに入りますが、生のデータはテーブルに保存されません。
    :::

**まとめ:** 簡単なインストールと迅速な設定だけでVectorを使用すると、NginxサーバーからClickHouse内のテーブルにログを送信できます。巧妙なMaterialized Viewを使用することで、これらのログをカラムに解析し、分析を容易に行うことができます。

## 関連コンテンツ

- ブログ: [Building an Observability Solution with ClickHouse in 2023 - Part 1 - Logs](https://clickhouse.com/blog/storing-log-data-in-clickhouse-fluent-bit-vector-open-telemetry)
- ブログ: [Sending Nginx logs to ClickHouse with Fluent Bit](https://clickhouse.com/blog/nginx-logs-to-clickhouse-fluent-bit)
- ブログ: [Sending Kubernetes logs To ClickHouse with Fluent Bit](https://clickhouse.com/blog/kubernetes-logs-to-clickhouse-fluent-bit)
