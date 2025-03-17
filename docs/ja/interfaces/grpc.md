---
slug: /ja/interfaces/grpc
sidebar_position: 19
sidebar_label: gRPC インターフェース
---

# gRPC インターフェース

## 概要 {#grpc-interface-introduction}

ClickHouseは[gRPC](https://grpc.io/) インターフェースをサポートしています。これは、HTTP/2と[Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers)を使用するオープンソースのリモートプロシージャコールシステムです。ClickHouseにおけるgRPCの実装は次の機能をサポートしています：

- SSL;
- 認証;
- セッション;
- 圧縮;
- 同じチャネルを通じた並列クエリ;
- クエリのキャンセル;
- 進捗とログ取得;
- 外部テーブル。

インターフェースの仕様は[clickhouse_grpc.proto](https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/grpc_protos/clickhouse_grpc.proto)に記載されています。

## gRPC 設定 {#grpc-interface-configuration}

gRPCインターフェースを使用するには、メイン[サーバー設定](../operations/configuration-files.md)で `grpc_port` を設定します。その他の設定オプションは次の例で確認してください：

```xml
<grpc_port>9100</grpc_port>
    <grpc>
        <enable_ssl>false</enable_ssl>

        <!-- SSLが有効な場合のみ使用される以下の2つのファイル -->
        <ssl_cert_file>/path/to/ssl_cert_file</ssl_cert_file>
        <ssl_key_file>/path/to/ssl_key_file</ssl_key_file>

        <!-- サーバーがクライアントに証明書を要求するかどうか -->
        <ssl_require_client_auth>false</ssl_require_client_auth>

        <!-- ssl_require_client_auth=trueの場合のみ使用 -->
        <ssl_ca_cert_file>/path/to/ssl_ca_cert_file</ssl_ca_cert_file>

        <!-- デフォルトの圧縮アルゴリズム（クライアントが別のアルゴリズムを指定しない場合に適用）。
             サポートされるアルゴリズム: none, deflate, gzip, stream_gzip -->
        <compression>deflate</compression>

        <!-- デフォルトの圧縮レベル（クライアントが別のレベルを指定しない場合に適用）。
             サポートされるレベル: none, low, medium, high -->
        <compression_level>medium</compression_level>

        <!-- 送受信メッセージサイズの制限（バイト単位）。-1は制限なしを意味します -->
        <max_send_message_size>-1</max_send_message_size>
        <max_receive_message_size>-1</max_receive_message_size>

        <!-- 詳細なログを取得したい場合は有効にします -->
        <verbose_logs>false</verbose_logs>
    </grpc>
```

## 組み込みクライアント {#grpc-client}

提供される[仕様](https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/grpc_protos/clickhouse_grpc.proto)を使用して、gRPCがサポートする任意のプログラミング言語でクライアントを開発できます。または、組み込みのPythonクライアントを使用することもできます。このクライアントはリポジトリ内の[utils/grpc-client/clickhouse-grpc-client.py](https://github.com/ClickHouse/ClickHouse/blob/master/utils/grpc-client/clickhouse-grpc-client.py)に配置されています。組み込みクライアントには、[grpcioとgrpcio-tools](https://grpc.io/docs/languages/python/quickstart)のPythonモジュールが必要です。

クライアントは次の引数をサポートしています：

- `--help` – ヘルプメッセージを表示して終了します。
- `--host HOST, -h HOST` – サーバー名。デフォルト値: `localhost`。IPv4またはIPv6アドレスも使用できます。
- `--port PORT` – 接続するポート。このポートはClickHouseサーバーの設定で有効にする必要があります（`grpc_port`を参照）。デフォルト値: `9100`。
- `--user USER_NAME, -u USER_NAME` – ユーザー名。デフォルト値: `default`。
- `--password PASSWORD` – パスワード。デフォルト値: 空の文字列。
- `--query QUERY, -q QUERY` – 非対話モードで使用するクエリ。
- `--database DATABASE, -d DATABASE` – デフォルトデータベース。指定されていない場合、サーバー設定でセットされた現在のデータベースが使用されます（デフォルトは`default`）。
- `--format OUTPUT_FORMAT, -f OUTPUT_FORMAT` – 結果の出力[形式](formats.md)。対話モードのデフォルト値: `PrettyCompact`。
- `--debug` – デバッグ情報の表示を有効にします。

`--query`引数なしで呼び出すことでクライアントを対話モードで開始できます。

バッチモードでは、`stdin`を通じてクエリデータを渡すことができます。

**クライアント使用例**

以下の例では、テーブルを作成し、CSVファイルからデータをロードします。その後、テーブルの内容をクエリします。

``` bash
./clickhouse-grpc-client.py -q "CREATE TABLE grpc_example_table (id UInt32, text String) ENGINE = MergeTree() ORDER BY id;"
echo -e "0,Input data for\n1,gRPC protocol example" > a.csv
cat a.csv | ./clickhouse-grpc-client.py -q "INSERT INTO grpc_example_table FORMAT CSV"

./clickhouse-grpc-client.py --format PrettyCompact -q "SELECT * FROM grpc_example_table;"
```

結果：

``` text
┌─id─┬─text──────────────────┐
│  0 │ Input data for        │
│  1 │ gRPC protocol example │
└────┴───────────────────────┘
```
