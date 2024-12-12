---
sidebar_label: NiFi
sidebar_position: 12
keywords: [clickhouse, nifi, connect, integrate, etl, data integration]
slug: /ja/integrations/nifi
description: NiFiデータパイプラインを使用してClickHouseにデータをストリーム
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# Apache NiFiをClickHouseに接続

<a href="https://nifi.apache.org/" target="_blank">Apache NiFi</a>は、オープンソースのワークフロー管理ソフトウェアで、ソフトウェアシステム間のデータフローを自動化するために設計されています。ETLデータパイプラインを作成することができ、300以上のデータプロセッサが用意されています。このステップバイステップのチュートリアルでは、Apache NiFiをClickHouseにソースおよびデスティネーションとして接続し、サンプルデータセットをロードする方法を示します。

## 1. 接続情報を集める
<ConnectionDetails />

## 2. Apache NiFiをダウンロードして実行

1. 新しいセットアップのために、https://nifi.apache.org/download.html からバイナリをダウンロードし、`./bin/nifi.sh start` を実行して開始します。

## 3. ClickHouse JDBCドライバーをダウンロード

1. GitHubの<a href="https://github.com/ClickHouse/clickhouse-java/releases" target="_blank">ClickHouse JDBCドライバーリリースページ</a>を訪問し、最新のJDBCリリース版を探します。
2. リリースバージョンで「Show all xx assets」をクリックし、「shaded」または「all」のキーワードを含むJARファイル、例えば `clickhouse-jdbc-0.5.0-all.jar` を探します。
3. JARファイルをApache NiFiがアクセスできるフォルダーに配置し、絶対パスをメモしておきます。

## 4. DBCPConnectionPoolコントローラーサービスを追加してプロパティを設定

1. Apache NiFiでコントローラーサービスを設定するために、「歯車」ボタンをクリックしてNiFiフロー構成ページにアクセスします。

    <img src={require('./images/nifi_01.png').default} class="image" alt="Nifi Flow Configuration" style={{width: '50%'}}/>

2. コントローラーサービスタブを選択し、右上の`+`ボタンをクリックして新しいコントローラーサービスを追加します。

    <img src={require('./images/nifi_02.png').default} class="image" alt="Add Controller Service" style={{width: '80%'}}/>

3. `DBCPConnectionPool`を検索して「Add」ボタンをクリックします。

    <img src={require('./images/nifi_03.png').default} class="image" alt="Search for DBCPConnectionPool" style={{width: '80%'}}/>

4. 新しく追加されたDBCPConnectionPoolはデフォルトで無効な状態になります。「歯車」ボタンをクリックして設定を開始します。

    <img src={require('./images/nifi_04.png').default} class="image" alt="Nifi Flow Configuration" style={{width: '80%'}}/>

5. 「プロパティ」セクションの下に以下の値を入力します。

  | プロパティ                     | 値                                                                 | 備考                                                                       |
  | --------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------- |
  | Database Connection URL     | jdbc:ch:https://HOSTNAME:8443/default?ssl=true                     | 接続URLに応じてHOSTNAMEを置き換える                                       |
  | Database Driver Class Name  | com.clickhouse.jdbc.ClickHouseDriver                               ||
  | Database Driver Location(s) | /etc/nifi/nifi-X.XX.X/lib/clickhouse-jdbc-0.X.X-patchXX-shaded.jar | ClickHouse JDBCドライバJARファイルへの絶対パス                           |
  | Database User               | default                                                            | ClickHouseのユーザー名                                                     |
  | Password                    | password                                                 | ClickHouseのパスワード                                                     |

6. 設定セクションで、コントローラーサービスの名前を「ClickHouse JDBC」に変更し、参照しやすいようにします。

    <img src={require('./images/nifi_05.png').default} class="image" alt="Nifi Flow Configuration" style={{width: '80%'}}/>

7. DBCPConnectionPoolコントローラーサービスを有効化するために「稲妻」ボタンをクリックし、「Enable」ボタンを押します。

    <img src={require('./images/nifi_06.png').default} class="image" alt="Nifi Flow Configuration" style={{width: '80%'}}/>

    <br/>

    <img src={require('./images/nifi_07.png').default} class="image" alt="Nifi Flow Configuration" style={{width: '80%'}}/>

8. コントローラーサービスタブを確認し、コントローラーサービスが有効になっていることを確かめます。

    <img src={require('./images/nifi_08.png').default} class="image" alt="Nifi Flow Configuration" style={{width: '80%'}}/>

## 5. ExecuteSQLプロセッサを使用してテーブルから読み込み

1. ​​ExecuteSQLプロセッサを追加し、適切な上流および下流のプロセッサを設定します。

    <img src={require('./images/nifi_09.png').default} class="image" alt="​​ExecuteSQL processor" style={{width: '50%'}}/>

2. ​​ExecuteSQLプロセッサの「プロパティ」セクションの下に以下の値を入力します。

    | プロパティ                            | 値                                      | 備考                                                     |
    | ------------------------------------- | -------------------------------------- | ------------------------------------------------------------ |
    | Database Connection Pooling Service   | ClickHouse JDBC                        | ClickHouse用に設定されたコントローラーサービスを選択           |
    | SQL select query                      | SELECT * FROM system.metrics           | ここにクエリを入力                                            |

3. ​​ExecuteSQLプロセッサを開始します。

    <img src={require('./images/nifi_10.png').default} class="image" alt="​​ExecuteSQL processor" style={{width: '80%'}}/>

4. クエリが正常に処理されたことを確認するために、出力キューのFlowFileを確認します。

    <img src={require('./images/nifi_11.png').default} class="image" alt="​​ExecuteSQL processor" style={{width: '80%'}}/>

5. 出力FlowFileの結果をフォーマット表示で確認します。

    <img src={require('./images/nifi_12.png').default} class="image" alt="​​ExecuteSQL processor" style={{width: '80%'}}/>

## 6. MergeRecordとPutDatabaseRecordプロセッサを使用してテーブルに書き込み

1. 複数の行を一度に挿入するには、まず複数のレコードを単一のレコードにマージする必要があります。これにはMergeRecordプロセッサを使用します。

2. MergeRecordプロセッサの「プロパティ」セクションの下に以下の値を入力します。

    | プロパティ                  | 値                | 備考                                                                                      |
    | --------------------------- | ------------------| ----------------------------------------------------------------------------------------- |
    | Record Reader               | JSONTreeReader    | 適切なレコードリーダーを選択                                                             |
    | Record Writer               | JSONReadSetWriter | 適切なレコードライターを選択                                                             |
    | Minimum Number of Records   | 1000              | 最低限マージされる必要がある行数を増やします。デフォルトは1行                               |
    | Maximum Number of Records   | 10000             | 「Minimum Number of Records」より大きい値にします。デフォルトは1,000行                     |

3. 複数のレコードが1つにマージされていることを確認するために、MergeRecordプロセッサの入力と出力を確認します。出力は複数の入力レコードの配列です。

    入力
    <img src={require('./images/nifi_13.png').default} class="image" alt="​​ExecuteSQL processor" style={{width: '50%'}}/>

    出力
    <img src={require('./images/nifi_14.png').default} class="image" alt="​​ExecuteSQL processor" style={{width: '50%'}}/>

4. PutDatabaseRecordプロセッサの「プロパティ」セクションの下に以下の値を入力します。

    | プロパティ                            | 値                | 備考                                                                                                                                     |
    | ----------------------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
    | Record Reader                       | JSONTreeReader  | 適切なレコードリーダーを選択                                                                                                         |
    | Database Type                       | Generic         | デフォルトのままにします                                                                                                               |
    | Statement Type                      | INSERT          |                                                                                                                                         |
    | Database Connection Pooling Service | ClickHouse JDBC | ClickHouseコントローラーサービスを選択                                                                                                |
    | Table Name                          | tbl             | テーブル名をここに入力                                                                                                                  |
    | Translate Field Names               | false           | 挿入されるフィールド名がカラム名と一致する必要がある場合は「false」に設定                                                                                   |
    | Maximum Batch Size                  | 1000            | 挿入ごとの最大行数。この値はMergeRecordプロセッサの「Minimum Number of Records」の値より小さくしてはいけません。 |

4. 各挿入に複数の行が含まれていることを確認するために、MergeRecordで定義された「Minimum Number of Records」の値以上にテーブルの行数が増加しているか確認します。

    <img src={require('./images/nifi_15.png').default} class="image" alt="​​ExecuteSQL processor" style={{width: '50%'}}/>

5. おめでとうございます - Apache NiFiを使用してClickHouseにデータを正常にロードしました！
