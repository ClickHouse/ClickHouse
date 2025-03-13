---
sidebar_label: Python
sidebar_position: 10
keywords: [clickhouse, python, client, connect, integrate]
slug: /ja/integrations/python
description: ClickHouseにPythonを接続するためのClickHouse Connectプロジェクトスイート
---

import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# ClickHouse Connectを使用したPythonの統合

## イントロダクション

ClickHouse Connectは、非常に多くのPythonアプリケーションとの相互運用性を提供するコアデータベースドライバです。

- 主なインターフェイスはパッケージ`clickhouse_connect.driver`内の`Client`オブジェクトです。このコアパッケージには、ClickHouseサーバーとの通信や挿入と選択クエリの高度な管理のための"コンテクスト"実装に使用されるさまざまなヘルパークラスとユーティリティ関数も含まれています。
- `clickhouse_connect.datatypes`パッケージは、エクスペリメンタルでないすべてのClickHouseデータタイプの基本実装とサブクラスを提供します。その主な機能は、ClickHouseとクライアントアプリケーション間で最も効率的な転送を達成するために使用されるClickHouseの"ネイティブ"バイナリ列形式へのデータのシリアライズとデシリアライズです。
- `clickhouse_connect.cdriver`パッケージ内のCython/Cクラスは、最も一般的なシリアル化とデシリアル化のいくつかを最適化し、純粋なPythonのパフォーマンスを大幅に向上させます。
- パッケージ`clickhouse_connect.cc_sqlalchemy`には、[SQLAlchemy](https://www.sqlalchemy.org/)方言が限られています。この制限された実装は、クエリ/カーソル機能に焦点を当て、通常SQLAlchemy DDLおよびORM操作はサポートしません（SQLAlchemyはOLTPデータベース向けであり、ClickHouse OLAP指向のデータベースを管理するためにより専門的なツールやフレームワークをお勧めします）。
- コアドライバおよびClickHouse Connect SQLAlchemy実装は、ClickHouseをApache Supersetに接続するための推奨方法です。`ClickHouse Connect`データベース接続または`clickhousedb` SQLAlchemy方言接続文字列を使用してください。

このドキュメントは、ベータリリース0.8.2の時点で有効です。

:::note
公式のClickHouse Connect Pythonドライバは、ClickHouseサーバーとの通信にHTTPプロトコルを使用します。
これは、柔軟性の向上、HTTPバランサのサポート、JDBCベースのツールとの互換性の向上などの利点がありますが、圧縮率やパフォーマンスのやや低下、ネイティブTCPベースのプロトコルの一部の複雑な機能のサポートの欠如などのデメリットもあります。
一部のユースケースでは、ネイティブTCPベースのプロトコルを使用する[コミュニティPythonドライバ](/docs/ja/interfaces/third-party/client-libraries.md)の使用を検討することもできます。
:::

### 要件と互換性

|    Python |   |       Platform¹ |   | ClickHouse |    | SQLAlchemy² |   | Apache Superset |   |
|----------:|:--|----------------:|:--|-----------:|:---|------------:|:--|----------------:|:--|
| 2.x, <3.8 | ❌ |     Linux (x86) | ✅ |     <24.3³ | 🟡 |        <1.3 | ❌ |            <1.4 | ❌ |
|     3.8.x | ✅ | Linux (Aarch64) | ✅ |     24.3.x | ✅  |       1.3.x | ✅ |           1.4.x | ✅ |
|     3.9.x | ✅ |     macOS (x86) | ✅ | 24.4-24.6³ | 🟡 |       1.4.x | ✅ |           1.5.x | ✅ |
|    3.10.x | ✅ |     macOS (ARM) | ✅ |     24.7.x | ✅  |       >=2.x | ❌ |           2.0.x | ✅ |
|    3.11.x | ✅ |         Windows | ✅ |     24.8.x | ✅  |             |   |           2.1.x | ✅ |
|    3.12.x | ✅ |                 |   |     24.9.x | ✅  |             |   |           3.0.x | ✅ |

¹ClickHouse Connectは、リストされたプラットフォームに対して明示的にテストされています。さらに、未テストのバイナリホイール（Cの最適化を含む）は、優れた[cibuildwheel](https://cibuildwheel.readthedocs.io/en/stable/)プロジェクトがサポートするすべてのアーキテクチャ用にビルドされます。
最後に、ClickHouse Connectは純粋なPythonとしても実行できるため、ソースインストールは最近のPythonインストールで動作するはずです。

²再度、SQLAlchemyサポートは主にクエリ機能に制限されています。SQLAlchemy API全体はサポートされていません。

³ClickHouse Connectは、現在サポートされているすべてのClickHouseバージョンに対してテストされています。HTTPプロトコルを使用しているため、他のバージョンのClickHouseでも正しく動作するはずですが、一部の高度なデータ型との非互換性が存在する可能性があります。

### インストール

PyPIからpipを通じてClickHouse Connectをインストールします：

`pip install clickhouse-connect`

ClickHouse Connectをソースからインストールすることもできます：
* `git clone`で[GitHubリポジトリ](https://github.com/ClickHouse/clickhouse-connect)をクローンします。
* （オプション）`pip install cython`を実行してC/Cythonの最適化をビルドおよび有効化
* プロジェクトのルートディレクトリに移動して`pip install .`を実行

### サポートポリシー

ClickHouse Connectは現在ベータですので、現在のベータリリースのみが積極的にサポートされています。問題を報告する前に最新バージョンに更新してください。問題は[GitHubプロジェクト](https://github.com/ClickHouse/clickhouse-connect/issues)に報告してください。ClickHouse Connectの将来のリリースは、リリース時点でアクティブにサポートされているClickHouseバージョンと互換性を持つことが保証されています（一般に、最新の`stable` 3つと`lts`の最近の2つのリリース）。

### 基本的な使用法

### 接続の詳細を集める

<ConnectionDetails />

#### 接続を確立する

ClickHouseに接続するための2つの例を示します：
- ローカルホストのClickHouseサーバーに接続する。
- ClickHouse Cloudサービスに接続する。

##### ローカルホストのClickHouseサーバーに接続するためにClickHouse Connectクライアントインスタンスを使用:

```python
import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', username='default', password='password')
```

##### ClickHouse Cloudサービスに接続するためにClickHouse Connectクライアントインスタンスを使用:

:::tip
接続の詳細を前もって集めてください。ClickHouse CloudサービスはTLSを必要とするため、ポート8443を使用します。
:::

```python
import clickhouse_connect

client = clickhouse_connect.get_client(host='HOSTNAME.clickhouse.cloud', port=8443, username='default', password='your password')
```

#### データベースと対話する

ClickHouse SQLコマンドを実行するには、クライアントの`command`メソッドを使用します：

```python
client.command('CREATE TABLE new_table (key UInt32, value String, metric Float64) ENGINE MergeTree ORDER BY key')
```

バッチデータを挿入するには、クライアントの`insert`メソッドを使用し、行と値の2次元配列を指定します：

```python
row1 = [1000, 'String Value 1000', 5.233]
row2 = [2000, 'String Value 2000', -107.04]
data = [row1, row2]
client.insert('new_table', data, column_names=['key', 'value', 'metric'])
```

ClickHouse SQLを使用してデータを取得するには、クライアントの`query`メソッドを使用します：

```python
result = client.query('SELECT max(key), avg(metric) FROM new_table')
result.result_rows
Out[13]: [(2000, -50.9035)]
```

## ClickHouse Connect ドライバAPI

***注:*** ほとんどのAPIメソッドには多くの引数があり、その多くはオプションであるため、キーワード引数を渡すことをお勧めします。

*ここに文書化されていないメソッドはAPIの一部と見なされず、削除または変更される可能性があります。*

### クライアントの初期化

`clickhouse_connect.driver.client`クラスは、PythonアプリケーションとClickHouseデータベースサーバー間の主要なインターフェイスを提供します。`clickhouse_connect.get_client`関数を使用してClientインスタンスを取得します。この関数は以下の引数を受け取ります：

#### 接続引数

| パラメータ                | タイプ       | デフォルト                     | 説明                                                                                                                                                                                                                                      |
|-----------------------|-------------|-------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| interface             | str         | http                          | httpまたはhttpsである必要があります。                                                                                                                                                                                                 |
| host                  | str         | localhost                     | ClickHouseサーバーのホスト名またはIPアドレス。設定しない場合、`localhost`が使用されます。                                                                                                                                                    |
| port                  | int         | 8123または8443                | ClickHouse HTTPまたはHTTPSポート。設定しない場合はデフォルトで8123、*secure*=*True*または*interface*=*https*が指定されている場合は8443が使用されます。                                                                                   |
| username              | str         | default                       | ClickHouseのユーザー名。設定しない場合は、`default` ClickHouseユーザーが使用されます。                                                                                                                                                  |
| password              | str         | *&lt;空の文字列&gt;*          | *username*のパスワード。                                                                                                                                                                                                               |
| database              | str         | *None*                        | 接続のデフォルトデータベース。設定しない場合、ClickHouse Connectは*username*のデフォルトデータベースを使用します。                                                                                                                         |
| secure                | bool        | False                         | https/TLSを使用します。これは、インターフェイスまたはポート引数から派生した値を上書きします。                                                                                                                                         |
| dsn                   | str         | *None*                        | 標準DSN（データソース名）形式の文字列。この文字列から、設定されていない他の接続値（ホストやユーザーなど）が抽出されます。                                                                                                                  |
| compress              | bool or str | True                          | ClickHouse HTTPの挿入とクエリ結果の圧縮を有効にする。 [追加オプション（圧縮）](#compression) を参照                                                                                                                                         |
| query_limit           | int         | 0（無制限）                   | `query`応答の行数の最大数。0に設定すると無制限の行が返されます。大きなクエリ制限は、結果がストリーミングされないときに一度にすべての結果がメモリにロードされるため、メモリ不足の例外を引き起こす可能性があります。                                                   |
| query_retries         | int         | 2                             | `query`リクエストの最大リトライ回数。リトライ可能なHTTPレスポンスのみがリトライされます。`command`または`insert`リクエストは、意図しない重複リクエストを防ぐため、ドライバによって自動的にリトライされません。                                                                         |
| connect_timeout       | int         | 10                            | HTTP接続タイムアウト（秒）。                                                                                                                                                                                                             |
| send_receive_timeout  | int         | 300                           | HTTP接続の送受信タイムアウト（秒）。                                                                                                                                                                                                    |
| client_name           | str         | *None*                        | HTTPのUser Agentヘッダーに先行するclient_nameあります。ClickHouseのsystem.query_logでクライアントクエリを追跡するためにこれを設定します。                                                                                               |
| pool_mgr              | obj         | *&lt;デフォルトのPoolManager&gt;* | `urllib3`ライブラリのPoolManagerを使用します。異なるホストへの複数の接続プールを必要とする高度な使い方が必要な場合に使用します。                                                                                                        |
| http_proxy            | str         | *None*                        | HTTPプロキシアドレス（HTTP_PROXY環境変数を設定するのと同様）。                                                                                                                                                                            |
| https_proxy           | str         | *None*                        | HTTPSプロキシアドレス（HTTPS_PROXY環境変数を設定するのと同様）。                                                                                                                                                                          |
| apply_server_timezone | bool        | True                          | タイムゾーン対応のクエリ結果にサーバーのタイムゾーンを使用します。 [Timezone Precedence](#time-zones) を参照                                                                                                                                  |

#### HTTPS/TLS 引数

| パラメータ          | タイプ | デフォルト | 説明                                                                                                                                                                                                                                                                            |
|------------------|------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| verify           | bool | True    | HTTPS/TLSを使用する場合、ClickHouseサーバーTLS/SSL証明書（ホスト名、有効期限など）を検証します。                                                                                                                                                                                     |
| ca_cert          | str  | *None*  | *verify*=*True*の場合、ClickHouseサーバー証明書を検証するための認証局ルートのファイルパス（.pem形式）。verifyがFalseの場合は無視されます。ClickHouseサーバー証明書がグローバルに信頼されているルートである場合、認証が不要です。                                                      |
| client_cert      | str  | *None*  | TLSクライアント証明書のファイルパス（.pem形式）。（双方向TLS認証用）。ファイルには中間証明書を含む完全な証明書チェーンが含まれている必要があります。                                                                                                                               |
| client_cert_key  | str  | *None*  | クライアント証明書の秘密鍵のファイルパス。秘密鍵がクライアント証明書ファイルに含まれていない場合に必要です。                                                                                                                                                                         |
| server_host_name | str  | *None*  | プロキシまたは異なるホスト名のトンネルを介して接続する際にSSLエラーを回避するために設定するTLS証明書のCNまたはSNIとして識別されるClickHouseサーバーのホスト名                                                                                                                                       |
| tls_mode         | str  | *None*  | 高度なTLS 動作を制御します。`proxy` および `strict` は ClickHouse の双方向 TLS 接続を呼び出しませんが、クライアント証明書とキーを送信します。 `mutual` はクライアント証明書による ClickHouse 双方向TLS認証を想定しています。*None*/デフォルトの動作は `mutual` です。                          |

#### 設定引数

最後に、`get_client`の`settings`引数は、クライアントリクエストごとにサーバーに追加のClickHouse設定を渡すために使用されます。ほとんどのクエリで*readonly*=*1*アクセスを持つユーザーは設定を変更できないので注意してください。そのため、ClickHouse Connectはそのような設定を最終リクエストでドロップし、警告をログに記録します。以下の設定は、ClickHouse Connectによって使用されるHTTPクエリ/セッションにのみ適用され、一般的なClickHouse設定としては文書化されていません。

| 設定               | 説明                                                                                                                                                                         |
|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| buffer_size       | ClickHouseサーバーがHTTPチャネルに書き込む前に使用するバッファサイズ（バイト単位）。                                                                                                                                              |
| session_id        | サーバー上の関連クエリを関連付けるための一意のセッションID。仮のテーブルに必要。                                                                                                                                                         |
| compress          | ClickHouseサーバーがPOST応答データを圧縮する必要があるかどうか。この設定は、"raw"クエリにのみ使用する必要があります。                                                                                                           |
| decompress        | ClickHouseサーバーに送信されたデータを解凍する必要があるかどうか。この設定は、"raw"挿入にのみ使用する必要があります。                                                                                                              |
| quota_key         | このリクエストに関連付けられたクォータキー。ClickHouseサーバーのドキュメント内のクォータを参照してください。                                                                                                                                      |
| session_check     | セッションステータスをチェックするために使用されます。                                                                                                                                                                |
| session_timeout   | セッションIDで識別されるセッションがタイムアウトし、有効でなくなったとみなされるまでの非アクティブ状態の秒数。デフォルトは60秒です。                                                                                                                    |
| wait_end_of_query | ClickHouseサーバーで応答全体をバッファリングします。この設定は、サマリー情報を返すために必要で、ストリーミングされないクエリで自動的に設定されます。                                                                                                               |

クエリとともに送信できる他のClickHouse設定については、[ClickHouseのドキュメント](/docs/ja/operations/settings/settings.md)を参照してください。

#### クライアント作成例

- すべてのパラメータを指定しない場合、ClickHouse Connect クライアントは、デフォルトのHTTPポートで`localhost`にデフォルトのユーザーで接続します：

```python
import clickhouse_connect

client = clickhouse_connect.get_client()
client.server_version
Out[2]: '22.10.1.98'
```

- セキュアな（https）外部ClickHouseサーバーへの接続

```python
import clickhouse_connect

client = clickhouse_connect.get_client(host='play.clickhouse.com', secure=True, port=443, user='play', password='clickhouse')
client.command('SELECT timezone()')
Out[2]: 'Etc/UTC'
```

- セッションIDやその他のカスタム接続パラメータ、ClickHouse設定を使用した接続。

```python
import clickhouse_connect

client = clickhouse_connect.get_client(host='play.clickhouse.com',
                                       user='play',
                                       password='clickhouse',
                                       port=443,
                                       session_id='example_session_1',
                                       connect_timeout=15,
                                       database='github',
                                       settings={'distributed_ddl_task_timeout':300})
client.database
Out[2]: 'github'
```

### 共通メソッド引数

いくつかのクライアントメソッドは、共通の`parameters`および`settings`引数を使用します。これらのキーワード引数は以下のように説明されています。

#### パラメータ引数

ClickHouse Connect Clientの`query*`および`command`メソッドは、ClickHouse値式にPython表現をバインドするために使用されるオプションの`parameters`キーワード引数を受け取ります。二つのバインド方法があります。

##### サーバーサイドバインディング

ClickHouseはほとんどのクエリ値に対して[サーバーサイドバインディング](/docs/ja/interfaces/cli.md#cli-queries-with-parameters)をサポートしており、バインドされた値はクエリとは別にHTTPクエリパラメータとして送信されます。ClickHouse Connectは、バインド式が{&lt;name&gt;:&lt;datatype&gt;}の形式であることを検出した場合、適切なクエリパラメータを追加します。サーバーサイドバインディングの場合、`parameters`引数はPythonのDictionaryであるべきです。

- PythonDictionaryを使用したサーバーサイドバインディング、DateTime値、および文字列値

```python
import datetime

my_date = datetime.datetime(2022, 10, 1, 15, 20, 5)

parameters = {'table': 'my_table', 'v1': my_date, 'v2': "a string with a single quote'"}
client.query('SELECT * FROM {table:Identifier} WHERE date >= {v1:DateTime} AND string ILIKE {v2:String}', parameters=parameters)

# サーバーで生成されるクエリは次のとおりです。
# SELECT * FROM my_table WHERE date >= '2022-10-01 15:20:05' AND string ILIKE 'a string with a single quote\''
```

**重要** -- サーバーサイドバインディングは、ClickHouseサーバーによって`SELECT`クエリのみにサポートされています。他のクエリタイプ（`ALTER`、`DELETE`、`INSERT`など）には対応していません。この点は将来的に変更される可能性があります：https://github.com/ClickHouse/ClickHouse/issues/42092 を参照。

##### クライアントサイドバインディング

ClickHouse Connectはクライアントサイドのパラメータバインディングもサポートしており、テンプレート化されたSQLクエリを生成する際の柔軟性を向上させます。クライアントサイドバインディングの場合、`parameters`引数はDictionaryまたはシーケンスであるべきです。クライアントサイドバインディングは、Pythonの[「printf」スタイル](https://docs.python.org/3/library/stdtypes.html#old-string-formatting)文字列フォーマットを使用してパラメータを置換します。

サーバーサイドバインディングとは異なり、データベース識別子（データベース、テーブル、カラム名など）にはクライアントサイドバインディングが機能しないため注意が必要です。Pythonスタイルのフォーマットは異なるタイプの文字列を区別できないため、これらは異なる方法でフォーマットする必要があります（データベース識別子には逆アクセントまたはダブルクォート、データ値にはシングルクォートが使用されます）。

- PythonDictionary、DateTime値、文字列エスケープを用いた例

```python
import datetime

my_date = datetime.datetime(2022, 10, 1, 15, 20, 5)

parameters = {'v1': my_date, 'v2': "a string with a single quote'"}
client.query('SELECT * FROM some_table WHERE date >= %(v1)s AND string ILIKE %(v2)s', parameters=parameters)

# 以下のクエリを生成:
# SELECT * FROM some_table WHERE date >= '2022-10-01 15:20:05' AND string ILIKE 'a string with a single quote\''
```

- Pythonシーケンス（タプル）、Float64、およびIPv4Addressを用いた例

```python
import ipaddress

parameters = (35200.44, ipaddress.IPv4Address(0x443d04fe))
client.query('SELECT * FROM some_table WHERE metric >= %s AND ip_address = %s', parameters=parameters)

# 以下のクエリを生成:
# SELECT * FROM some_table WHERE metric >= 35200.44 AND ip_address = '68.61.4.254''
```

:::note
DateTime64引数（サブ秒精度を含むClickHouseタイプ）をバインドするには、次のいずれかのカスタムアプローチが必要です:
- Pythonの`datetime.datetime`値を新しいDT64Paramクラスでラップします。例
  ```python
    query = 'SELECT {p1:DateTime64(3)}'  # Dictionaryを使ったサーバーサイドバインディング
    parameters={'p1': DT64Param(dt_value)}
  
    query = 'SELECT %s as string, toDateTime64(%s,6) as dateTime' # リストを使ったクライアントサイドバインディング 
    parameters=['a string', DT64Param(datetime.now())]
  ```
  - パラメータ値のDictionaryを使う場合、パラメータ名に文字列 `_64` を付加します
  ```python
    query = 'SELECT {p1:DateTime64(3)}, {a1:Array(DateTime(3))}'  # Dictionaryを使ったサーバーサイドバインディング
  
    parameters={'p1_64': dt_value, 'a1_64': [dt_value1, dt_value2]}
  ```
:::

#### 設定引数

すべての主要なClickHouse Connectクライアントの「挿入」と「選択」メソッドは、渡されたSQL文に対するClickHouseサーバーの[ユーザー設定](/docs/ja/operations/settings/settings.md)を渡すためのオプションの`settings`キーワード引数を受け取ります。`settings`引数はDictionaryであるべきです。各アイテムは、ClickHouse設定名とその関連する値であるべきです。設定がサーバーにクエリパラメータとして送信される際に、値は文字列に変換されます。

クライアントレベル設定と同様に、ClickHouse Connectは、サーバーによって*readonly*=*1*とマークされた設定をドロップし、関連するログメッセージを出力します。HTTPインターフェースを介したクエリにのみ適用される設定は常に有効です。それらの設定については`get_client` [API](#settings-argument)を参照してください。

ClickHouse設定を使用した例：

```python
settings = {'merge_tree_min_rows_for_concurrent_read': 65535,
            'session_id': 'session_1234',
            'use_skip_indexes': False}
client.query("SELECT event_type, sum(timeout) FROM event_errors WHERE event_time > '2022-08-01'", settings=settings)
```

### クライアント_command_ メソッド

`Client.command`メソッドを使用して、通常データを返さないまたは単一のプリミティブまたは配列値を返すSQLクエリをClickHouseサーバーに送信します。このメソッドは以下のパラメータを取ります：

| パラメータ     | タイプ             | デフォルト    | 説明                                                                                                                                                     |
|---------------|------------------|------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| cmd           | str              | *必須*     | 単一の値または単一行の値を返すClickHouse SQLステートメント。                                                                                              |                                                                                                                                                                                                                                                                              |
| parameters    | dictまたはiterable | *None*     | [parameters description](#parameters-argument)を参照してください。                                                                                                             |
| data          | strまたはbytes     | *None*     | コマンドとともにPOST本文として含めるオプションのデータ。                                                                                                  |
| settings      | dict             | *None*     | [settings description](#settings-argument)を参照してください。                                                                                                             |
| use_database  | bool             | True       | クライアントデータベース（クライアントを作成する際に指定された）を使用します。Falseの場合、コマンドは接続されたユーザーのデフォルトClickHouseサーバーデータベースを使用します。                                                     |
| external_data | ExternalData     | *None*     | クエリで使用するファイルまたはバイナリデータを含むExternalDataオブジェクト。[Advanced Queries (External Data)](#external-data)を参照してください。                           |

- _command_は、DDLステートメントに使用できます。SQL "コマンド"がデータを返さない場合、ClickHouse X-ClickHouse-SummaryおよびX-ClickHouse-Query-Idヘッダーのキー/値ペア`written_rows`、`written_bytes`、および`query_id`を含む"クエリサマリー"Dictionaryが代わりに返されます。

```python
client.command('CREATE TABLE test_command (col_1 String, col_2 DateTime) Engine MergeTree ORDER BY tuple()')
client.command('SHOW CREATE TABLE test_command')
Out[6]: 'CREATE TABLE default.test_command\\n(\\n    `col_1` String,\\n    `col_2` DateTime\\n)\\nENGINE = MergeTree\\nORDER BY tuple()\\nSETTINGS index_granularity = 8192'
```

- _command_は、単一行のみを返す単純なクエリにも使用できます。

```python
result = client.command('SELECT count() FROM system.tables')
result
Out[7]: 110
```

### クライアント_query_ メソッド

`Client.query`メソッドは、ClickHouseサーバーから単一の"バッチ"データセットを取得するための主要な方法です。
これは、ClickHouseのネイティブ形式をHTTP経由で使用して、大規模なデータセット（最大約100万行）を効率的に転送します。このメソッドは以下のパラメータを取ります。

| パラメータ               | タイプ             | デフォルト    | 説明                                                                                                                                                                            |
|-----------------------|------------------|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| query               | str              | *必須*     | ClickHouse SQLのSELECTまたはDESCRIBEクエリ。                                                                                                                                     |
| parameters          | dictまたはiterable | *None*     | [parameters description](#parameters-argument)を参照してください。                                                                                                             |
| settings            | dict             | *None*     | [settings description](#settings-argument)を参照してください。                                                                                                                 |                                                                                                                                                |
| query_formats       | dict             | *None*     | 結果値のデータ型フォーマット指定。Advanced Usage (Read Formats)を参照。                                                                                                         |
| column_formats      | dict             | *None*     | カラムごとのデータ型フォーマット。Advanced Usage (Read Formats)を参照。                                                                                                          |
| encoding            | str              | *None*     | ClickHouse StringカラムをPython文字列にエンコードするためのエンコーディング。Pythonは設定していない場合、`UTF-8`を使用します。                                                |
| use_none            | bool             | True       | ClickHouseのNULLに対してPythonの*None*タイプを使用します。Falseの場合、ClickHouseのNULLに対してデータ型のデフォルト（たとえば0）を使用します。注：パフォーマンス上の理由からnumpy/PandasではデフォルトはFalseです。 |
| column_oriented     | bool             | False      | 結果を列のシーケンスとして返します。Pythonデータを他の列指向データ形式に変換する際に便利です。                                                                                                     |
| query_tz            | str              | *None*     | zoneinfoデータベースからのタイムゾーン名。このタイムゾーンはクエリによって返されるすべてのdatetimeまたはPandas Timestampオブジェクトに適用されます。                                                       |
| column_tzs          | dict             | *None*     | カラム名とタイムゾーン名のDictionary。`query_tz`に似ていますが、異なるカラムに異なるタイムゾーンを指定できます。                                                                                                            |
| use_extended_dtypes | bool             | True       | Pandasの拡張データ型（StringArrayなど）およびClickHouseのNULL値に対するpandas.NAおよびpandas.NaTを使用します。主に`query_df`および`query_df_stream`メソッドに適用されます。                                   |
| external_data       | ExternalData     | *None*     | クエリに使用するファイルまたはバイナリデータを含むExternalDataオブジェクト。[Advanced Queries (External Data)](#external-data)を参照してください。                                                          |
| context             | QueryContext     | *None*     | 上記のメソッド引数をカプセル化する再利用可能なQueryContextオブジェクトを使用できます。 [Advanced Queries (QueryContexts)](#querycontexts)を参照してください。                                                |

#### QueryResult オブジェクト

基本的な`query`メソッドは、以下の公開プロパティを持つQueryResultオブジェクトを返します：

- `result_rows` -- シーケンスとしてデータを返します。各行要素が列値のシーケンスである行のシーケンス。
- `result_columns` -- シーケンスとしてデータを返します。各列要素がそのカラムに対する行値のシーケンスである列のシーケンス。
- `column_names` -- `result_set`内の列名を表す文字列のタプル
- `column_types` -- `result_columns`内の各列に対するClickHouseデータ型を表すClickHouseTypeインスタンスのタプル
- `query_id` -- ClickHouseのquery_id（`system.query_log`テーブルでのクエリ表示に便利です）
- `summary` -- `X-ClickHouse-Summary` HTTPレスポンスヘッダーによって返されたデータ
- `first_item` -- 応答の最初の行をDictionaryとして取得するための便利なプロパティ（キーはカラム名です）
- `first_row` -- 結果の最初の行を返すための便利なプロパティ
- `column_block_stream` -- 列指向フォーマットでクエリ結果を返すジェネレータ。このプロパティは直接参照すべきではありません（以下を参照）。
- `row_block_stream` -- 行指向フォーマットでクエリ結果を返すジェネレータ。このプロパティは直接参照すべきではありません（以下を参照）。
- `rows_stream` -- クエリ結果を1回の呼び出しで1行ずつ生成するジェネレータ。このプロパティは直接参照すべきではありません（以下を参照）。
- `summary` -- `command`メソッドで説明されている通り、ClickHouseから返されるサマリー情報のDictionary

`*_stream`プロパティは、返されたデータのイテレータとして使用できるPython Contextを返します。それらはクライアント`*_stream`メソッドを使用して間接的にのみアクセスする必要があります。

ストリーミングクエリ結果（StreamContextオブジェクトを使用）を完全に理解するには、[Advanced Queries (Streaming Queries)](#streaming-queries)を参照してください。

### Numpy、Pandas、またはArrowを使用したクエリ結果の消費

主要な`query`メソッドの派生バージョンが3つあります：

- `query_np` -- このバージョンは、ClickHouse Connect QueryResultの代わりにNumpy配列を返します。
- `query_df` -- このバージョンは、ClickHouse Connect QueryResultの代わりにPandasデータフレームを返します。
- `query_arrow` -- このバージョンはPyArrowのテーブルを返します。ClickHouseの`Arrow`フォーマットを直接使用し、主要な`query`メソッドと共通の3つの引数のみを受け取ります：`query`、`parameters`、および`settings`。また、`use_strings`という追加の引数があり、ArrowテーブルがClickHouseの文字列型を文字列（Trueの場合）またはバイト（Falseの場合）として表現するかどうかを決定します。

### クライアントストリーミングクエリメソッド

ClickHouse Connect クライアントはストリームとしてデータを取得する（Pythonのジェネレータとして実装された）ための複数のメソッドを提供します：

- `query_column_block_stream` -- ネイティブPythonオブジェクトを使用して、列のシーケンスとしてクエリデータをブロックで返します。
- `query_row_block_stream` -- ネイティブPythonオブジェクトを使用して、行のブロックとしてクエリデータを返します。
- `query_rows_stream` -- ネイティブPythonオブジェクトを使用して、行のシーケンスとしてクエリデータを返します。
- `query_np_stream` -- 各ClickHouseクエリデータブロックをNumpy配列として返します。
- `query_df_stream` -- 各ClickHouseブロックのクエリデータをPandasデータフレームとして返します。
- `query_arrow_stream` -- PyArrow RecordBlocksとしてクエリデータを返します。

これらのメソッドの各々は、消費を開始するために`with`ステートメントを通して開く必要がある`ContextStream`オブジェクトを返します。詳細と例は[Advanced Queries (Streaming Queries)](#streaming-queries)を参照してください。

### クライアント_insert_ メソッド

ClickHouseに複数のレコードを挿入する一般的なユースケースのために、`Client.insert`メソッドがあります。このメソッドは以下のパラメータを受け取ります：

| パラメータ           | タイプ                              | デフォルト    | 説明                                                                                                                                                                                   |
|-------------------|-----------------------------------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table             | str                               | *必須*     | 挿入するClickHouseテーブル。データベースを含む完全なテーブル名が許可されます。                                                                                                            |
| data              | シーケンスのシーケンス             | *必須*     | データを挿入する行のシーケンスまたは列のシーケンス。各行は列値のシーケンス、または列は行値のシーケンスで構成されます。                                                                         |
| column_names      | strまたはstrのシーケンス           | '*'        | データマトリックスの列名のリスト。'*'が使用されている場合、ClickHouse Connectはすべての列名をテーブルから取得するための"pre-query"を実行します。                                    |
| database          | str                               | ''         | 挿入先のデータベース。指定されていない場合、クライアントのデータベースが使用されます。                                                                                                                                     |
| column_types      | ClickHouseTypeのシーケンス        | *None*     | ClickHouseTypeインスタンスのリスト。column_typesまたはcolumn_type_namesのどちらも指定されていない場合、ClickHouse Connectはテーブルのすべての列型を取得するための"pre-query"を実行します。  |
| column_type_names | ClickHouse型名のシーケンス        | *None*     | ClickHouseデータタイプ名のリスト。column_typesまたはcolumn_type_namesのどちらも指定されていない場合、ClickHouse Connectはテーブルのすべての列型を取得するための"pre-query"を実行します。    |
| column_oriented   | bool                              | False      | Trueの場合、`data`引数は列のシーケンスとして仮定され（折り返しは必要ない）、そうでない場合は`data`は行のシーケンスとして解釈されます。                                                 |
| settings          | dict                              | *None*     | [settings description](#settings-argument)を参照してください。                                                                                                                                               |
| insert_context    | InsertContext                     | *None*     | 上記のメソッド引数をカプセル化する再利用可能なInsertContextオブジェクト。 [Advanced Inserts (InsertContexts)](#insertcontexts)を参照してください。                                               |

このメソッドは"クエリサマリー"Dictionaryを返します。"command"メソッドで説明された通りです。挿入が何らかの理由で失敗した場合、例外が発生します。

主要な`insert`メソッドの派生バージョンが2つあります：

- `insert_df` -- Pythonのシーケンスの代わりに、このメソッドの2番目のパラメータは`df`引数を要求し、それはPandas Dataframeインスタンスである必要があります。ClickHouse Connectは、Dataframeを列指向のデータソースとして自動的に処理するため、`column_oriented`パラメータは不要です。
- `insert_arrow` -- Pythonのシーケンスの代わりに、このメソッドは`arrow_table`を要求します。ClickHouse ConnectはArrowテーブルを未変更のままClickHouseサーバーに渡し、`table`と`arrow_table`の他に`database`および`settings`引数のみが利用可能です。

*注意:* Numpy 配列は有効なシーケンスのシーケンスであり、主要な`insert`メソッドの`data`引数として使用できます。そのため、特化されたメソッドは不要です。

### ファイル挿入

`clickhouse_connect.driver.tools`には、既存のClickHouseテーブルに直接ファイルシステムからデータを挿入するための`insert_file`メソッドが含まれています。解析はClickHouseサーバーに委ねられます。`insert_file`は次のパラメータを受け取ります：

| パラメータ        | タイプ            | デフォルト           | 説明                                                                                                                    |
|--------------|-----------------|-------------------|------------------------------------------------------------------------------------------------------------------------|
| client       | Client          | *必須*            | 挿入を行う`driver.Client `                                                                                    |
| table        | str             | *必須*            | 挿入するClickHouseテーブル。データベースを含む完全なテーブル名が許可されます。                                                                        |
| file_path    | str             | *必須*            | データファイルのネイティブなファイルパス                                                                                        |
| fmt          | str             | CSV, CSVWithNames | ファイルのClickHouse入力フォーマット。 `column_names`が提供されていない場合はCSVWithNamesが想定されます。                                   |
| column_names | strのシーケンス | *None*            | データファイル内の列名リスト。列名を含むフォーマットには必須ではありません。                                                                  |
| database     | str             | *None*            | テーブルのデータベース。 テーブルが完全に資格化されている場合は無視されます。指定されていない場合、挿入はクライアントデータベースを使用します。 |
| settings     | dict            | *None*            | [settings description](#settings-argument)を参照してください。                                                                                   |
| compression  | str             | *None*            | Content-Encoding HTTPヘッダーに使用される、認識されたClickHouse圧縮タイプ（zstd, lz4, gzip）                                                 |

データが不整合であるか、日付/時刻の形式が異常な形式であるファイルの場合、設定（`input_format_allow_errors_num`または`input_format_allow_errors_ratio`など）はこのメソッドで認識されます。

```python
import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file

client = clickhouse_connect.get_client()
insert_file(client, 'example_table', 'my_data.csv',
            settings={'input_format_allow_errors_ratio': .2,
                      'input_format_allow_errors_num': 5})
```

### クエリ結果のファイルとして保存する

ClickHouseからローカルファイルシステムに直接ファイルをストリーミングすることができます。このために`raw_stream`メソッドを使用します。例として、クエリ結果をCSVファイルに保存したい場合、以下のコードスニペットを使用できます：

```python
import clickhouse_connect

if __name__ == '__main__':
    client = clickhouse_connect.get_client()
    query = 'SELECT number, toString(number) AS number_as_str FROM system.numbers LIMIT 5'
    fmt = 'CSVWithNames'  # または、CSV, CSVWithNamesAndTypes, TabSeparated, etc.
    stream = client.raw_stream(query=query, fmt=fmt)
    with open("output.csv", "wb") as f:
        for chunk in stream:
            f.write(chunk)
```

上記のコードは、以下の内容を持つ`output.csv`ファイルを生成します：

```csv
"number","number_as_str"
0,"0"
1,"1"
2,"2"
3,"3"
4,"4"
```

これと同様に、[TabSeparated](https://clickhouse.com/docs/ja/interfaces/formats#tabseparated)やその他のフォーマットでデータを保存することができます。[データの入力と出力フォーマット](https://clickhouse.com/docs/ja/interfaces/formats)で利用可能なすべてのフォーマットオプションの概要を参照してください。

### 生のAPI

ClickHouseデータとネイティブまたはサードパーティーデータタイプおよび構造間の変換を必要としないユースケースに対して、ClickHouse Connectクライアントは ClickHouse接続の直接使用のための2つのメソッドを提供します。

#### クライアント_raw_query_ メソッド

`Client.raw_query`メソッドは、クライアント接続を使用してClickHouse HTTPクエリインターフェースを直接使用できるようにします。戻り値は未処理の`bytes`オブジェクトです。パラメータバインディング、エラー処理、リトライおよび設定管理を最小限のインターフェースで提供する便利なラッパーです：

| パラメータ        | タイプ             | デフォルト    | 説明                                                                                                      |
|---------------|------------------|------------|----------------------------------------------------------------------------------------------------------|
| query         | str              | *必須*     | 有効なClickHouseクエリ                                                                                     |
| parameters    | dictまたはiterable | *None*     | [parameters description](#parameters-argument)を参照してください。                                         |
| settings      | dict             | *None*     | [settings description](#settings-argument)を参照してください。                                           |                                                                                                                                                |
| fmt           | str              | *None*     | 結果のbytesのClickHouse出力フォーマット。 （指定されない場合、ClickHouseはTSVを使用します）            |
| use_database  | bool             | True       | クエリコンテキストのためにclickhouse-connectクライアントが割り当てたデータベースを使用                    |
| external_data | ExternalData     | *None*     | クエリで使用するファイルまたはバイナリデータを含むExternalDataオブジェクト。[Advanced Queries (External Data)](#external-data)を参照してください。|

結果の`bytes`オブジェクトを処理するのは呼び出し側の責任です。`Client.query_arrow`はClickHouseの`Arrow`出力フォーマットを使用して、このメソッドの薄いラッパーにすぎないことに注意してください。

#### クライアント_raw_stream_ メソッド

`Client.raw_stream`メソッドは、`raw_query`メソッドと同じAPIを持ちますが、`bytes`オブジェクトのジェネレータ/ストリームソースとして使用できる`io.IOBase`オブジェクトを返します。現在では`query_arrow_stream`メソッドによって利用されています。

#### クライアント_raw_insert_ メソッド

`Client.raw_insert`メソッドはクライアント接続を使用して`bytes`オブジェクトまたは`bytes`オブジェクトジェネレータの直接挿入を可能にします。挿入ペイロードを処理しないため、非常に高性能です。このメソッドは設定および挿入フォーマットの指定にオプションを提供します：

| パラメータ       | タイプ                                   | デフォルト    | 説明                                                                                       |
|--------------|----------------------------------------|------------|-------------------------------------------------------------------------------------------|
| table        | str                                    | *必須*     | 単一のテーブル名またはデータベース修飾されたテーブル名                                    |
| column_names | Sequence[str]                          | *None*     | 挿入ブロックの列名。`fmt`から名前が含まれていない場合は必須                               |
| insert_block | str, bytes, Generator[bytes], BinaryIO | *必須*     | 挿入するデータ。文字列はクライアントのエンコーディングでエンコードされます。                           |
| settings     | dict                                   | *None*     | [settings description](#settings-argument)を参照してください。                            |                                                                                                                                                |
| fmt          | str                                    | *None*     | `insert_block` bytesのClickHouse入力フォーマット。（指定されない場合、ClickHouseはTSVを使用します） |

の呼び出し側の責任は、`insert_block`が指定されたフォーマットであることと指定された圧縮方法を使用していることです。ClickHouse ConnectはファイルのアップロードやPyArrowテーブルにこれらのraw insertsを使用し、解析をClickHouseサーバーに委ねます。

### ユーティリティクラスと機能

以下のクラスと関数も"パブリック"の`clickhouse-connect` APIの一部と見なされ、上記のクラスとメソッドと同様に、マイナリリース間で安定しています。これらのクラスと関数の重大な変更は、マイナ（ではなくパッチ）リリースでのみ発生し、少なくとも1つのマイナリリースで廃止ステータスのままで使用可能です。

#### 例外

すべてのカスタム例外（DB API 2.0仕様で定義されているものを含む）は、`clickhouse_connect.driver.exceptions`モジュールに定義されています。
ドライバが実際に検出した例外はこれらのタイプを使用します。

#### Clickhouse SQLユーティリティ

`clickhouse_connect.driver.binding`モジュールの関数およびDT64Paramクラスは、ClickHouse SQLクエリを適切に構築し、エスケープするために使用できます。同様に、`clickhouse_connect.driver.parser`モジュールの関数は、ClickHouseデータ型の名前を解析するために使用できます。

### マルチスレッド、マルチプロセス、および非同期/イベント駆動型の使用例

ClickHouse Connectは、マルチスレッド、マルチプロセス、およびイベントループ駆動/非同期アプリケーションでうまく機能します。すべてのクエリおよび挿入処理は単一スレッド内で行われるため、これらの操作は一般にスレッドセーフです。
（いくつかの操作を低レベルで並列処理することは、単一スレッドのパフォーマンスペナルティを克服するための今後の改良可能性ですが、それでもスレッドセーフは維持されます。）

各クエリまたは挿入が、それぞれ独自のQueryContextまたはInsertContextオブジェクト内で状態を維持するため、これらのヘルパーオブジェクトはスレッドセーフではなく、複数の処理ストリーム間で共有してはなりません。
以下のセクションでは、コンテキストオブジェクトについての追加の議論が続きます。

さらに、同時に複数のクエリやインサートが「飛行中」になるアプリケーションの場合、考慮すべき2つの要素があります。1つ目は、クエリ/インサートに関連するClickHouseの「セッション」であり、2つ目はClickHouse Connect Clientインスタンスによって使用されるHTTP接続プールです。

### AsyncClientラッパー

バージョン0.7.16以降、ClickHouse Connectは通常の`Client`の上に非同期ラッパーを提供しており、これにより`asyncio`環境でクライアントを使用することが可能です。

`AsyncClient`のインスタンスを取得するには、標準の`get_client`と同じパラメータを受け取る`get_async_client`ファクトリ関数を使用できます：

```python
import asyncio

import clickhouse_connect


async def main():
    client = await clickhouse_connect.get_async_client()
    result = await client.query("SELECT name FROM system.databases LIMIT 1")
    print(result.result_rows)


asyncio.run(main())
```

`AsyncClient`は、標準の`Client`と同じメソッドを同じパラメータで持っていますが、適用可能な場合はコルーチンとして動作します。内部的には、I/O操作を行う`Client`のこれらのメソッドは、[run_in_executor](https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_in_executor)呼び出しでラップされています。

`AsyncClient`ラッパーを使用すると、I/O操作の完了を待つ間、実行スレッドとGILが解放されるため、マルチスレッドの性能が向上します。

注意：通常の`Client`と異なり、`AsyncClient`はデフォルトで`autogenerate_session_id`を`False`に強制します。

関連リンク：[run_async example](https://github.com/ClickHouse/clickhouse-connect/blob/main/examples/run_async.py).

### ClickHouseセッションIDの管理

各ClickHouseクエリは、ClickHouseの「セッション」のコンテキスト内で実行されます。セッションは現在2つの目的で使用されます：
- 複数のクエリに特定のClickHouse設定を関連付けるため（[ユーザー設定](/docs/ja/operations/settings/settings.md)を参照）。ClickHouseの`SET`コマンドを使用してユーザーセッションのスコープにおける設定を変更します。
- [一時テーブル](https://clickhouse.com/docs/ja/sql-reference/statements/create/table#temporary-tables)を追跡するため。

デフォルトでは、ClickHouse Connect Clientインスタンスを使用して実行される各クエリは、このセッション機能を有効にするために同じセッションIDを使用します。つまり、`SET`ステートメントと一時テーブルは、単一のClickHouseクライアントを使用する際に期待通りに機能します。しかし、設計上、ClickHouseサーバーは同じセッション内での同時クエリを許可しません。その結果、同時クエリを実行するClickHouse Connectアプリケーションには2つのオプションがあります。

- 実行の各スレッド（スレッド、プロセス、またはイベントハンドラ）に対して個別の`Client`インスタンスを作成し、自身のセッションIDを持たせます。これが最良のアプローチであり、各クライアントのセッション状態を保持します。
- 各クエリにユニークなセッションIDを使用します。この方法は、一時テーブルや共有セッション設定が不要な状況で同時セッション問題を回避します。（共有設定はクライアント作成時にも提供できますが、これらは各リクエストで送信され、セッションに関連付けられません）。ユニークなsession_idは各リクエストの`settings`ディクショナリに追加するか、`autogenerate_session_id`共通設定を無効にすることができます：

```python
from clickhouse_connect import common

common.set_setting('autogenerate_session_id', False)  # これはクライアントを作成する前に常に設定する必要があります。
client = clickhouse_connect.get_client(host='somehost.com', user='dbuser', password=1234)
```

この場合、ClickHouse ConnectはセッションIDを送信せず、ClickHouseサーバーによってランダムなセッションIDが生成されます。再度、一時テーブルとセッションレベルの設定は利用できません。

### HTTP接続プールのカスタマイズ

ClickHouse Connectは、`urllib3`接続プールを使用してサーバーへの基礎的なHTTP接続を処理します。デフォルトでは、すべてのクライアントインスタンスは同じ接続プールを共有しており、これはほとんどの使用例で十分です。このデフォルトプールは、アプリケーションで使用される各ClickHouseサーバーに最大8つのHTTP Keep Alive接続を維持します。

大規模なマルチスレッドアプリケーションでは、個別の接続プールが適切である場合があります。カスタマイズされた接続プールは、メインの`clickhouse_connect.get_client`関数への`pool_mgr`キーワード引数として提供できます：

```python
import clickhouse_connect
from clickhouse_connect.driver import httputil

big_pool_mgr = httputil.get_pool_manager(maxsize=16, num_pools=12)

client1 = clickhouse_connect.get_client(pool_mgr=big_pool_mgr)
client2 = clickhouse_connect.get_client(pool_mgr=big_pool_mgr)
```

上記の例が示すように、クライアントはプールマネージャを共有することができ、各クライアント用に別々のプールマネージャを作成することもできます。PoolManagerを作成する際のオプションについて詳細を知りたい場合は、[urllib3 ドキュメンテーション](https://urllib3.readthedocs.io/en/stable/advanced-usage.html#customizing-pool-behavior)を参照してください。

## ClickHouse Connectを使ったデータのクエリ：上級利用法

### QueryContexts

ClickHouse Connectは標準のクエリをQueryContext内で実行します。QueryContextには、ClickHouseデータベースに対するクエリを構築するために使用される主要な構造と、QueryResultや他の応答データ構造に結果を処理するために使用される設定が含まれています。これには、クエリそのもの、パラメータ、設定、読み取りフォーマット、その他のプロパティが含まれます。

QueryContextは、クライアントの`create_query_context`メソッドを使用して取得できます。このメソッドはコアクエリメソッドと同じパラメータを取ります。このクエリコンテキストは、`query`、`query_df`、または`query_np`メソッドに`context`キーワード引数として渡すことができ、これらのメソッドへの他の引数をすべてもしくは一部の代わりとして渡すことができます。メソッド呼び出しのために指定された追加の引数は、QueryContextのプロパティを上書きします。

最も明確なQueryContextの使用例は、異なるバインディングパラメータ値で同じクエリを送信することです。すべてのパラメータ値はディクショナリを使用して`QueryContext.set_parameters`メソッドを呼び出すことによって更新でき、`QueryContext.set_parameter`を使用して好きな`key`、`value`ペアを更新することもできます。

```python
client.create_query_context(query='SELECT value1, value2 FROM data_table WHERE key = {k:Int32}',
                            parameters={'k': 2},
                            column_oriented=True)
result = client.query(context=qc)
assert result.result_set[1][0] == 'second_value2'
qc.set_parameter('k', 1)
result = test_client.query(context=qc)
assert result.result_set[1][0] == 'first_value2'
```

QueryContextsはスレッドセーフではないことに注意してくださいが、`QueryContext.updated_copy`メソッドを呼び出すことによってマルチスレッド環境でコピーを取得することができます。

### ストリーミングクエリ

#### データブロック
ClickHouse Connectは、主要な`query`メソッドから取得するすべてのデータを、ClickHouseサーバーから受信したブロックのストリームとして処理します。これらのブロックは、ClickHouse独自の「ネイティブ」フォーマットで送受信されます。「ブロック」とは、単にバイナリデータのカラム列のシーケンスであり、各カラムは指定されたデータ型のデータ値を等しく含みます。（列指向データベースであるClickHouseは、このデータを同様の形式で保存します。）クエリから返されるブロックのサイズは、いくつかのレベル（ユーザープロファイル、ユーザー、セッション、またはクエリ）で設定できる2つのユーザー設定によって決まります。それらは次の通りです：

- [max_block_size](/docs/ja/operations/settings/settings.md/#setting-max_block_size) -- ブロックの行数の制限。デフォルトは65536です。
- [preferred_block_size_bytes](/docs/ja/operations/settings/settings.md/#preferred-block-size-bytes) -- ブロックのサイズのバイト数のソフト制限。デフォルトは1,000,000です。

`preferred_block_size_setting`に関わらず、各ブロックは`max_block_size`行を超えることはありません。クエリの種類によっては、返される実際のブロックのサイズは任意です。たとえば、多くのシャードをカバーする分散テーブルへのクエリは、各シャードから直接取得される小さなブロックを含むことがあります。

クライアントの`query_*_stream`メソッドの1つを使用する場合、結果はブロックごとに返されます。ClickHouse Connectは常に1つのブロックのみを読み込みます。これにより、大量のデータをメモリにすべて読み込むことなく処理することができます。アプリケーションは任意の数のブロックを処理する準備ができているはずで、各ブロックの正確なサイズは制御できません。

#### 遅延処理のためのHTTPデータバッファ

HTTPプロトコルの制約により、ブロックがClickHouseサーバーがデータをストリーミングする速度よりも著しく遅い速度で処理されると、ClickHouseサーバーは接続を閉じ、処理スレッドで例外がスローされます。これをある程度緩和するためには、HTTPストリーミングバッファのバッファサイズを増やすことができます（デフォルトは10メガバイト）`http_buffer_size`設定を使用します。この状況で十分なメモリがアプリケーション利用可能であれば、大きな`http_buffer_size`値は問題ありません。バッファ内のデータは`lz4`や`zstd`圧縮を使用している場合は圧縮されて保存されるため、これらの圧縮タイプを使用すると利用可能なバッファ全体が増えます。

#### StreamContexts

各`query_*_stream`メソッド（例えば`query_row_block_stream`）は、ClickHouseの`StreamContext`オブジェクトを返します。これはPythonのコンテキスト/ジェネレータを組み合わせたものです。基本的な使用法は以下の通りです：

```python
with client.query_row_block_stream('SELECT pickup, dropoff, pickup_longitude, pickup_latitude FROM taxi_trips') as stream:
    for block in stream:
        for row in block:
            <do something with each row of Python trip data>
```

この例では、`StreamContext`を`with`文なしで使用しようとするとエラーが発生します。Pythonコンテキストを使用することで、すべてのデータが消費されなかった場合や、処理中に例外が発生した場合でもストリーム（この場合はストリーミングHTTPレスポンス）が適切に閉じられることが保証されます。さらに、`StreamContext`はストリームを消費するために一度だけ使用できます。`StreamContext`が終了した後に使用しようとすると、`StreamClosedError`が発生します。

StreamContextの`source`プロパティを使用して、親`QueryResult`オブジェクトにアクセスすることができ、そこにはカラム名やタイプが含まれています。

#### ストリームタイプ

`query_column_block_stream`メソッドは、ネイティブのPythonデータタイプとして保存されたカラムデータのシーケンスとしてブロックを返します。上記の`taxi_trips`クエリを使用すると、返されるデータはリストで、リストの各要素は関連するカラムのすべてのデータを含む別のリスト（またはタプル）になります。したがって、`block[0]`は文字列だけを含むタプルとなります。列指向フォーマットは、カラム中のすべての値を集計操作する際に最も多く使用されます。

`query_row_block_stream`メソッドは、従来のリレーショナルデータベースのように行ごとのシーケンスとしてブロックを返します。タクシートリップの場合、返されるデータはリストで、リストの各要素はデータ行を表す別のリストです。したがって、`block[0]`は最初のタクシートリップのすべてのフィールド（順番に）を含み、`block[1]`は2番目のタクシートリップのすべてのフィールドを含む行となります。行指向の結果は通常、表示や変換プロセスに使用されます。

`query_row_stream`は便利なメソッドで、ストリームを通してiteratingする際に自動的に次のブロックに移動します。それ以外は`query_row_block_stream`と同じです。

`query_np_stream`メソッドは、各ブロックを2次元Numpy配列として返します。内部的にNumpy配列は(通常)カラムとして保存されているため、特定の行またはカラムメソッドは必要ありません。Numpy配列の「形状」は(columns, rows)として表現されます。NumpyライブラリはNumpy配列を操作するための多くのメソッドを提供します。なお、クエリ中のすべてのカラムが同じNumpy dtypeを共有している場合、返されるNumpy配列も1つのdtypeだけを持ち、内部構造を変更することなくreshape/rotateできることができます。

`query_df_stream`メソッドは、各ClickHouseブロックを2次元のPandas Dataframeとして返します。この例では、StreamContextオブジェクトが延期された形式でコンテキストとして使用できることを示していますが（ただし一度だけ）

最後に、`query_arrow_stream`メソッドはClickHouse`ArrowStream`形式の結果をpyarrow.ipc.RecordBatchStreamReaderとして返し、StreamContextでラップします。各ストリームの反復はPyArrow RecordBlockを返します。

```python
df_stream = client.query_df_stream('SELECT * FROM hits')
column_names = df_stream.source.column_names
with df_stream:
    for df in df_stream:
        <do something with the pandas DataFrame>
```

### 読取りフォーマット

読み取りフォーマットは、クライアントの`query`、`query_np`、および`query_df`メソッドから返される値のデータタイプを制御します。（`raw_query`および`query_arrow`はClickHouseからのデータを変更しないため、フォーマット制御は適用されません。）たとえば、UUIDを`native`フォーマットから`string`フォーマットに変更すると、ClickHouseの`UUID`カラムのクエリは標準の8-4-4-4-12RFC1422形式で文字列値として返されます。

データタイプのフォーマットとしては、ワイルドカードを含むことができます。フォーマットは1つの小文字の文字列です。

読み取りフォーマットは、複数のレベルで設定できます：

- グローバルに、`clickhouse_connect.datatypes.format`パッケージ内で定義されているメソッドを使用して。これにより、構成されたデータタイプのフォーマットがすべてのクエリで制御されます。
```python
from clickhouse_connect.datatypes.format import set_read_format

# IPv6およびIPv4の両方の値を文字列として返します
set_read_format('IPv*', 'string')

# すべてのDateタイプを基礎的なエポック秒またはエポック日として返します
set_read_format('Date*', 'int')
```
- クエリ全体、オプションの`query_formats`ディクショナリアーギュメントを使用して。この場合、特定のデータタイプ（またはサブカラム）のカラムごとに設定されたフォーマットが使用されます。
```python
# 任意のUUIDカラムを文字列として返します
client.query('SELECT user_id, user_uuid, device_uuid from users', query_formats={'UUID': 'string'})
```
- 特定のカラムに含まれる値の場合、オプションの`column_formats`ディクショナリアーギュメントを使用して。キーはClickHouseによって返されるカラム名で、フォーマットはデータカラムに対してのものであり、またはClickHouseのタイプ名とクエリフォーマットの値である2次レベルの「フォーマット」ディクショナリです。この2次ディクショナリは、タプルやマップなどのネストされたカラムタイプに使用できます。
```python
# dev_addressカラムのIPv6値を文字列として返します
client.query('SELECT device_id, dev_address, gw_address from devices', column_formats={'dev_address':'string'})
```

#### 読み取りフォーマットオプション（Pythonタイプ）

| ClickHouse Type       | Native Python Type    | Read Formats | Comments                                                                                                          |
|-----------------------|-----------------------|--------------|-------------------------------------------------------------------------------------------------------------------|
| Int[8-64], UInt[8-32] | int                   | -            |                                                                                                                   |
| UInt64                | int                   | signed       | Supersetは現在大きな符号なしUInt64値を処理できません                                                          |
| [U]Int[128,256]       | int                   | string       | PandasとNumpyのint値は最大64ビットであるため、これらは文字列として返すことができます                          |
| Float32               | float                 | -            | すべてのPythonのfloatは内部で64ビットです                                                                          |
| Float64               | float                 | -            |                                                                                                                   |
| Decimal               | decimal.Decimal       | -            |                                                                                                                   |
| String                | string                | bytes        | ClickHouseのStringカラムは固有のエンコーディングを持っていないため、可変長のバイナリデータにも使用されます       |
| FixedString           | bytes                 | string       | FixedStringは固定サイズのバイト配列ですが、時にはPythonの文字列として扱われることもあります                       |
| Enum[8,16]            | string                | string, int  | Pythonのenumは空の文字列を許容しないため、すべてのenumは文字列か基本のint値としてレンダリングされます              |
| Date                  | datetime.date         | int          | ClickHouseはDateを1970年1月1日からの日数として保存します。この値はintとして提供されます                        |
| Date32                | datetime.date         | int          | Dateと同じですが、より広い日付の範囲を持ちます                                                                   |
| DateTime              | datetime.datetime     | int          | ClickHouseはDateTimeをエポック秒で保存します。この値はintとして提供されます                                        |
| DateTime64            | datetime.datetime     | int          | Pythonのdatetime.datetimeはマイクロ秒精度に制限されています。生の64ビットのint値が利用可能です                   |
| IPv4                  | ipaddress.IPv4Address | string       | IPアドレスは文字列として読み込まれ、適切にフォーマットされた文字列がIPアドレスとして挿入されることができます       |
| IPv6                  | ipaddress.IPv6Address | string       | IPアドレスは文字列として読み込まれ、適切にフォーマットされた文字列がIPアドレスとして挿入されることができます       |
| Tuple                 | dict or tuple         | tuple, json  | 名前付きタプルはデフォルトでDictionaryとして返されます。また、名前付きタプルはJSON文字列としても返されます              |
| Map                   | dict                  | -            |                                                                                                                   |
| Nested                | Sequence[dict]        | -            |                                                                                                                   |
| UUID                  | uuid.UUID             | string       | UUIDはRFC 4122の標準形式を使用して文字列として読み込まれることができます                                          |
| JSON                  | dict                  | string       | デフォルトではPythonのDictionaryが返されます。`string`フォーマットではJSON文字列が返されます                        |
| Variant               | object                | -            | 保存されている値のClickHouseデータタイプに一致するPythonタイプが返されます                                         |
| Dynamic               | object                | -            | 保存されている値のClickHouseデータタイプに一致するPythonタイプが返されます                                         |


### 外部データ

ClickHouseのクエリは、任意のClickHouseフォーマットで外部データを受け入れることができます。このバイナリデータはクエリ文字列と共に送信され、データを処理するために使用されます。外部データ機能の詳細は[こちら](/docs/ja/engines/table-engines/special/external-data.md)です。クライアントの`query*`メソッドは、この機能を利用するためにオプションの`external_data`パラメータを受け入れます。`external_data`パラメータの値は`clickhouse_connect.driver.external.ExternalData`オブジェクトである必要があります。このオブジェクトのコンストラクタは、以下の引数を受け入れます：

| Name      | Type              | Description                                                                                                                                     |
|-----------|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| file_path | str               | 外部データを読み込むためのローカルシステムパスのファイルパス。`file_path`または`data`のいずれかが必要です                                | 
| file_name | str               | 外部データ「ファイル」の名前。提供されない場合は、（拡張子なしの）`file_path`から決定されます                                 |
| data      | bytes             | バイナリ形式の外部データ（ファイルから読み込む代わりに）。`data`または`file_path`のいずれかが必要です                               |
| fmt       | str               | データのClickHouse [入力フォーマット](/docs/ja/sql-reference/formats.mdx)。デフォルトは`TSV`                                                 |
| types     | str or seq of str | 外部データのカラムデータタイプのリスト。文字列の場合、型はコンマで区切られるべきです。`types`または`structure`のいずれかが必要です |
| structure | str or seq of str | データのカラム名+データタイプのリスト（例を参照）。`structure`または`types`のいずれかが必要です                                          |
| mime_type | str               | ファイルデータのオプションのMIMEタイプ。現在、ClickHouseはこのHTTPサブヘッダを無視します                                                      |


外部CSVファイルを含む「映画」データをクエリに送信し、ClickHouseサーバー上に既に存在する`directors`テーブルとそのデータを結合する：

```python
import clickhouse_connect
from clickhouse_connect.driver.external import ExternalData

client = clickhouse_connect.get_client()
ext_data = ExternalData(file_path='/data/movies.csv',
                        fmt='CSV',
                        structure=['movie String', 'year UInt16', 'rating Decimal32(3)', 'director String'])
result = client.query('SELECT name, avg(rating) FROM directors INNER JOIN movies ON directors.name = movies.director GROUP BY directors.name',
                      external_data=ext_data).result_rows
```

追加の外部データファイルは、コンストラクタと同じパラメータを取る`add_file`メソッドを使用して初期ExternalDataオブジェクトに追加できます。HTTPの場合、すべての外部データは`multi-part/form-data`ファイルアップロードの一部として送信されます。

### タイムゾーン
ClickHouse DateTimeおよびDateTime64の値には複数の方法でタイムゾーンを適用できます。内部的に、ClickHouseサーバーは常に任意のDateTimeまたはDateTime64オブジェクトをタイムゾーンナイーブな数値、つまり1970-01-01 00:00:00 UTC時間からの秒数として保存します。DateTime64値の場合、表現は精度に依存してエポックからのミリ秒、マイクロ秒、またはナノ秒です。その結果、いかなるタイムゾーン情報の適用も常にクライアント側で行われます。これには有意義な追加計算が伴うため、パフォーマンスが重要なアプリケーションでは、DateTimeタイプをエポックタイムスタンプとして扱うことをお勧めします。ユーザー表示や変換を除き、Pandas TimestampなどのPandasデータ型はパフォーマンスを向上させるために常にエポックナノ秒を表す64ビット整数となるためです。

クエリでタイムゾーンアウェアなデータタイプ、特にPythonの`datetime.datetime`オブジェクトを使用する際、`clickhouse-connect`はクライアント側タイムゾーンを以下の優先順位ルールに従って適用します：

1. クエリメソッドパラメータ`client_tzs`がクエリに指定されている場合、その特定のカラムタイムゾーンが適用されます。
2. ClickHouseカラムにタイムゾーンメタデータが含まれている場合（例えば、DateTime64(3, 'America/Denver')のようなタイプ）、ClickHouseカラムタイムゾーンが適用されます。（このタイムゾーンメタデータはClickHouseバージョン23.2以前のDateTimeカラムには利用できません）
3. クエリメソッドパラメータ`query_tz`がクエリに指定されている場合、「クエリタイムゾーン」が適用されます。
4. クエリまたはセッションにタイムゾーン設定が適用されている場合、そのタイムゾーンが適用されます。（この機能はClickHouseサーバーでまだリリースされていません）
5. 最後に、クライアントの`apply_server_timezone`パラメータがTrue（デフォルトの設定）に設定されている場合、ClickHouseサーバータイムゾーンが適用されます。

これらのルールに基づいて適用されたタイムゾーンがUTCである場合、`clickhouse-connect`は常にタイムゾーンナイーブなPython`datetime.datetime`オブジェクトを返します。このタイムゾーンナイーブなオブジェクトには、必要に応じてアプリケーションコードで追加のタイムゾーン情報を加えることができます。

## ClickHouse Connectを使ったデータのインサート：上級利用法

### InsertContexts

ClickHouse ConnectはすべてのインサートをInsertContext内で実行します。InsertContextには、クライアントの`insert`メソッドに引数として渡されたすべての値が含まれています。さらに、InsertContextが最初に構築されたとき、ClickHouse Connectはネイティブフォーマットの効率的なインサートのために必要なインサートカラムのデータタイプを取得します。このInsertContextを複数のインサートで再使用することにより、この「事前クエリ」を回避し、インサートをより迅速かつ効率的に実行します。

InsertContextは、クライアントの`create_insert_context`メソッドを使用して取得できます。このメソッドは、`insert`関数と同じ引数を受け取ります。再利用のために修正すべきはInsertContextsの`data`プロパティのみであることに注意してください。これによって、同じテーブルに新しいデータを繰り返しインサートするための再利用可能なオブジェクトが提供されることと一致しています。

```python
test_data = [[1, 'v1', 'v2'], [2, 'v3', 'v4']]
ic = test_client.create_insert_context(table='test_table', data='test_data')
client.insert(context=ic)
assert client.command('SELECT count() FROM test_table') == 2
new_data = [[3, 'v5', 'v6'], [4, 'v7', 'v8']]
ic.data = new_data
client.insert(context=ic)
qr = test_client.query('SELECT * FROM test_table ORDER BY key DESC')
assert qr.row_count == 4
assert qr[0][0] == 4
```

InsertContextsはインサートプロセス中に更新される可変の状態を持つため、スレッドセーフではありません。

### 書き込みフォーマット

書き込みフォーマットは、現在限られた数のタイプに対して実装されています。ほとんどの場合、ClickHouse Connectは最初の（nullでない）データ値の型をチェックすることによって、カラムに対して適切な書き込みフォーマットを自動的に判断しようとします。たとえば、DateTimeカラムに挿入する場合、カラムの最初のインサート値がPython整数である場合、ClickHouse Connectはそれが実際にはエポック秒であると仮定して整数値を直接挿入します。

ほとんどのケースで、データタイプの書き込みフォーマットを上書きする必要はありませんが、`clickhouse_connect.datatypes.format`パッケージの関連メソッドを使用してグローバルレベルで行うことができます。

#### 書き込みフォーマットオプション

| ClickHouse Type       | Native Python Type    | Write Formats | Comments                                                                                                    |
|-----------------------|-----------------------|---------------|-------------------------------------------------------------------------------------------------------------|
| Int[8-64], UInt[8-32] | int                   | -             |                                                                                                             |
| UInt64                | int                   |               |                                                                                                             |
| [U]Int[128,256]       | int                   |               |                                                                                                             |
| Float32               | float                 |               |                                                                                                             |
| Float64               | float                 |               |                                                                                                             |
| Decimal               | decimal.Decimal       |               |                                                                                                             |
| String                | string                |               |                                                                                                             |
| FixedString           | bytes                 | string        | 文字列として挿入された場合、追加のバイトはゼロに設定されます                                                   |
| Enum[8,16]            | string                |               |                                                                                                             |
| Date                  | datetime.date         | int           | ClickHouseは日付を1970年1月1日からの「エポック日」値として保存します。整数型はこれを仮定します              |
| Date32                | datetime.date         | int           | 日付と同じですが、より広い日付の範囲を持ちます                                                               |
| DateTime              | datetime.datetime     | int           | ClickHouseはDateTimeをエポック秒で保存します。int型はこの「エポック秒」値と仮定されます                      |
| DateTime64            | datetime.datetime     | int           | Pythonのdatetime.datetimeはマイクロ秒の精度に制限されています。生の64ビットint値が利用可能です               |
| IPv4                  | ipaddress.IPv4Address | string        | 正しくフォーマットされた文字列はIPv4アドレスとして挿入されることができます                                               |
| IPv6                  | ipaddress.IPv6Address | string        | 正しくフォーマットされた文字列はIPv6アドレスとして挿入されることができます                                               |
| Tuple                 | dict or tuple         |               |                                                                                                            |
| Map                   | dict                  |               |                                                                                                            |
| Nested                | Sequence[dict]        |               |                                                                                                            |
| UUID                  | uuid.UUID             | string        | 正しくフォーマットされた文字列はClickHouse UUIDとして挿入されることができます                                           |
| JSON/Object('json')   | dict                  | string        | DictionaryまたはJSON文字列をJSONカラムに挿入することができます（注意： `Object('json')` は非推奨です）             |
| Variant               | object                |               | 現時点ではすべてのバリアントが文字列として挿入され、ClickHouseサーバーによって解析されます                    |
| Dynamic               | object                |               | 警告 - 現時点では、Dynamicカラムへの挿入はClickHouse文字列として永続化されます                                   |


## 追加オプション

ClickHouse Connectは、上級のユースケースのためにいくつかの追加オプションを提供します

### グローバル設定

ClickHouse Connectの動作をグローバルに制御する設定が少数あります。それらはトップレベルの`common`パッケージからアクセス可能です：

```python
from clickhouse_connect import common

common.set_setting('autogenerate_session_id', False)
common.get_setting('invalid_setting_action')
'drop'
```

:::note
これらの共通設定 `autogenerate_session_id`, `product_name`, および `readonly` は、常にクライアントを作成する前に
`clickhouse_connect.get_client` メソッドを使用して変更する必要があります。クライアント作成後にこれらの設定を変更しても
既存のクライアントの動作に影響を及ぼしません。
:::

現在定義されているグローバル設定は10個です：

| Setting Name            | Default | Options                 | Description                                                                                                                                                                                                                                                   |
|-------------------------|---------|-------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| autogenerate_session_id | True    | True, False             | 各クライアントセッションのために新しいUUID(1)セッションIDを自動生成（指定されていない場合）。クライアントまたはクエリレベルでセッションIDが提供されない場合（ClickHouseは内部ランダムIDを各クエリに生成します）                                                     |
| invalid_setting_action  | 'error' | 'drop', 'send', 'error' | 無効または読み取り専用の設定が提供された場合のアクション（クライアントセッションまたはクエリのいずれか）。`drop`の場合は設定が無視され、`send`の場合は設定がClickHouseへ送信され、`error`の場合はクライアント側でProgrammingErrorが発生します                         |
| dict_parameter_format   | 'json'  | 'json', 'map'           | これはパラメータ化を行ったクエリがPythonのDictionaryをJSONまたはClickHouseのMap構文に変換するかどうかを制御します。JSONカラムへの挿入には`json`、ClickHouseのMapカラム用には`map`を使用します                                                                     |
| product_name            |         |                         | クエリと共にClickHouseに渡される文字列で、ClickHouse Connectを使用しているアプリをトラッキングするために使用されます。形式&gl/;product name;&gl;&lt;product version&gt;であるべきです                                                   |
| max_connection_age      | 600     |                         | HTTP Keep Alive接続がオープン/再利用され続ける最大の秒数。ロードバランサ/プロキシの背後の単一のClickHouseノードへの接続の集まりを防ぎます（デフォルト10分です）。                                                                                                               |
| readonly                | 0       | 0, 1                    | ClickHouseバージョン19.17以前の操作を許可するために"read_only"設定と一致するよう設定可能。非常に古いClickHouseバージョンでの操作を許可するために使用。                                                                                                        |
| use_protocol_version    | True    | True, False             | クライアントプロトコルバージョンを使用します。 これはDateTimeタイムゾーンカラムに必要ですが、chproxyの現在のバージョンでは動作しません。                                                                                                              |
| max_error_size          | 1024    |                         | クライアントエラーメッセージに返される最大文字数。ClickHouseのエラーメッセージ全体を取得する設定としては0を使用します。デフォルトは1024文字。                                                                                                                       |
| send_os_user            | True    | True, False             | クライアント情報でClickHouseに送信されるオペレーティングシステムユーザー（HTTP User-Agent文字列）を含める。                                                                                                                                               |
| http_buffer_size        | 10MB    |                         | HTTPストリーミングクエリに使用される「インメモリ」バッファのサイズ（バイト単位）                                                                                                                                                                                   |

### 圧縮

ClickHouse Connectは、クエリ結果とインサートの両方に対してlz4、zstd、brotli、およびgzip圧縮をサポートしています。圧縮を使用することは、多くの場合、ネットワーク帯域幅/転送速度とCPU使用量（クライアントとサーバーの両方）の間のトレードオフがあることを常に覚えておいてください。

圧縮データを受信するには、ClickHouseサーバーの`enable_http_compression`を1に設定するか、ユーザーが「クエリごと」に設定を変更する権限を持っている必要があります。

圧縮は、`clickhouse_connect.get_client`ファクトリメソッドを呼び出す場合の`compress`パラメータで制御されます。デフォルトでは`compress`は `True` に設定されており、これによりデフォルトの圧縮設定がトリガーされます。`query`、`query_np`、および`query_df`クライアントメソッドを使用して実行されるクエリでは、ClickHouse Connectは`query`クライアントメソッドを使用して実行されるクエリに対して、`Accept-Encoding`ヘッダーを`lz4`、 `zstd`、`br`（brotliライブラリがインストールされている場合）、`gzip` および`deflate`エンコーディングとともにクエリを実行します。（クリックハウスサーバーの大多数のリクエストでは、`zstd`圧縮ペイロードが返されます。）インサートの場合、デフォルトでClickHouse Connectはlz4圧縮を使ってインサートブロックを圧縮し、`Content-Encoding： lz4` HTTP ヘッダーを送信します。

`get_client`の`compress`パラメータは、特定の圧縮メソッド、`lz4`、`zstd`、`br` または`gzip`のいずれかに設定することもできます。そのメソッドは、インサートとクエリ結果（クライアントサーバーによってサポートされている場合）の両方で使用されます。`zstd`および`lz4`圧縮ライブラリは、現在ClickHouse Connectとともにデフォルトでインストールされています。`br`/brotliが指定されている場合、brotliライブラリは別途インストールする必要があります。

`raw*`クライアントメソッドは、クライアント設定によって指定された圧縮を使用しないことに注意してください。

また、`gzip`圧縮の使用を推奨しません。データの圧縮と解凍の両方で代替手段と比較してかなり遅いためです。

### HTTPプロキシサポート

ClickHouse Connect では、urllib3ライブラリを使用した基本的なHTTPプロキシサポートが追加されています。標準的な`HTTP_PROXY`と`HTTPS_PROXY`環境変数を認識します。これらの環境変数を使用することで、`clickhouse_connect.get_client`メソッドを使用して作成されるすべてのクライアントになります。または、クライアントごとに設定するために、get_clientメソッドの`http_proxy`や`https_proxy`引数を使用することができます。詳細なHTTPプロキシサポートの実装については、[urllib3](https://urllib3.readthedocs.io/en/stable/advanced-usage.html#http-and-https-proxies)
のドキュメントを参照してください。

Socksプロキシを使用するには、urllib3 SOCKSProxyManagerを`pool_mgr`引数として`get_client`に送ることができます。これにはPySocksライブラリを直接インストールするか、urllib3の依存関係として`[socks]`オプションを使用する必要があります。

### 古いJSONデータタイプ

エクスペリメンタルな`Object`（または`Object('json')`）データタイプは非推奨とされており、製品化環境には避けるべきです。ClickHouse Connectはこのデータタイプに対する限定的なサポートを引き続き提供しており、過去との互換性のためのものです。このサポートには、ディクショナリまたはそれに相当する「トップレベル」または「親」JSON値を返すと想定されるクエリは含まれておらず、そのようなクエリは例外を発生させます。

### 新しい Variant/Dynamic/JSON データタイプ(エクスペリメンタル機能)

バージョン0.8.0のリリースから、クリックハウスコネクトは新しい（まだエクスペリメンタルな）ClickHouseタイプVariant、Dynamic、およびJSONに対するエクスペリメンタルサポートを提供しています。

#### 使用上の注意事項
- JSONデータは、PythonDictionaryまたはJSON文字列（例えば`{}`）として挿入できます。他の形式のJSONデータはサポートされていません。
- これらのタイプのサブカラム/パスを使用したクエリは、サブカラムの型を返します。
- 他の使用上の注意については、メインのClickHouseドキュメントを参照してください。

#### 既知の制限:
- これらのタイプを使用する前に、ClickHouseの設定で有効化する必要があります。
- 新しいJSONタイプはClickHouse 24.8リリースから利用可能です。
- 内部フォーマットの変更のため、クリックハウスコネクトはClickHouse 24.7リリース以降のVariantタイプとしか互換性がありません。
- 返されるJSONオブジェクトは最大で`max_dynamic_paths`の要素数しか返されません（デフォルトで1024）。これは将来のリリースで修正されます。
- `Dynamic`カラムへの挿入は常にPython値の文字列表現となります。これについては、https://github.com/ClickHouse/ClickHouse/issues/70395の修正後、将来のリリースで修正される予定です。
- 新しいタイプの実装はCコードでの最適化がされていないため、シンプルで確立されたデータタイプと比べて若干パフォーマンスが遅い可能性があります。
