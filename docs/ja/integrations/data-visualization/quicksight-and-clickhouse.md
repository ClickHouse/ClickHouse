---
sidebar_label: QuickSight
slug: /ja/integrations/quicksight
keywords: [clickhouse, aws, amazon, quicksight, mysql, connect, integrate, ui]
description: Amazon QuickSightは、ハイパースケールで統一されたビジネスインテリジェンス（BI）を提供するデータ駆動の組織をサポートします。
---

import MySQLOnPremiseSetup from '@site/docs/ja/_snippets/_clickhouse_mysql_on_premise_setup.mdx';

# QuickSight

QuickSightは、公式のMySQLデータソースおよびDirect Queryモードを使用して、MySQLインターフェースを介してオンプレミスのClickHouseセットアップ（23.11+）に接続できます。

## オンプレミスClickHouseサーバーの設定

MySQLインターフェースを有効にしたClickHouseサーバーの設定方法については、[公式ドキュメント](https://clickhouse.com/docs/ja/interfaces/mysql)を参照してください。

サーバーの`config.xml`にエントリを追加することに加えて

```xml
<clickhouse>
    <mysql_port>9004</mysql_port>
</clickhouse>
```

Double SHA1パスワード暗号化を使用することが_必須_です。ユーザーがMySQLインターフェースを使用する際の設定です。

シェルからDouble SHA1で暗号化されたランダムパスワードを生成するには:

```shell
PASSWORD=$(base64 < /dev/urandom | head -c16); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'
```

出力は次のようになります:

```
LZOQYnqQN4L/T6L0
fbc958cc745a82188a51f30de69eebfc67c40ee4
```

1行目が生成されたパスワードで、2行目がClickHouseを設定するために使用できるハッシュです。

以下は、生成されたハッシュを使用する`mysql_user`の設定例です:

`/etc/clickhouse-server/users.d/mysql_user.xml`

```xml
<users>
    <mysql_user>
        <password_double_sha1_hex>fbc958cc745a82188a51f30de69eebfc67c40ee4</password_double_sha1_hex>
        <networks>
            <ip>::/0</ip>
        </networks>
        <profile>default</profile>
        <quota>default</quota>
    </mysql_user>
</users>
```

`password_double_sha1_hex`のエントリを、自分で生成したDouble SHA1ハッシュに置き換えてください。

QuickSightは、MySQLユーザーのプロファイルにいくつかの追加設定が必要です。

`/etc/clickhouse-server/users.d/mysql_user.xml`

```xml
<profiles>
    <default>
        <prefer_column_name_to_alias>1</prefer_column_name_to_alias>
        <allow_experimental_analyzer>1</allow_experimental_analyzer>
        <mysql_map_string_to_text_in_show_columns>1</mysql_map_string_to_text_in_show_columns>
        <mysql_map_fixed_string_to_text_in_show_columns>1</mysql_map_fixed_string_to_text_in_show_columns>
    </default>
</profiles>
```

ただし、これをデフォルトのものではなく、MySQLユーザーが使用できる別のプロファイルに割り当てることをお勧めします。

最後に、ClickHouseサーバーを希望するIPアドレスでリッスンするように設定します。 `config.xml`で、次の部分のコメントを解除して、すべてのアドレスでリッスンするように設定します:

```bash
<listen_host>::</listen_host> 
```

`mysql`バイナリが利用可能な場合は、コマンドラインから接続をテストできます。 上記のサンプルユーザー名(`mysql_user`)とパスワード(`LZOQYnqQN4L/T6L0`)を使用してコマンドラインで次のようにします:

```bash
mysql --protocol tcp -h localhost -u mysql_user -P 9004 --password=LZOQYnqQN4L/T6L0
```

```
mysql> show databases;
+--------------------+
| name               |
+--------------------+
| INFORMATION_SCHEMA |
| default            |
| information_schema |
| system             |
+--------------------+
4 rows in set (0.00 sec)
Read 4 rows, 603.00 B in 0.00156 sec., 2564 rows/sec., 377.48 KiB/sec.
```

## QuickSightとClickHouseの接続

まず、https://quicksight.aws.amazon.com にアクセスし、[データセット] に移動して、「新しいデータセット」をクリックします。

<img src={require('./images/quicksight_01.png').default} class="image" alt="Creating a new dataset" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

QuickSightにバンドルされている公式のMySQLコネクタ（**MySQL**のみ）を検索します。

<img src={require('./images/quicksight_02.png').default} class="image" alt="MySQL connector search" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

接続の詳細を指定します。 MySQLインターフェースのポートはデフォルトで9004で、サーバー構成により異なる場合があります。

<img src={require('./images/quicksight_03.png').default} class="image" alt="Specifying the connection details" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

次に、ClickHouseからデータを取得する方法として2つのオプションがあります。 まず、リストからテーブルを選択する方法です。

<img src={require('./images/quicksight_04.png').default} class="image" alt="Selecting a table from the list" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

あるいは、カスタムSQLを指定してデータを取得することもできます。

<img src={require('./images/quicksight_05.png').default} class="image" alt="Using custom SQL to fetch the data" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

「データの編集/プレビュー」をクリックすると、イントロスペクトされたテーブル構造を確認するか、データを取得する方法としてカスタムSQLを使用する場合は調整を行うことができます。

<img src={require('./images/quicksight_06.png').default} class="image" alt="Viewing the introspected table structure" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

UIの左下隅で「Direct Query」モードが選択されていることを確認してください。

<img src={require('./images/quicksight_07.png').default} class="image" alt="Choosing the Direct Query mode" style={{width: '50%', 'background-color': 'transparent'}}/>  
<br/>

これでデータセットの公開と新しいビジュアル化の作成を進めることができます！

## 既知の制限事項

- SPICEインポートは期待通りに動作しません。Direct Queryモードを使用してください。[#58553](https://github.com/ClickHouse/ClickHouse/issues/58553)を参照してください。
