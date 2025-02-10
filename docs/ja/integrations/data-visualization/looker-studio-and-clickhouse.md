---
sidebar_label: Looker Studio
slug: /ja/integrations/lookerstudio
keywords: [clickhouse, looker, studio, connect, mysql, integrate, ui]
description: Looker Studio（以前のGoogleデータスタジオ）は、データをカスタマイズ可能な情報報告書やダッシュボードに変換するオンラインツールです。
---

import MySQLCloudSetup from '@site/docs/ja/_snippets/_clickhouse_mysql_cloud_setup.mdx';
import MySQLOnPremiseSetup from '@site/docs/ja/_snippets/_clickhouse_mysql_on_premise_setup.mdx';

# Looker Studio

Looker Studioは、公式のGoogle MySQLデータソースを使用して、MySQLインターフェース経由でClickHouseに接続できます。

## ClickHouse Cloudのセットアップ
<MySQLCloudSetup />

## オンプレミスClickHouseサーバーのセットアップ
<MySQLOnPremiseSetup />

## Looker StudioをClickHouseに接続する

まず、https://lookerstudio.google.com にGoogleアカウントでログインし、新しいデータソースを作成します。

<img src={require('./images/looker_studio_01.png').default} class="image" alt="新しいデータソースの作成" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

Googleが提供する公式のMySQLコネクタ（名称は単に**MySQL**）を検索します。

<img src={require('./images/looker_studio_02.png').default} class="image" alt="MySQLコネクタの検索" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

接続の詳細を指定します。MySQLインターフェースのポートはデフォルトで9004で、サーバーの設定によって異なる場合があります。

<img src={require('./images/looker_studio_03.png').default} class="image" alt="接続の詳細を指定" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

次に、ClickHouseからデータを取得する方法を2つ選ぶことができます。まず、テーブルブラウザ機能を使用することができます。

<img src={require('./images/looker_studio_04.png').default} class="image" alt="テーブルブラウザの使用" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

または、カスタムクエリを指定してデータを取得することもできます。

<img src={require('./images/looker_studio_05.png').default} class="image" alt="カスタムクエリを使用してデータを取得" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

最後に、イントロスペクトされたテーブル構造を確認し、必要に応じてデータ型を調整できます。

<img src={require('./images/looker_studio_06.png').default} class="image" alt="イントロスペクトされたテーブル構造の表示" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

これで、データの探査や新しいレポートの作成を進めることができます！

## ClickHouse CloudでLooker Studioを使用する

ClickHouse Cloudを使用する場合、最初にMySQLインターフェースを有効にする必要があります。接続ダイアログの「MySQL」タブでそれを行うことができます。

<img src={require('./images/looker_studio_enable_mysql.png').default} class="image" alt="MySQLの有効化が必要" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

Looker StudioのUIで「SSLを有効にする」オプションを選択します。ClickHouse CloudのSSL証明書は[Let's Encrypt](https://letsencrypt.org/certificates/)により署名されています。このルート証明書を[こちら](https://letsencrypt.org/certs/isrgrootx1.pem)からダウンロードできます。

<img src={require('./images/looker_studio_mysql_cloud.png').default} class="image" alt="ClickHouse CloudでのSSL設定" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

その後の手順は、前述のセクションに記載されている手順と同じです。
