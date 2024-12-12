---
sidebar_label: Retool
slug: /ja/integrations/retool
keywords: [clickhouse, retool, connect, integrate, ui, admin, panel, dashboard, nocode, no-code]
description: 豊富なユーザーインターフェースを備えたウェブやモバイルアプリをすばやく構築し、複雑なタスクを自動化し、AIを統合する—すべてデータによって支えられています。
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# RetoolをClickHouseに接続する

## 1. 接続情報を収集する
<ConnectionDetails />

## 2. ClickHouseリソースを作成する

Retoolアカウントにログインし、_Resources_ タブに移動します。「Create New」 -> 「Resource」を選択してください：

<img src={require('./images/retool_01.png').default} className="image" alt="新しいリソースを作成" style={{width: '75%', 'backgroundColor': 'transparent'}}/>
<br/>

利用可能なコネクタのリストから「JDBC」を選択します：

<img src={require('./images/retool_02.png').default} className="image" alt="JDBCコネクタを選択" style={{width: '75%', 'backgroundColor': 'transparent'}}/>
<br/>

セットアップウィザードで、「Driver name」として `com.clickhouse.jdbc.ClickHouseDriver` を必ず選択してください：

<img src={require('./images/retool_03.png').default} className="image" alt="正しいドライバを選択" style={{width: '75%', 'backgroundColor': 'transparent'}}/>
<br/>

ClickHouseの認証情報を次の形式で入力します： `jdbc:clickhouse://HOST:PORT/DATABASE?user=USERNAME&password=PASSWORD`。インスタンスがSSLを必要とする場合やClickHouse Cloudを使用している場合は、接続文字列に `&ssl=true` を追加してください。このようになります： `jdbc:clickhouse://HOST:PORT/DATABASE?user=USERNAME&password=PASSWORD&ssl=true`

<img src={require('./images/retool_04.png').default} className="image" alt="認証情報を指定" style={{width: '75%', 'backgroundColor': 'transparent'}}/>
<br/>

その後、接続をテストしてください：

<img src={require('./images/retool_05.png').default} className="image" alt="接続をテスト" style={{width: '75%', 'backgroundColor': 'transparent'}}/>
<br/>

これで、あなたのアプリでClickHouseリソースを使用して進めることができます。
