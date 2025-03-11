---
sidebar_label: DataGrip
slug: /ja/integrations/datagrip
description: DataGrip は、ClickHouse を標準でサポートするデータベース IDE です。
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# DataGrip を ClickHouse に接続する

## DataGrip を開始またはダウンロード

DataGrip は https://www.jetbrains.com/datagrip/ で入手できます。

## 1. 接続情報を集める
<ConnectionDetails />

## 2. ClickHouse ドライバーをロードする

1. DataGrip を起動し、**Data Sources and Drivers** ダイアログの **Data Sources** タブで **+** アイコンをクリックします。

  ![](@site/docs/ja/integrations/sql-clients/images/datagrip-5.png)

  **ClickHouse** を選択します。

  :::tip
  接続を確立するにつれて順序が変わるため、ClickHouse がリストの一番上にまだ表示されていない場合があります。
  :::

  ![](@site/docs/ja/integrations/sql-clients/images/datagrip-6.png)

- **Drivers** タブに切り替えて ClickHouse ドライバーをロードします。

  DataGrip には、ダウンロードサイズを最小限に抑えるためにドライバーが付属していません。**Drivers** タブで **Complete Support** リストから **ClickHouse** を選択し、**+** 記号を展開します。**Provided Driver** オプションから **Latest stable** ドライバーを選択します。

  ![](@site/docs/ja/integrations/sql-clients/images/datagrip-1.png)

## 3. ClickHouse に接続する

- データベースの接続情報を指定し、**Test Connection** をクリックします：

  手順1で接続情報を集めました。ホストの URL、ポート、ユーザー名、パスワード、データベース名を入力し、接続をテストします。

  :::tip
  DataGrip ダイアログの **HOST** エントリーは実際には URL です。以下の画像を参照してください。

  JDBC URL 設定の詳細については、[ClickHouse JDBC ドライバー](https://github.com/ClickHouse/clickhouse-java) リポジトリを参照してください。
  :::

  ![](@site/docs/ja/integrations/sql-clients/images/datagrip-7.png)

## 詳細情報

DataGrip の詳細については、DataGrip ドキュメントを参照してください。
