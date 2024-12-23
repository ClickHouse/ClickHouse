---
sidebar_label: Power BI
slug: /ja/integrations/powerbi
keywords: [ clickhouse, powerbi, connect, integrate, ui ]
description: Microsoft Power BIは、ビジネスインテリジェンスに主眼を置いてMicrosoftが開発したインタラクティブなデータ可視化ソフトウェア製品です。
---

import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# Power BI

Power BIは、[ODBCドライバー](https://github.com/ClickHouse/clickhouse-odbc)や[ClickHouseネイティブコネクタ](https://github.com/ClickHouse/power-bi-clickhouse)を使用して、ClickHouse Cloudまたはオンプレミスのデプロイメントからデータを読み込むことができます。どちらの方法もロードモードをサポートしていますが、後者はDirect Queryモードもサポートし、テーブル全体をロードする必要がありません。

このチュートリアルでは、これらの方法のどちらかを使用してデータを読み込むプロセスを案内します。
<br/>
<br/>
<br/>

# ClickHouseネイティブコネクタ

## 1. 接続情報を集める

<ConnectionDetails />

## 2. ClickHouse ODBCクライアントをインストールする

最新のClickHouse ODBCリリースを[こちら](https://github.com/ClickHouse/clickhouse-odbc/releases)からダウンロードしてください。付属の`.msi`インストーラーを実行し、ウィザードに従ってください。"デバッグシンボル"はオプションで必要ありませんので、デフォルトのままで大丈夫です。

<img src={require('./images/powerbi_01.png').default} class="image" alt="ODBCドライバーのインストール" style={{width: 
'50%', 'background-color': 'transparent'}}/>
<br/>

ドライバーのインストールが完了したら、インストールが成功したことを確認できます。スタートメニューでODBCを検索し、「ODBCデータソース **(64-bit)**」を選択します。

<img src={require('./images/powerbi_02.png').default} class="image" alt="新しいODBCデータソースの作成" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

ClickHouseドライバーがリストにあることを確認します。

<img src={require('./images/powerbi_03.png').default} class="image" alt="ODBCの存在確認" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

まだPower BIがインストールされていない場合は、[Power BI Desktopをダウンロードしてインストール](https://www.microsoft.com/en-us/download/details.aspx?id=58494)してください。

## 3. ClickHouseネイティブコネクタをインストールする

* カスタムコネクター用に次のディレクトリを作成します：[Documents]\Power BI Desktop\Custom Connectors
* 最新のリリース (.mezファイル) を[Releases Section](https://github.com/ClickHouse/power-bi-clickhouse/releases)からダウンロードし、前のステップで作成したディレクトリに配置します。
* Power BIを開き、署名されていないコネクタの読み込みを有効にします：ファイル -> オプションと設定 -> オプション -> セキュリティ -> データ拡張 -> 警告なしまたは検証なしで任意の拡張機能をロードすることを許可

<img src={require('./images/powerbi_04.png').default} class="image" alt="署名されていないコネクタの読み込みを有効にする" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

* Power BIを再起動します。

## 4. Power BIにデータを取得する

Power BI Desktopの開始画面で、「データ取得」をクリックします。

<img src={require('./images/powerbi_05.png').default} class="image" alt="Power BI Desktopの開始" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

「ClickHouseConnector (Beta)」を検索します。

<img src={require('./images/powerbi_06.png').default} class="image" alt="データソースの選択" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

コネクタを選択し、次のボックスを埋めます：

* サーバー（必須フィールド） - インスタンスのドメイン/アドレス。プレフィックス/サフィックスなしで追加してください。
* ポート（必須フィールド） - インスタンスのポート。
* データベース - データベース名。
* オプション - [ClickHouse ODBC GitHubページ](https://github.com/ClickHouse/clickhouse-odbc#configuration)にリストされている任意のODBCオプション
* データ接続モード - ClickHouseに直接クエリを行うためにDirectQueryを選択します。小さな負荷がある場合は、インポートモードを選択し、データ全体をPower BIにロードすることができます。

<img src={require('./images/powerbi_07.png').default} class="image" alt="ClickHouseインスタンス情報の入力" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

* ユーザー名とパスワードを指定します。

<img src={require('./images/powerbi_08.png').default} class="image" alt="ユーザー名とパスワードのプロンプト" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

最後に、Navigatorビューでデータベースとテーブルが表示されるはずです。希望のテーブルを選択し、「ロード」をクリックしてClickHouseからデータをインポートします。

<img src={require('./images/powerbi_09.png').default} class="image" alt="Navigatorビュー" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

インポートが完了すると、通常通りPower BIでClickHouseデータにアクセスできるようになります。
<br/>

## Power BIサービス

クラウドでの使用については、Microsoftのドキュメントを参照してください。オンプレミスデータゲートウェイを使用して[カスタムデータコネクタを使用する](https://learn.microsoft.com/en-us/power-bi/connect-data/service-gateway-custom-connectors)方法をご覧ください。
##
<br/>
<br/>

# ODBCドライバー

上記のネイティブコネクタセクションのステップ1と2に従ってください。

## 3. 新しいユーザーDSNを作成する

ドライバーのインストールが完了したら、ODBCデータソースを作成できます。スタートメニューでODBCを検索し、「ODBCデータソース (64-bit)」を選択します。

<img src={require('./images/powerbi_02.png').default} class="image" alt="新しいODBCデータソースの作成" style={{width: '40%', 'background-color': 'transparent'}}/>
<br/>

ここで新しいユーザーDSNを追加する必要があります。左の「追加」ボタンをクリックします。

<img src={require('./images/powerbi_10.png').default} class="image" alt="新しいユーザーDSNの追加" style={{width: '40%', 'background-color': 'transparent'}}/>
<br/>

ODBCドライバーのUnicodeバージョンを選択します。

<img src={require('./images/powerbi_11.png').default} class="image" alt="Unicodeバージョンの選択" style={{width: '40%', 'background-color': 'transparent'}}/>
<br/>

接続情報を入力します。「ホスト」フィールドにはプロトコルを含めないでください（例： http:// や https:// 部分を省略）。ClickHouse Cloudを使用している場合やオンプレミスのデプロイメントでSSLが有効になっている場合、「SSLMode」フィールドにrequireと入力します。「タイムアウト」フィールド値は秒単位で設定され、省略された場合はデフォルト値の30秒になります。

<img src={require('./images/powerbi_12.png').default} class="image" alt="接続情報" style={{width: '30%', 'background-color': 'transparent'}}/>
<br/>

## 4. Power BIにデータを取得する

まだPower BIがインストールされていない場合は、[Power BI Desktopをダウンロードしてインストール](https://www.microsoft.com/en-us/download/details.aspx?id=58494)してください。

Power BI Desktopの開始画面で、「データ取得」をクリックします。

<img src={require('./images/powerbi_05.png').default} class="image" alt="Power BI Desktopの開始" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

「その他」 -> 「ODBC」を選択します。

<img src={require('./images/powerbi_13.png').default} class="image" alt="データソースメニュー" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

リストから先ほど作成したデータソースを選択します。

<img src={require('./images/powerbi_14.png').default} class="image" alt="ODBCデータソースの選択" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

データソース作成時にすべての資格情報を指定した場合、すぐに接続するはずです。そうでない場合は、ユーザー名とパスワードを指定するよう求められます。

<img src={require('./images/powerbi_15.png').default} class="image" alt="Navigatorビュー" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

最後に、Navigatorビューでデータベースとテーブルが表示されるはずです。希望のテーブルを選択し、「ロード」をクリックしてClickHouseからデータをインポートします。

<img src={require('./images/powerbi_09.png').default} class="image" alt="Navigatorビュー" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

インポートが完了すると、通常通りPower BIでClickHouseデータにアクセスできるようになります。
<br/>
<br/>

:::note
UInt64などの符号なし整数型は、自動的にはデータセットにロードされません。Power BIでサポートされている最大の整数型はInt64です。<br/>
データを正しくインポートするには、Navigatorで「ロード」ボタンを押す前に、「データを変換」をクリックしてください。
:::

この例では、`pageviews`テーブルにUInt64カラムがあり、デフォルトでは「バイナリ」として認識されています。「データを変換」を行うことで、カラムの型を再割り当てし、例えばTextとして設定することができます。

<img src={require('./images/powerbi_16.png').default} class="image" alt="Navigatorビュー" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

完了したら、左上の「閉じて適用」をクリックし、データのロードを続行します。
