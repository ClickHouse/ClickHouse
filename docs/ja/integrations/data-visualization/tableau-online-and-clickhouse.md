---
sidebar_label: Tableau Online
slug: /ja/integrations/tableau-online
keywords: [clickhouse, tableau, online, mysql, connect, integrate, ui]
description: Tableau Onlineは、どこからでも人々がより早く、より自信を持って意思決定できるよう、データの力を簡素化します。
---

import MySQLCloudSetup from '@site/docs/ja/_snippets/_clickhouse_mysql_cloud_setup.mdx';
import MySQLOnPremiseSetup from '@site/docs/ja/_snippets/_clickhouse_mysql_on_premise_setup.mdx';

# Tableau Online

Tableau Onlineは、公式のMySQLデータソースを使用して、MySQLインターフェース経由でClickHouse CloudまたはオンプレミスのClickHouseセットアップに接続できます。

## ClickHouse Cloud セットアップ
<MySQLCloudSetup />

## オンプレミスのClickHouse サーバーセットアップ
<MySQLOnPremiseSetup />

## Tableau OnlineからClickHouseに接続する（SSLなしオンプレミスの場合）

Tableau Cloudサイトにログインし、新しい公開データソースを追加します。

<img src={require('./images/tableau_online_01.png').default} class="image" alt="Creating a new published data source" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

利用可能なコネクタのリストから「MySQL」を選択します。

<img src={require('./images/tableau_online_02.png').default} class="image" alt="Selecting MySQL connector" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

ClickHouseのセットアップ中に収集した接続情報を指定します。

<img src={require('./images/tableau_online_03.png').default} class="image" alt="Specifying your connection details" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

Tableau Onlineはデータベースを調べ、利用可能なテーブルのリストを提供します。目的のテーブルを右側のキャンバスにドラッグします。さらに、「今すぐ更新」をクリックしてデータをプレビューしたり、調査したフィールドのタイプや名前を微調整することもできます。

<img src={require('./images/tableau_online_04.png').default} class="image" alt="Selecting the tables to use" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

その後、右上の「Publish As」をクリックすれば、新しく作成したデータセットをTableau Onlineで通常通り使用できるようになります。

注：Tableau OnlineをTableau Desktopと組み合わせて使用し、ClickHouseデータセットを共有する場合は、こちらに表示されるセットアップガイドに従い、デフォルトのMySQLコネクタを使用するTableau Desktopを使用することを確認してください。[こちら](https://www.tableau.com/support/drivers)のデータソースドロップダウンでMySQLを選択した場合のみ表示されます。また、M1 Macをお使いの方は、[こちらのトラブルシューティングスレッド](https://community.tableau.com/s/question/0D58b0000Ar6OhvCQE/unable-to-install-mysql-driver-for-m1-mac)でドライバインストールの方法を確認してください。

## Tableau OnlineからClickHouseに接続する（CloudまたはオンプレミスセットアップでSSL使用）

Tableau OnlineのMySQL接続設定ウィザードではSSL証明書を提供することができないため、接続設定はTableau Desktopを使用して行い、それをTableau Onlineにエクスポートする必要があります。このプロセスは比較的簡単です。

WindowsまたはMacマシンでTableau Desktopを実行し、「接続」->「サーバーに接続」->「MySQL」を選択します。
おそらく、最初にマシンにMySQLドライバをインストールする必要があります。[こちら](https://www.tableau.com/support/drivers)のデータソースドロップダウンでMySQLを選択した場合に表示されるセットアップガイドに従ってください。また、M1 Macをお使いの方は、[こちらのトラブルシューティングスレッド](https://community.tableau.com/s/question/0D58b0000Ar6OhvCQE/unable-to-install-mysql-driver-for-m1-mac)でドライバインストールの方法を確認してください。

<img src={require('./images/tableau_desktop_01.png').default} class="image" alt="Create a new data source" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

:::note
MySQL接続設定画面で、「SSL」オプションが有効になっていることを確認してください。
ClickHouse CloudのSSL証明書は、[LetsEncrypt](https://letsencrypt.org/certificates/)によって署名されています。
このルート証明書を[こちら](https://letsencrypt.org/certs/isrgrootx1.pem)からダウンロードできます。
:::

ClickHouse CloudインスタンスのMySQLユーザー資格情報と、ダウンロードしたルート証明書へのパスを提供します。

<img src={require('./images/tableau_desktop_02.png').default} class="image" alt="Specifying your credentials" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

通常通り、目的のテーブルを選択し（Tableau Onlineと同様）、サーバー -> データソースの公開 -> Tableau Cloudを選択します。

<img src={require('./images/tableau_desktop_03.png').default} class="image" alt="Publish data source" style={{width: '75%', 'background-color': 'transparent'}}/>
<br/>

重要：認証オプションで「埋め込みパスワード」を選択する必要があります。

<img src={require('./images/tableau_desktop_04.png').default} class="image" alt="Data source publishing settings - embedding your credentials" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

さらに、「発行したデータソースを使用するようにワークブックを更新」を選択します。

<img src={require('./images/tableau_desktop_05.png').default} class="image" alt="Data source publishing settings - updating the workbook for online usage" style={{width: '50%', 'background-color': 'transparent'}}/>
<br/>

最後に「発行」をクリックすると、埋め込まれた資格情報を持つデータソースがTableau Onlineで自動的に開かれます。


## 既知の制限事項 (ClickHouse 23.11)

既知の制限事項はすべてClickHouse `23.11`で修正されています。他の互換性の問題が発生した場合は、[こちら](https://clickhouse.com/company/contact)からご連絡いただくか、新しい[問題を作成](https://github.com/ClickHouse/ClickHouse/issues)してください。
