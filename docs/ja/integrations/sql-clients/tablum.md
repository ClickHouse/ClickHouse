---
sidebar_label: TABLUM.IO
slug: /ja/integrations/tablumio
description: TABLUM.IOは、ClickHouseをすぐにサポートするデータ管理SaaSです。
---

# TABLUM.IOをClickHouseに接続する

## TABLUM.IOのスタートアップページを開く

TABLUM.IOのクラウド版は[https://go.tablum.io/](https://go.tablum.io/)で利用可能です。

:::note
  TABLUM.IOの自己ホスト版をLinuxサーバーにDockerでインストールすることもできます。
:::

## 1. サービスにサインアップまたはサインインする

  最初に、メールを使用してTABLUM.IOにサインアップするか、GoogleまたはFacebookのアカウントを使用してクイックログインを行ってください。

  ![](@site/docs/ja/integrations/sql-clients/images/tablum-ch-0.png)

## 2. ClickHouseコネクタを追加する

ClickHouseの接続情報を準備し、**Connector**タブに移動して、ホストのURL、ポート、ユーザー名、パスワード、データベース名、コネクタの名前を入力します。これらのフィールドを入力後、**Test connection**ボタンをクリックして情報を検証し、その後、**Save connector for me**をクリックして永続化します。

:::tip
正しい**HTTP**ポートを指定し、接続情報に応じて**SSL**モードを切り替えることを確認してください。
:::

:::tip
通常、TLSを使用する場合はポートが8443で、使用しない場合は8123です。
:::

  ![](@site/docs/ja/integrations/sql-clients/images/tablum-ch-1.png)

## 3. コネクタを選択する

**Dataset**タブに移動します。ドロップダウンで最近作成したClickHouseコネクタを選択します。右側のパネルには利用可能なテーブルとスキーマのリストが表示されます。

  ![](@site/docs/ja/integrations/sql-clients/images/tablum-ch-2.png)

## 4. SQLクエリを入力して実行する

SQLコンソールにクエリを入力し、**Run Query**を押します。結果はスプレッドシートとして表示されます。

:::tip
カラム名を右クリックすると、ソート、フィルタ、その他のアクションを含むドロップダウンメニューが開きます。
:::

  ![](@site/docs/ja/integrations/sql-clients/images/tablum-ch-3.png)

:::note
TABLUM.IOを使用すると、
* あなたのTABLUM.IOアカウント内で複数のClickHouseコネクタを作成および利用でき、
* データソースに関係なくロードされたデータでクエリを実行でき、
* 結果を新しいClickHouseデータベースとして共有することができます。
:::

## 詳しく学ぶ

TABLUM.IOに関する詳細情報はhttps://tablum.ioで見つけることができます。
