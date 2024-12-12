---
sidebar_label: Explo
sidebar_position: 131
slug: /ja/integrations/explo
keywords: [clickhouse, Explo, connect, integrate, ui]
description: Exploはデータに関する質問を問うための使いやすいオープンソースのUIツールです。
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# ExploをClickHouseに接続する

あらゆるプラットフォーム向けの顧客向け分析。美しい視覚化のために設計され、簡素さを求めて構築されています。

## 目的

このガイドでは、ClickHouseからExploにデータを接続し、結果を視覚化する方法を説明します。チャートは以下のように表示されます:
<img src={require('./images/explo_15.png').default} class="image" alt="Explo Dashboard" />

<p/>

:::tip データを追加
作業するデータセットがない場合、例を追加することができます。このガイドでは[UK Price Paid](/docs/ja/getting-started/example-datasets/uk-price-paid.md)データセットを使用しているので、それを選ぶことをお勧めします。同じドキュメントカテゴリに他の例もいくつかあります。
:::

## 1. 接続に必要な詳細を集めます
<ConnectionDetails />

## 2. ExploをClickHouseに接続する

1. Exploアカウントにサインアップします。

2. 左側のサイドバーでExploの**データ**タブをクリックします。

<img src={require('./images/explo_01.png').default} class="image" alt="Data Tab" />

3. 右上の**データソースを接続**をクリックします。

<img src={require('./images/explo_02.png').default} class="image" alt="Connect Data Source" />

4. **はじめに**ページの情報を入力します。

<img src={require('./images/explo_03.png').default} class="image" alt="Getting Started" />

5. **Clickhouse**を選択します。

<img src={require('./images/explo_04.png').default} class="image" alt="Clickhouse" />

6. **Clickhouseの資格情報**を入力します。

<img src={require('./images/explo_05.png').default} class="image" alt="Credentials" />

7. **セキュリティ**を設定します。

<img src={require('./images/explo_06.png').default} class="image" alt="Security" />

8. Clickhouse内で、**ExploのIPをホワイトリストに追加**します。
`
54.211.43.19, 52.55.98.121, 3.214.169.94, and 54.156.141.148
`

## 3. ダッシュボードを作成する

1. 左側のナビゲーションバーで**ダッシュボード**タブに移動します。

<img src={require('./images/explo_07.png').default} class="image" alt="Dashboard" />

2. 右上の**ダッシュボードを作成**をクリックし、ダッシュボードに名前を付けます。これでダッシュボードが作成されました！

<img src={require('./images/explo_08.png').default} class="image" alt="Create Dashboard" />

3. 次のような画面が表示されます：

<img src={require('./images/explo_09.png').default} class="image" alt="Explo Dashboard" />

## 4. SQLクエリを実行する

1. スキーマタイトルの下にある右側のサイドバーからテーブル名を取得します。それから以下のコマンドをデータセットエディタに入力します：
`
SELECT * FROM YOUR_TABLE_NAME
LIMIT 100
`

<img src={require('./images/explo_10.png').default} class="image" alt="Explo Dashboard" />

2. 実行をクリックし、プレビュータブに移動してデータを確認します。

<img src={require('./images/explo_11.png').default} class="image" alt="Explo Dashboard" />

## 5. チャートを作成する

1. 左側からバーチャートのアイコンをドラッグして画面に配置します。

<img src={require('./images/explo_16.png').default} class="image" alt="Explo Dashboard" />

2. データセットを選択します。次のような画面が表示されます：

<img src={require('./images/explo_12.png').default} class="image" alt="Explo Dashboard" />

3. **county**をX軸に、**Price**をY軸のセクションに入力します：

<img src={require('./images/explo_13.png').default} class="image" alt="Explo Dashboard" />

4. 集約を**AVG**に変更します。

<img src={require('./images/explo_14.png').default} class="image" alt="Explo Dashboard" />

5. これで価格ごとに分類された住宅の平均価格が表示されました！

<img src={require('./images/explo_15.png').default} class="image" alt="Explo Dashboard" />

## 詳細を学ぶ

Exploやダッシュボードの作成について詳しくは、<a href="https://docs.explo.co/" target="_blank">Exploのドキュメント</a>を参照してください。
