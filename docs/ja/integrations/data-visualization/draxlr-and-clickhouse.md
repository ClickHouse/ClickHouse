---
sidebar_label: Draxlr
sidebar_position: 131
slug: /ja/integrations/draxlr
keywords: [clickhouse, draxlr, connect, integrate, ui]
description: Draxlrは、データの視覚化と分析を可能にするビジネスインテリジェンスツールです。
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# DraxlrをClickHouseに接続する

Draxlrは、ClickHouseデータベースへの接続を直感的なインターフェースで提供し、チームが数分で洞察を探索、視覚化、公開することを可能にします。このガイドでは、成功した接続を確立するための手順を順を追って説明します。

## 1. ClickHouseの認証情報を取得する
<ConnectionDetails />

## 2. DraxlrをClickHouseに接続する

1. ナビバーで**Connect a Database**ボタンをクリックします。

2. 利用可能なデータベースのリストから**ClickHouse**を選択し、次へをクリックします。

3. ホスティングサービスのいずれかを選び、次へをクリックします。

4. **Connection Name**フィールドに任意の名前を使用します。

5. フォームに接続詳細を追加します。

  <img src={require('./images/draxlr_01.png').default} class="image" style={{width: '80%'}}  alt="Connection Form" />

6. **Next**ボタンをクリックし、接続が確立されるまで待ちます。接続が成功するとテーブルページが表示されます。

## 3. データを探索する

1. リスト内のいずれかのテーブルをクリックします。

2. テーブル内のデータを表示するための探索ページに移動します。

3. フィルターの追加、結合、並べ替えを開始できます。

  <img src={require('./images/draxlr_02.png').default} class="image" style={{width: '80%'}}  alt="Connection Form" />

4. **Graph**ボタンを使用してデータを視覚化するグラフタイプを選択することもできます。

  <img src={require('./images/draxlr_05.png').default} class="image" style={{width: '80%'}}  alt="Connection Form" />


## 4. SQLクエリの使用

1. ナビバーでExploreボタンをクリックします。

2. **Raw Query**ボタンをクリックし、テキストエリアにクエリを入力します。

  <img src={require('./images/draxlr_03.png').default} class="image" style={{width: '80%'}}  alt="Connection Form" />

3. **Execute Query**ボタンをクリックして結果を確認します。


## 5. クエリの保存

1. クエリを実行した後、**Save Query**ボタンをクリックします。

  <img src={require('./images/draxlr_04.png').default} class="image" style={{width: '80%'}}  alt="Connection Form" />

2. **Query Name**テキストボックスにクエリの名前を付け、カテゴリ分けのためフォルダを選択できます。

3. **Add to dashboard**オプションを使用して、結果をダッシュボードに追加することもできます。

4. **Save**ボタンをクリックしてクエリを保存します。


## 6. ダッシュボードの構築

1. ナビバーで**Dashboards**ボタンをクリックします。

  <img src={require('./images/draxlr_06.png').default} class="image" style={{width: '80%'}}  alt="Connection Form" />

2. 左サイドバーの**Add +**ボタンをクリックして新しいダッシュボードを追加できます。

3. 新しいウィジェットを追加するには、右上の**Add**ボタンをクリックします。

4. 保存されたクエリのリストからクエリを選択し、視覚化タイプを選んで**Add Dashboard Item**ボタンをクリックします。


## 詳細を学ぶ
Draxlrについてもっと知るためには、[Draxlrドキュメンテーション](https://draxlr.notion.site/draxlr/Draxlr-Docs-d228b23383f64d00a70836ff9643a928)サイトを訪れてください。
