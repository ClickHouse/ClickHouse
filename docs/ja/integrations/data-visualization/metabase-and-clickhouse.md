---
sidebar_label: Metabase
sidebar_position: 131
slug: /ja/integrations/metabase
keywords: [clickhouse, metabase, 接続, 統合, UI]
description: Metabaseは、データに関する質問をするための使いやすいオープンソースのUIツールです。
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# MetabaseをClickHouseに接続する

Metabaseは、データに関する質問をするための使いやすいオープンソースのUIツールです。MetabaseはJavaアプリケーションであり、<a href="https://www.metabase.com/start/oss/jar" target="_blank">JARファイルをダウンロード</a>して`java -jar metabase.jar`で実行するだけで動作します。Metabaseは、JDBCドライバーを使用してClickHouseに接続します。このドライバーはダウンロードして`plugins`フォルダに配置する必要があります。

## 目的

このガイドでは、Metabaseを使用してClickHouseのデータに関するいくつかの質問をして、回答を視覚化します。その回答の一例は次のようになります：

  <img src={require('./images/metabase_08.png').default} class="image" alt="Pie Chart" />
<p/>

:::tip データを追加する
作業するデータセットがない場合は、例のデータセットを追加できます。このガイドでは[UK Price Paid](/docs/ja/getting-started/example-datasets/uk-price-paid.md)データセットを使用するため、それを選ぶこともできます。同じドキュメンテーションカテゴリには他にもいくつかの選択肢があります。
:::

## 1. 接続の詳細を集める
<ConnectionDetails />

## 2. Metabase用のClickHouseプラグインをダウンロードする

1. `plugins`フォルダがない場合は、`metabase.jar`が保存されている場所のサブフォルダとして作成してください。

2. プラグインは`clickhouse.metabase-driver.jar`という名前のJARファイルです。最新バージョンのJARファイルを<a href="https://github.com/clickhouse/metabase-clickhouse-driver/release" target="_blank">https://github.com/clickhouse/metabase-clickhouse-driver/releases/latest</a>からダウンロードしてください。

3. `clickhouse.metabase-driver.jar`を`plugins`フォルダに保存します。

4. ドライバーが正しく読み込まれるようにMetabaseを開始（または再起動）します。

5. Metabaseを<a href="http://localhost:3000/" target="_blank">http://hostname:3000</a>でアクセスします。初回起動時にはウェルカム画面が表示され、一連の質問を通過する必要があります。データベースの選択を促された場合は、「**後でデータを追加します**」を選択してください：

## 3. MetabaseをClickHouseに接続する

1. 右上の歯車アイコンをクリックして、**Admin Settings**を選択し、<a href="http://localhost:3000/admin/settings/setup" target="_blank">Metabase管理ページ</a>にアクセスします。

2. **データベースを追加**をクリックします。または、**データベース**タブをクリックして、**データベースを追加**ボタンを選択します。

3. ドライバーのインストールが正常に行われた場合は、ドロップダウンメニューの**データベースタイプ**として**ClickHouse**が表示されます：

    <img src={require('./images/metabase_01.png').default} class="image" alt="Add a ClickHouse database" />

4. データベースに**表示名**を付けます。これはMetabaseの設定であるため、任意の名前を使用できます。

5. ClickHouseデータベースの接続詳細を入力します。ClickHouseサーバーがSSLを使用するように設定されている場合は、セキュア接続を有効にします。例：

    <img src={require('./images/metabase_02.png').default} class="image" style={{width: '80%'}}  alt="Connection details" />

6. **保存**ボタンをクリックすると、Metabaseはデータベースのテーブルをスキャンします。

## 4. SQLクエリを実行する

1. 右上の**Exit admin**ボタンをクリックして**管理設定**を終了します。

2. 右上の**+ New**メニューをクリックすると、質問を行い、SQLクエリを実行し、ダッシュボードを作成することができます：

    <img src={require('./images/metabase_03.png').default} class="image" style={{width: 283}} alt="New menu" />

3. 例えば、1995年から2022年までの年ごとの平均価格を返す`uk_price_paid`テーブルに対して実行されたSQLクエリは次の通りです：

    <img src={require('./images/metabase_04.png').default} class="image" alt="Run a SQL query" />

## 5. 質問をする

1. **+ New**をクリックし、**Question**を選択します。データベースとテーブルを基に質問を構築できます。例えば、以下の質問は`default`データベース内の`uk_price_paid`テーブルに関するものです。グレーター・マンチェスター郡内の町ごとの平均価格を計算する簡単な質問です：

    <img src={require('./images/metabase_06.png').default} class="image" alt="New question" />

2. **Visualize**ボタンをクリックして、結果を表形式で表示します。

    <img src={require('./images/metabase_07.png').default} class="image" alt="New question" />

3. 結果の下にある**Visualization**ボタンをクリックして、視覚化を棒グラフ（または他の利用可能なオプションのいずれか）に変更します：

    <img src={require('./images/metabase_08.png').default} class="image" alt="Pie Chart visualization" />

## 詳しく学ぶ

Metabaseおよびダッシュボードの構築方法についての詳細は、<a href="https://www.metabase.com/docs/latest/" target="_blank">Metabaseのドキュメンテーション</a>を訪問してください。

## 関連コンテンツ

- ブログ: [データの視覚化 - 第3部 - Metabase](https://clickhouse.com/blog/visualizing-data-with-metabase)
