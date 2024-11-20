---
sidebar_label: Superset
sidebar_position: 198
slug: /ja/integrations/superset
keywords: [clickhouse, superset, connect, integrate, ui]
description: Apache Supersetはオープンソースのデータ探索および視覚化プラットフォームです。
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# SupersetをClickHouseに接続する

<a href="https://superset.apache.org/" target="_blank">Apache Superset</a>は、Pythonで書かれたオープンソースのデータ探索および視覚化プラットフォームです。Supersetは、ClickHouseが提供するPythonドライバーを使用してClickHouseに接続します。それでは、その方法を見てみましょう...

## 目的

このガイドでは、ClickHouseデータベースからデータを使用してSupersetでダッシュボードを作成します。ダッシュボードは次のようになります：

  <img src={require('./images/superset_12.png').default} class="image" alt="New Dashboard" />
<p/>

:::tip データを追加する
作業するデータセットがない場合は、例の1つを追加できます。このガイドでは[UK Price Paid](/docs/ja/getting-started/example-datasets/uk-price-paid.md)データセットを使用するので、それを選ぶこともできます。同じドキュメントカテゴリには他にもいくつかあります。
:::

## 1. 接続情報を収集する
<ConnectionDetails />

## 2. ドライバをインストールする

1. Supersetは`clickhouse-connect`ドライバーを使用してClickHouseに接続します。`clickhouse-connect`の詳細は<a href="https://pypi.org/project/clickhouse-connect/" target="_blank">https://pypi.org/project/clickhouse-connect/</a>にあり、以下のコマンドでインストールできます：

    ```console
    pip install clickhouse-connect
    ```

2. Supersetを開始（または再起動）します。

## 3. SupersetをClickHouseに接続する

1. Superset内で、上部メニューから**Data**を選択し、ドロップダウンメニューから**Databases**を選択します。**+ Database**ボタンをクリックして新しいデータベースを追加します：

  <img src={require('./images/superset_01.png').default} class="image" alt="Add a new database" />

2. 最初のステップでは、データベースのタイプとして**ClickHouse Connect**を選択します：

  <img src={require('./images/superset_02.png').default} class="image" alt="Select ClickHouse" />

3. 次のステップでは：
  - SSLをオンまたはオフに設定します。
  - 先ほど収集した接続情報を入力します。
  - **DISPLAY NAME**を指定します。これは任意の名前にできます。複数のClickHouseデータベースに接続する場合は、名前をより説明的にします。

  <img src={require('./images/superset_03.png').default} class="image" alt="Test the connection" />

4. **CONNECT**をクリックし、続けて**FINISH**ボタンをクリックしてセットアップウィザードを完了します。これで、データベースがデータベース一覧に表示されるはずです。

## 4. データセットを追加する

1. SupersetでClickHouseのデータを操作するには、**_データセット_**を定義する必要があります。Supersetの上部メニューから**Data**を選択し、ドロップダウンメニューから**Datasets**を選択します。

2. データセットを追加するボタンをクリックします。データソースとして新しいデータベースを選択し、データベース内に定義されているテーブルが表示されます：

  <img src={require('./images/superset_04.png').default} class="image" alt="New dataset" />


3. ダイアログウィンドウの下部にある**ADD**ボタンをクリックすると、テーブルがデータセットリストに表示されます。これでダッシュボードを作成してClickHouseデータを分析する準備が整いました！


## 5. Supersetでチャートとダッシュボードを作成する

Supersetに慣れている場合、この次のセクションは簡単に感じるかもしれません。Supersetに初めて触れる方も、他の多くの視覚化ツールと似ており、始めるのに時間はかかりませんが、詳細と微妙な点は使用していく中で学んでいきます。

1. 最初にダッシュボードを作成します。Supersetの上部メニューから**Dashboards**を選択します。右上のボタンをクリックして新しいダッシュボードを追加します。次のダッシュボードは**UK property prices**という名前です：

  <img src={require('./images/superset_05.png').default} class="image" alt="New dashboard" />

2. 新しいチャートを作成するには、上部メニューから**Charts**を選択し、新しいチャートを追加するボタンをクリックします。多数のオプションが表示されます。次の例は、**uk_price_paid**データセットを使用した**Pie Chart**チャートを示しています：

  <img src={require('./images/superset_06.png').default} class="image" alt="New chart" />

3. Supersetの円グラフには、**Dimension**と**Metric**が必要で、他の設定はオプションです。次元とメトリックのフィールドを自分で選択でき、この例ではClickHouseフィールドの`district`を次元として、`AVG(price)`をメトリックとして使用しています。

  <img src={require('./images/superset_08.png').default} class="image" alt="The SUM metric" />
  <img src={require('./images/superset_09.png').default} class="image" alt="The SUM metric" />

5. 円グラフではなくドーナツグラフを好む場合は、**CUSTOMIZE**でそれとその他のオプションを設定できます：

  <img src={require('./images/superset_10.png').default} class="image" alt="Add Chart to Dashboard" />

6. **SAVE**ボタンをクリックしてチャートを保存し、**ADD TO DASHBOARD**ドロップダウンから**UK property prices**を選び、**SAVE & GO TO DASHBOARD**を選択してチャートを保存し、ダッシュボードに追加します：

  <img src={require('./images/superset_11.png').default} class="image" alt="Add Chart to Dashboard" />

7. これで完了です。ClickHouseのデータに基づくSupersetでのダッシュボード作成は、非常に高速なデータ分析の世界を切り開きます！

  <img src={require('./images/superset_12.png').default} class="image" alt="New Dashboard" />

## 関連コンテンツ

- ブログ: [ClickHouseでデータを視覚化する - Part 2 - Superset](https://clickhouse.com/blog/visualizing-data-with-superset)
