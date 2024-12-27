---
sidebar_label: Rocket BI
sidebar_position: 131
slug: /ja/integrations/rocketbi
keywords: [clickhouse, rocketbi, connect, integrate, ui]
description: RocketBIは、データを迅速に分析し、ドラッグ＆ドロップでビジュアライゼーションを構築し、ウェブブラウザ上で同僚と共同作業できるセルフサービスのビジネスインテリジェンスプラットフォームです。
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# 目標: 最初のダッシュボードを作成する

このガイドでは、Rocket.BIをインストールして簡単なダッシュボードを作成します。
以下がそのダッシュボードです：

<img width="800" alt="github_rocketbi2" src={require('./images/rocketbi_01.gif').default}/>
<br/>

[こちらのリンクからダッシュボードを確認できます。](https://demo.rocket.bi/dashboard/sales-dashboard-7?token=7eecf750-cbde-4c53-8fa8-8b905fec667e)

## インストール

事前にビルドされたDockerイメージを使用してRocketBIを起動します。

docker-compose.ymlと設定ファイルを取得します：
```
wget https://raw.githubusercontent.com/datainsider-co/rocket-bi/main/docker/docker-compose.yml
wget https://raw.githubusercontent.com/datainsider-co/rocket-bi/main/docker/.clickhouse.env
```
.clickhouse.envを編集し、ClickHouseサーバーの情報を追加します。

以下のコマンドでRocketBIを開始します： ```docker-compose up -d```

ブラウザで```localhost:5050```を開き、こちらのアカウントでログインします： ```hello@gmail.com/123456```

ソースからのビルドや高度な設定については、[Rocket.BI Readme](https://github.com/datainsider-co/rocket-bi/blob/main/README.md)をご参照ください。

## ダッシュボードを作ってみましょう

ダッシュボードでは、レポートを見つけることができ、**+New**をクリックして視覚化を開始します。
ダッシュボードで**無制限のダッシュボード**と**無制限のチャート**を作成できます。

<img width="800" alt="rocketbi_create_chart" src={require('./images/rocketbi_02.gif').default}/>
<br/>

YouTubeで高解像度のチュートリアルを見る：[https://www.youtube.com/watch?v=TMkdMHHfvqY](https://www.youtube.com/watch?v=TMkdMHHfvqY)

### チャートコントロールを作成する

#### メトリクスコントロールを作成
タブフィルターで使用したいメトリックフィールドを選択します。集計設定を確認してください。

<img width="650" alt="rocketbi_chart_6" src={require('./images/rocketbi_03.png').default}/>
<br/>

フィルタに名前を付け、コントロールをダッシュボードに保存します。

<img width="400" alt="Metrics Control" src={require('./images/rocketbi_04.png').default}/>

#### 日付型コントロールを作成
メインの日付カラムとして日付フィールドを選択します：

<img width="650" alt="rocketbi_chart_4" src={require('./images/rocketbi_05.png').default}/>
<br/>

異なるルックアップ範囲で重複バリアントを追加します。例えば、年、月、日付や曜日など。

<img width="650" alt="rocketbi_chart_5" src={require('./images/rocketbi_06.png').default}/>
<br/>

フィルタに名前を付け、コントロールをダッシュボードに保存します。

<img width="200" alt="Date Range Control" src={require('./images/rocketbi_07.png').default}/>

### さあ、チャートを作りましょう

#### 円グラフ: 地域別売上指標
新しいチャートを追加し、円グラフを選択します。

<img width="650" alt="Add Pie Chart" src={require('./images/rocketbi_08.png').default}/>
<br/>

まず、データセットからカラム「Region」をドラッグ＆ドロップして凡例フィールドに配置します。

<img width="650" alt="Drag-n-drop Column to Chart" src={require('./images/rocketbi_09.png').default}/>
<br/>

その後、チャートコントロールタブに移動します。

<img width="650" alt="Navigate to Chart Control in Visualization" src={require('./images/rocketbi_10.png').default}/>
<br/>

メトリクスコントロールを値フィールドにドラッグ＆ドロップします。

<img width="650" alt="Use Metrics Control in Chart" src={require('./images/rocketbi_11.png').default}/>
<br/>

（メトリクスコントロールはソートとしても使えます）

さらなるカスタマイズのためにチャート設定に移動します。

<img width="650" alt="Custom the Chart with Setting" src={require('./images/rocketbi_12.png').default}/>
<br/>

例えば、データラベルをパーセンテージに変更します。

<img width="650" alt="Chart Customization Example" src={require('./images/rocketbi_13.png').default}/>
<br/>

チャートを保存してダッシュボードに追加します。

<img width="650" alt="Overview Dashboard with Pie Chart" src={require('./images/rocketbi_14.png').default}/>

#### 時系列チャートに日付コントロールを使用
積み上げカラムチャートを使用しましょう。

<img width="650" alt="Create a Time-series chart with Tab Control" src={require('./images/rocketbi_15.png').default}/>
<br/>

チャートコントロールで、メトリクスコントロールをY軸、日付範囲をX軸に使用します。

<img width="650" alt="Use Date Range as Controller" src={require('./images/rocketbi_16.png').default}/>
<br/>

地域カラムをブレークダウンに追加します。

<img width="650" alt="Add Region into Breakdown" src={require('./images/rocketbi_17.png').default}/>
<br/>

KPIとしてナンバーチャートを追加し、ダッシュボードを目立たせます。

<img width="800" alt="Screenshot 2022-11-17 at 10 43 29" src={require('./images/rocketbi_18.png').default} />
<br/>

これで、rocket.BIを使って最初のダッシュボードを無事に作成しました。
