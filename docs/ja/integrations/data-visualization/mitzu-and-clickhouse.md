---
sidebar_label: Mitzu
slug: /ja/integrations/mitzu
keywords: [clickhouse, mitzu, connect, integrate, ui]
description: Mitzuはノーコードのデータウェアハウスネイティブ製品分析アプリケーションです。
---

import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# MitzuをClickHouseに接続する

Mitzuはノーコードのデータウェアハウスネイティブ製品分析アプリケーションです。Amplitude、Mixpanel、Posthogと同様に、MitzuはユーザーがSQLやPythonの知識なしに製品使用データをクエリできるようにします。Mitzuは企業の製品使用データをコピーするのではなく、企業のデータウェアハウスやデータレイク上でネイティブなSQLクエリを生成します。

## 目的

本ガイドでは以下をカバーします：

- データウェアハウスネイティブの製品分析
- MitzuをClickHouseに統合する方法

:::tip サンプルデータセット
Mitzuで使用するデータセットがない場合は、NYC Taxi Dataを使用できます。このデータセットはClickhouse Cloudで利用可能です。
:::

## 1. 接続情報を集める

<ConnectionDetails />

## 2. Mitzuにサインインまたはサインアップ

最初のステップとして[https://app.mitzu.io](https://app.mitzu.io)にアクセスしてサインアップしてください。

<img src={require('./images/mitzu_01.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="サインイン" />

## 3. ワークスペースを作成する

組織を作成した後、最初のワークスペースを作成するように求められます。

<img src={require('./images/mitzu_02.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="ワークスペースを作成" ></img>

## 4. MitzuをClickHouseに接続する

ワークスペースが作成されたら、接続情報を手動で設定する必要があります。

<img src={require('./images/mitzu_03.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="接続情報を設定" ></img>

ガイド付きオンボーディングでは、Mitzuは単一のテーブルと統合することができます。

> ClickHouseセットアップで製品分析を行うには、テーブルのいくつかの重要なカラムを指定する必要があります。
>
> 以下がそのカラムです：
>
> - **ユーザーID** - ユーザーのユニークな識別子のカラム。
> - **イベント時間** - イベントのタイムスタンプのカラム。
> - オプション[**イベント名**] - テーブルに複数のイベントタイプが含まれている場合、イベントをセグメントするカラム。

<img src={require('./images/mitzu_04.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="テーブル接続を設定"></img>

<br/>

:::tip 追加のテーブルを追加
最初のガイド付きセットアップが完了した後、追加のテーブルを追加することが可能です。以下を参照してください。
:::

## 5. イベントカタログを作成する

オンボーディングの最終ステップは、`イベントカタログ`の作成です。

<img src={require('./images/mitzu_05.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="イベントカタログを作成" ></img>

このステップでは、上記で定義されたテーブルからすべてのイベントとそのプロパティを見つけます。
このステップはデータセットのサイズによりますが、数分かかる場合があります。

すべてがうまくいけば、イベントを探索する準備が整います。
<img src={require('./images/mitzu_06.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="探索する" width="300px"></img>

## 6. セグメンテーションクエリを実行する

Mitzuでのユーザーセグメンテーションは、Amplitude、Mixpanel、Posthogと同様に簡単です。

探索ページの左側でイベントを選択し、時間軸の設定は上部で行います。

<img src={require('./images/mitzu_07.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="セグメンテーション" ></img>

<br/>

:::tip フィルターとブレークダウン
フィルタリングは期待通りに行えます。プロパティ（ClickHouseカラム）を選び、フィルターしたい値をドロップダウンから選択します。
ブレークダウンの場合は、任意のイベントまたはユーザープロパティを選択します（ユーザープロパティを統合する方法は以下を参照）。
:::

## 7. ファネルクエリを実行する

ファネルのために最大9ステップを選択します。ファネルがユーザーによって完了される時間枠を選択します。
単一行のSQLコードを書くことなく、すぐにコンバージョン率の洞察を得ることができます。

<img src={require('./images/mitzu_08.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="ファネル" ></img>

<br/>

:::tip 傾向を視覚化する
`ファネル傾向`を選択して、時間の経過とともにファネルの傾向を視覚化します。
:::

## 8. リテンションクエリを実行する

リテンション率計算のために最大2ステップを選択します。再帰的なウィンドウを選択するためのリテンションウィンドウを選択します。
単一のSQLコードを書くことなく、すぐにコンバージョン率の洞察を得ることができます。

<img src={require('./images/mitzu_09.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="リテンション" ></img>

<br/>

:::tip コホートリテンション
`週間コホートリテンション`を選択して、リテンション率が時間の経過とともにどのように変化しているかを視覚化します。
:::

## 9. SQLネイティブ

MitzuはSQLネイティブです。これにより、Exploreページで選択した構成に基づいてネイティブSQLコードを生成します。

<img src={require('./images/mitzu_10.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="SQLネイティブ" ></img>

<br/>

:::tip BIツールで作業を続ける
MitzuのUIで制限に達した場合、SQLコードをコピーしてBIツールで作業を続けることができます。
:::

## 10. イベントテーブルを追加する

複数のテーブルに製品使用イベントを保存している場合、それらをイベントカタログにも追加できます。
ページ上部のギアアイコンからワークスペース設定ページに移動し、イベントテーブルタブを選択します。

ClickHouseウェアハウスから残りのイベントテーブルを追加します。

<img src={require('./images/mitzu_11.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="追加のテーブル" ></img>

<br/>

すべての他のイベントテーブルをワークスペースに追加したら、それらも設定する必要があります。
**ユーザーID**、**イベント時間**、およびオプションで**イベント名**カラムを設定します。

<img src={require('./images/mitzu_12.png').default} class="image" style={{width: '50%', 'background-color': 'transparent'}} alt="テーブルを設定" ></img>

設定ボタンをクリックし、一括でこれらのカラムを設定します。
Mitzuには最大5000テーブルを追加できます。

最後に、**イベントカタログを保存して更新**することを忘れないでください。

## Mitzuサポート

迷った場合は、[support@mitzu.io](email://support@mitzu.io)までお気軽にお問い合わせください。

または、Slackコミュニティに[こちら](https://join.slack.com/t/mitzu-io/shared_invite/zt-1h1ykr93a-_VtVu0XshfspFjOg6sczKg)から参加できます。

## 詳細を学ぶ

Mitzuに関する詳細情報は[mitzu.io](https://mitzu.io)をご覧ください。

ドキュメントページは[docs.mitzu.io](https://docs.mitzu.io)に訪問してください。
