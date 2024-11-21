---
sidebar_label: Deepnote
sidebar_position: 11
slug: /ja/integrations/deepnote
keywords: [clickhouse, Deepnote, connect, integrate, notebook]
description: 非常に大きなデータセットを効率的にクエリし、使い慣れたノートブック環境で分析とモデリングを行う。
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# ClickHouse を Deepnote に接続する

<a href="https://www.deepnote.com/" target="_blank">Deepnote</a> は、チームが発見や知見を共有するために構築された協力型データノートブックです。Jupyter 互換であることに加えて、クラウドで動作し、データサイエンスプロジェクトを効率的に進めるための中央集権的な場所を提供します。

このガイドは、すでに Deepnote アカウントがあり、稼働中の ClickHouse インスタンスをお持ちであることを前提としています。

## インタラクティブな例
Deepnote のデータノートブックから ClickHouse にクエリを実行するインタラクティブな例をお探しの場合は、以下のボタンをクリックして、[ClickHouse プレイグラウンド](../../getting-started/playground.md)に接続されたテンプレートプロジェクトを起動してください。

[<img src="https://deepnote.com/buttons/launch-in-deepnote.svg"/>](https://deepnote.com/launch?template=ClickHouse%20and%20Deepnote)

## ClickHouse に接続する

1. Deepnote で「Integrations」概要を選択し、ClickHouse タイルをクリックします。

<img src={require('./images/deepnote_01.png').default} class="image" alt="ClickHouse integration tile" style={{width: '100%'}}/>

2. ClickHouse インスタンスの接続詳細を入力します:
<ConnectionDetails />

   <img src={require('./images/deepnote_02.png').default} class="image" alt="ClickHouse details dialog" style={{width: '100%'}}/>

   **_注意:_** ClickHouse への接続が IP アクセスリストで保護されている場合、Deepnote の IP アドレスを許可する必要があるかもしれません。詳細は [Deepnote のドキュメント](https://docs.deepnote.com/integrations/authorize-connections-from-deepnote-ip-addresses)を参照してください。
3. おめでとうございます！これで ClickHouse が Deepnote に統合されました。

## ClickHouse 統合の使用方法

1. ノートブックの右側で ClickHouse 統合に接続を開始します。

   <img src={require('./images/deepnote_03.png').default} class="image" alt="ClickHouse details dialog" style={{width: '100%'}}/>
2. 次に、新しい ClickHouse クエリブロックを作成し、データベースにクエリを実行します。クエリ結果はデータフレームとして保存され、SQL ブロックで指定された変数に格納されます。
3. また、既存の [SQL ブロック](https://docs.deepnote.com/features/sql-cells) を ClickHouse ブロックに変換することもできます。
