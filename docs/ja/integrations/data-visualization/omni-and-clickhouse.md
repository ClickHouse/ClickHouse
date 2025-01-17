---
sidebar_label: Omni
slug: /ja/integrations/omni
keywords: [clickhouse, omni, connect, integrate, ui]
description: Omniは、BI、データアプリケーション、埋め込み分析のためのエンタープライズプラットフォームで、リアルタイムでインサイトを探索し共有するのに役立ちます。
---

import ConnectionDetails from '@site/docs/ja/\_snippets/\_gather_your_details_http.mdx';

# Omni

Omniは、公式のClickHouseデータソースを通じて、ClickHouse Cloudまたはオンプレミス展開に接続することができます。

## 1. 接続情報を収集する

<ConnectionDetails />

## 2. ClickHouseデータソースを作成する

Admin -> Connectionsに移動し、右上の「Add Connection」ボタンをクリックします。

<img src={require('./images/omni_01.png').default} class="image" alt="新しい接続の追加" style={{width: '80%', 'background-color': 'transparent'}}/>
<br/>

`ClickHouse`を選択し、フォームに資格情報を入力します。

<img src={require('./images/omni_02.png').default} class="image" alt="資格情報の指定" style={{width: '80%', 'background-color': 'transparent'}}/>
<br/>

これで、OmniでClickHouseからデータのクエリを実行し、可視化できるようになります。
