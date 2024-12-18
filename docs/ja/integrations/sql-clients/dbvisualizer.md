---
sidebar_label: DbVisualizer
slug: /ja/integrations/dbvisualizer
description: DbVisualizerはClickHouseに対する拡張サポートを備えたデータベースツールです。
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# DbVisualizerをClickHouseに接続

## DbVisualizerの開始またはダウンロード

DbVisualizerは https://www.dbvis.com/download/ で入手可能です。

## 1. 接続詳細を集める

<ConnectionDetails />

## 2. 組み込みJDBCドライバ管理

DbVisualizerには、ClickHouse用の最新のJDBCドライバが含まれています。最新のリリースや過去のバージョンへの完全なJDBCドライバ管理が組み込まれています。

![](@site/docs/ja/integrations/sql-clients/images/dbvisualizer-driver-manager.png)

## 3. ClickHouseに接続

DbVisualizerでデータベースに接続するには、まずデータベース接続を作成し設定する必要があります。

1. **Database->Create Database Connection**から新しい接続を作成し、ポップアップメニューからデータベースのドライバを選びます。

2. 新しい接続のための**Object View**タブが開かれます。

3. **Name**フィールドに接続の名前を入力し、オプションで**Notes**フィールドに接続の説明を入力します。

4. **Database Type**は**Auto Detect**のままにしておきます。

5. **Driver Type**内で選択したドライバに緑のチェックマークが付いている場合、それは使用可能です。緑のチェックマークが付いていない場合は、**Driver Manager**でドライバを構成する必要があるかもしれません。

6. 残りのフィールドにデータベースサーバーの情報を入力します。

7. **Ping Server**ボタンをクリックして、指定されたアドレスとポートにネットワーク接続を確立できることを確認します。

8. Ping Serverの結果がサーバーに到達できることを示している場合は、**Connect**をクリックしてデータベースサーバーに接続します。

:::tip
データベースへの接続に問題がある場合は、[Fixing Connection Issues](https://confluence.dbvis.com/display/UG231/Fixing+Connection+Issues)を参照してください。

## 詳細情報

DbVisualizerの詳細については、[DbVisualizer のドキュメント](https://confluence.dbvis.com/display/UG231/Users+Guide)を参照してください。
