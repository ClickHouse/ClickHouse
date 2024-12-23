---
slug: /ja/cloud/manage/postman
sidebar_label: Postmanを使用したプログラムによるAPIアクセス
title: Postmanを使用したプログラムによるAPIアクセス
---

このガイドでは、[Postman](https://www.postman.com/product/what-is-postman/)を使用してClickHouse Cloud APIをテストする方法を紹介します。Postmanアプリケーションは、Webブラウザ内で使用するか、デスクトップにダウンロードして使用できます。

### アカウントを作成
* 無料のアカウントは[https://www.postman.com](https://www.postman.com)で利用できます。
![Postmanサイト](@site/docs/ja/cloud/manage/images/postman/postman1.png)

### ワークスペースを作成
* ワークスペースに名前を付け、表示レベルを設定します。
![ワークスペースを作成](@site/docs/ja/cloud/manage/images/postman/postman2.png)

### コレクションを作成
* 左上のメニューの「Explore」以下で「Import」をクリックします。
![Explore > Import](@site/docs/ja/cloud/manage/images/postman/postman3.png)

* モーダルが表示されます。
![API URLの入力](@site/docs/ja/cloud/manage/images/postman/postman4.png)

* APIアドレス「https://api.clickhouse.cloud/v1」を入力し、'Enter'を押します。
![インポート](@site/docs/ja/cloud/manage/images/postman/postman5.png)

* 「Import」ボタンをクリックして「Postman Collection」を選択します。
![コレクション > Import](@site/docs/ja/cloud/manage/images/postman/postman6.png)

### ClickHouse Cloud API仕様とインターフェース
* 「ClickHouse CloudのAPI仕様」が「Collections」内に表示されます（左ナビゲーション）。
![APIのインポート](@site/docs/ja/cloud/manage/images/postman/postman7.png)

* 「ClickHouse CloudのAPI仕様」をクリックします。中央のウィンドウから「Authorization」タブを選択します。
![インポート完了](@site/docs/ja/cloud/manage/images/postman/postman8.png)

### 認証を設定
* ドロップダウンメニューを切り替えて「Basic Auth」を選択します。
![Basic auth](@site/docs/ja/cloud/manage/images/postman/postman9.png)

* ClickHouse Cloud APIキーを設定したときに受け取ったユーザー名とパスワードを入力します。
![クレデンシャル](@site/docs/ja/cloud/manage/images/postman/postman10.png)

### 変数を有効化
* [変数](https://learning.postman.com/docs/sending-requests/variables/)を利用すると、Postmanで値を保存して再利用でき、APIテストが容易になります。
#### 組織IDとサービスIDを設定
* 「Collection」内で、中央のペインにある「Variable」タブをクリックします（ベースURLは先ほどのAPIインポートによって設定されています）。
* 「baseURL」以下で「Add new value」と書かれたフィールドを開き、組織IDとサービスIDを代入します。
![組織IDとサービスID](@site/docs/ja/cloud/manage/images/postman/postman11.png)

## ClickHouse Cloud API機能のテスト
### "GET list of available organizations"のテスト
* 「OpenAPI spec for ClickHouse Cloud」以下でフォルダを展開し、> V1 > organizationsを選択します。
* 「GET list of available organizations」をクリックし、右の青い「Send」ボタンを押します。
![組織の取得テスト](@site/docs/ja/cloud/manage/images/postman/postman12.png)
* 戻り結果は「status: 200」とともに組織の詳細を返すべきです。（組織情報が表示されず「status: 400」を受け取った場合、設定が正しくありません）。
![ステータス](@site/docs/ja/cloud/manage/images/postman/postman13.png)

### "GET organizational details"のテスト
* organizationidフォルダ以下で「GET organizational details」に移動します。
* 中央フレームのメニューでParamsの下に、organizationidが必要です。
![組織詳細の取得テスト](@site/docs/ja/cloud/manage/images/postman/postman14.png)
* この値を中括弧で囲まれた"{{orgid}}"に編集します（この値を以前に設定したため、値が表示されるメニューが現れます）。
![テストの提出](@site/docs/ja/cloud/manage/images/postman/postman15.png)
* 「Save」ボタンを押した後、画面右上の青い「Send」ボタンを押します。
![返り値](@site/docs/ja/cloud/manage/images/postman/postman16.png)
* 戻り結果は「status: 200」とともに組織の詳細を返すべきです。（組織情報が表示されず「status: 400」を受け取った場合、設定が正しくありません）。

### "GET service details"のテスト
* 「GET service details」をクリックします。
* organizationidおよびserviceidの値をそれぞれ{{orgid}}と{{serviceid}}に編集します。
* 「Save」を押し、その後右の青い「Send」ボタンを押します。
![サービスのリスト](@site/docs/ja/cloud/manage/images/postman/postman17.png)
* 戻り結果は「status: 200」とともにサービスの一覧とその詳細を返すべきです。（サービス情報が表示されず「status: 400」を受け取った場合、設定が正しくありません）。
