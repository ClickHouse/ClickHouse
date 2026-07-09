---
description: 'ClickHouse で利用できるサードパーティ製のプロキシソリューションについて説明します'
sidebar_label: 'プロキシ'
sidebar_position: 29
slug: /interfaces/third-party/proxy
title: 'サードパーティ製のプロキシサーバー'
doc_type: 'reference'
---

<div id="chproxy">
  ## chproxy
</div>

[chproxy](https://github.com/Vertamedia/chproxy) は、ClickHouse database 向けの HTTP プロキシ兼ロードバランサーです。

特長:

* ユーザーごとのルーティングとレスポンスのキャッシュ。
* 柔軟な制限設定。
* SSL証明書の自動更新。

Go で実装されています。

<div id="kittenhouse">
  ## KittenHouse
</div>

[KittenHouse](https://github.com/VKCOM/kittenhouse) は、アプリケーション側で INSERT データをバッファリングできない場合や、それが難しい場合に、ClickHouse とアプリケーションサーバーの間で動作するローカルプロキシとして設計されています。

特長:

* メモリ内およびディスクへのデータバッファリング。
* テーブルごとのルーティング。
* 負荷分散とヘルスチェック。

Go で実装されています。

<div id="clickhouse-bulk">
  ## ClickHouse-Bulk
</div>

[ClickHouse-Bulk](https://github.com/nikepan/clickhouse-bulk) は、シンプルな ClickHouse の insert collector です。

機能:

* リクエストをグループ化し、しきい値または間隔に応じて送信します。
* 複数のリモートサーバーに対応。
* Basic 認証。

Go で実装されています。