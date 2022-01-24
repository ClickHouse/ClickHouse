---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 29
toc_title: "\u30D7\u30ED\u30AD\u30B7"
---

# サードパーテ {#proxy-servers-from-third-party-developers}

## chproxy {#chproxy}

[chproxy](https://github.com/Vertamedia/chproxy),は、ClickHouseデータベース用のHTTPプロキシとロードバランサです。

特徴:

-   ユーザーごとのルーティ
-   適用範囲が広い限界。
-   SSL証明書の自動renewal。

Goで実装。

## キッテンハウス {#kittenhouse}

[キッテンハウス](https://github.com/VKCOM/kittenhouse) することが重要である現地代理人とClickHouse、アプリケーションサーバの場合でもバッファに挿入データとお客様側です。

特徴:

-   メモリ内およびディスク上のデータバッファリング。
-   テーブル単位のルーティング。
-   負荷分散と正常性チェック。

Goで実装。

## クリックハウス-バルク {#clickhouse-bulk}

[クリックハウス-バルク](https://github.com/nikepan/clickhouse-bulk) 単純なClickHouse挿入コレクターです。

特徴:

-   グループの要求送信によるしきい値または間隔で出ています。
-   複数のリモートサーバー。
-   基本認証。

Goで実装。

[元の記事](https://clickhouse.com/docs/en/interfaces/third-party/proxy/) <!--hide-->
