---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 29
toc_title: "\u30D7\u30ED\u30AD\u30B7"
---

# サードパーティ開発者のプロキシサーバー {#proxy-servers-from-third-party-developers}

## chproxy {#chproxy}

[chproxy](https://github.com/Vertamedia/chproxy)、ClickHouseデータベース用のHTTPプロキシとロードバランサです。

特徴:

-   ユーザー単位のルーティ
-   柔軟な制限。
-   自動ssl証明書の更新。

Goで実装されています。

## KittenHouse {#kittenhouse}

[KittenHouse](https://github.com/VKCOM/kittenhouse) することが重要である現地代理人とClickHouse、アプリケーションサーバの場合でもバッファに挿入データとお客様側です。

特徴:

-   メモリ内およびディスク上のデータバッファリング。
-   テーブル単位のルーティング。
-   負荷分散とヘルスチェック。

Goで実装されています。

## クリックハウス-バルク {#clickhouse-bulk}

[クリックハウス-バルク](https://github.com/nikepan/clickhouse-bulk) 簡単なClickHouseの挿入物のコレクターはある。

特徴:

-   グループの要求送信によるしきい値または間隔で出ています。
-   複数のリモートサーバー。
-   基本認証。

Goで実装されています。

[元の記事](https://clickhouse.tech/docs/en/interfaces/third-party/proxy/) <!--hide-->
