---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 31
toc_title: "\u6020\u3051\u8005"
---

# 怠け者 {#lazy}

RAM内のテーブルのみを保持 `expiration_time_in_seconds` 最後のアクセスの秒後。 \*Logテーブルでのみ使用できます。

これは、アクセス間に長い時間間隔がある多くの小さな\*ログテーブルを格納するために最適化されています。

## データベースの作成 {#creating-a-database}

    CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);

[元の記事](https://clickhouse.tech/docs/en/database_engines/lazy/) <!--hide-->
