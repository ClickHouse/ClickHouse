---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 31
toc_title: "\u6020\u3051\u8005"
---

# 怠け者 {#lazy}

RAMのみのテーブルを保持します `expiration_time_in_seconds` 最後のアクセスの後の秒。 \*ログテーブルでのみ使用できます。

これは、アクセス間に長い時間間隔がある多くの小さな\*ログテーブルを格納するために最適化されています。

## データベースの作成 {#creating-a-database}

    CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);

[元の記事](https://clickhouse.tech/docs/en/database_engines/lazy/) <!--hide-->
