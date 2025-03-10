---
slug: /ja/engines/database-engines/lazy
sidebar_label: Lazy
sidebar_position: 20
---

# Lazy

最後にアクセスされてから `expiration_time_in_seconds` 秒間のみテーブルをRAMに保持します。これは、\*Log テーブルのみで使用可能です。

アクセス間の時間間隔が長い小さな \*Log テーブルを多数保存するのに最適化されています。

## データベースの作成 {#creating-a-database}

    CREATE DATABASE testlazy ENGINE = Lazy(expiration_time_in_seconds);
