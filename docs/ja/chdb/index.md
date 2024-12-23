---
title: chDB
sidebar_label: Overview
slug: /ja/chdb
description: chDBはClickHouseによって駆動されるインプロセスSQL OLAPエンジンです
keywords: [chdb, embedded, clickhouse-lite, in-process, in process]
---

# chDB

chDBは、[ClickHouse](https://github.com/clickhouse/clickhouse)によって駆動される高速なインプロセスSQL OLAPエンジンです。ClickHouseサーバーに接続することなく、プログラミング言語でClickHouseの力を得たい場合に使用できます。

## chDBはどの言語をサポートしていますか？

chDBは以下の言語バインディングを提供しています：

* [Python](install/python.md)
* [Go](install/go.md)
* [Rust](install/rust.md)
* [NodeJS](install/nodejs.md)
* [Bun](install/bun.md)

## どの入力および出力フォーマットがサポートされていますか？

chDBはParquet、CSV、JSON、Apache Arrow、ORC、[60以上のフォーマット](https://clickhouse.com/docs/ja/interfaces/formats)をサポートしています。

## どのように開始しますか？

* [Go](install/go.md)、[Rust](install/rust.md)、[NodeJS](install/nodejs.md)、または[Bun](install/bun.md)を使用している場合は、対応する言語ページをご覧ください。
* Pythonを使用している場合は、[開発者向けの開始ガイド](getting-started.md)を参照してください。以下のような一般的なタスクを行うためのガイドも用意されています：
    * [JupySQL](guides/jupysql.md)
    * [Pandasのクエリ](guides/querying-pandas.md)
    * [Apache Arrowへのクエリ](guides/querying-apache-arrow.md)
    * [S3内のデータへのクエリ](guides/querying-s3-bucket.md)
    * [Parquetファイルへのクエリ](guides/querying-parquet.md)
    * [リモートClickHouseへのクエリ](guides/query-remote-clickhouse.md)
    * [clickhouse-localデータベースの使用](guides/clickhouse-local.md)

<!-- ## What is chDB?

chDB lets you 

- Supports Python DB API 2.0: [example](https://github.com/chdb-io/chdb/blob/main/examples/dbapi.py) and [custom UDF Functions](https://github.com/chdb-io/chdb/blob/main/examples/udf.py) -->

## 紹介ビデオ

ClickHouseのオリジナル作成者、Alexey MilovidovによるchDBの簡潔なプロジェクト紹介をお聞きください：

<div class='vimeo-container'>
<iframe width="560" height="315" src="https://www.youtube.com/embed/cuf_hYn7dqU?si=SzUm7RW4Ae5-YwFo" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>
</div>

## chDBについて

- [Auxtenのブログ](https://clickhouse.com/blog/chdb-embedded-clickhouse-rocket-engine-on-a-bicycle)でchDBプロジェクト誕生の全過程を読む
- [公式ClickHouseブログ](https://clickhouse.com/blog/welcome-chdb-to-clickhouse)でchDBとその使用事例について読む
- [codapiの例](https://antonz.org/trying-chdb/)を使用してブラウザでchDBを発見する

## どのライセンスが使用されていますか？

chDBはApache License, Version 2.0のもとで利用可能です。
