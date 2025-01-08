---
sidebar_label: 概要
sidebar_position: 1
keywords: [clickhouse, 移行, マイグレーション, データ]
---

# ClickHouse へのデータ移行

<div class='vimeo-container'>
  <iframe src="https://player.vimeo.com/video/753082620?h=eb566c8c08"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
    fullscreen;
    picture-in-picture"
    allowfullscreen>
  </iframe>
</div>

<br/>

現在のデータの場所に応じて、ClickHouse Cloud にデータを移行するためのいくつかのオプションがあります。

- [セルフマネージドからクラウドへ](./clickhouse-to-cloud.md): `remoteSecure` 関数を使用してデータを転送
- [他のDBMS](./clickhouse-local-etl.md): [clickhouse-local] ETL ツールと現在のDBMS用の適切なClickHouseテーブル関数を使用
- [どこからでも！](./etl-tool-to-clickhouse.md): 様々なデータソースに接続できる人気のETL/ELTツールの一つを使用
- [オブジェクトストレージ](./object-storage-to-clickhouse.md): S3からClickHouseにデータを簡単に挿入

例として、[Redshiftからの移行](/docs/ja/integrations/data-ingestion/redshift/index.md)において、ClickHouseへのデータ移行の3つの方法を紹介しています。
