---
title: オブジェクトストレージからClickHouse Cloudへ
description: オブジェクトストレージからClickHouse Cloudへのデータ移行
keywords: [オブジェクトストレージ, s3, azure blob, gcs, マイグレーション]
---

# クラウドオブジェクトストレージからClickHouse Cloudにデータを移動する

<img src={require('./images/object-storage-01.png').default} class="image" alt="セルフマネージドのClickHouseのマイグレーション" style={{width: '90%', padding: '30px'}}/>

クラウドオブジェクトストレージをデータレイクとして使用し、このデータをClickHouse Cloudにインポートしたい場合や、現在のデータベースシステムがデータをクラウドオブジェクトストレージに直接オフロードできる場合、Cloud Object Storageに格納されたデータをClickHouse Cloudのテーブルに移行するために、以下のテーブル関数を使用できます:

- [s3](/docs/ja/sql-reference/table-functions/s3.md) または [s3Cluster](/docs/ja/sql-reference/table-functions/s3Cluster.md)
- [gcs](/docs/ja/sql-reference/table-functions/gcs)
- [azureBlobStorage](/docs/ja/sql-reference/table-functions/azureBlobStorage)

現在のデータベースシステムがクラウドオブジェクトストレージにデータを直接オフロードできない場合は、[サードパーティのETL/ELTツール](./etl-tool-to-clickhouse.md) や [clickhouse-local](./clickhouse-local-etl.md) を使用して、現在のデータベースシステムからクラウドオブジェクトストレージにデータを移動し、その後、第二段階でClickHouse Cloudのテーブルにデータを移行することができます。

これは、データをクラウドオブジェクトストレージにオフロードし、その後ClickHouseにロードするという2段階のプロセスですが、この方法の利点は、クラウドオブジェクトストレージからの高度に並列化された読み込みをサポートする[堅牢なClickHouse Cloud](https://clickhouse.com/blog/getting-data-into-clickhouse-part-3-s3) により、ペタバイト規模までスケールできることです。また、[Parquet](https://clickhouse.com/docs/ja/interfaces/formats/#data-format-parquet) のような高度な圧縮フォーマットを活用することもできます。

S3を使用してClickHouse Cloudにデータを取り込む方法を具体的なコード例で示した[ブログ記事](https://clickhouse.com/blog/getting-data-into-clickhouse-part-3-s3)があります。
