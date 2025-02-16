---
slug: /ja/faq/operations/deploy-separate-storage-and-compute
title: ClickHouseをストレージとコンピュートを分離してデプロイすることは可能ですか？
sidebar_label: ClickHouseをストレージとコンピュートを分離してデプロイすることは可能ですか？
toc_hidden: true
toc_priority: 20
---

簡潔に言うと、「はい」です。

オブジェクトストレージ (S3, GCS) は、ClickHouseテーブルのデータの弾性主記憶バックエンドとして使用できます。[S3サポートのMergeTree](/docs/ja/integrations/data-ingestion/s3/index.md) と [GCSサポートのMergeTree](/docs/ja/integrations/data-ingestion/gcs/index.md) のガイドが公開されています。この構成では、メタデータのみがコンピュートノードにローカルで保存されます。このセットアップでは、追加のノードがメタデータのみをレプリケーションする必要があるため、コンピュートリソースを簡単にスケールアップおよびスケールダウンできます。
