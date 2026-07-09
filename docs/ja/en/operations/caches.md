---
description: 'クエリの実行時に、ClickHouse はさまざまなキャッシュを使用します。'
sidebar_label: 'キャッシュ'
sidebar_position: 65
slug: /operations/caches
title: 'キャッシュの種類'
keywords: ['cache']
doc_type: 'reference'
---

クエリの実行時に、ClickHouse はさまざまなキャッシュを使用してクエリを高速化し、
ディスクへの読み書きの回数を減らします。

主なキャッシュの種類は次のとおりです。

* `mark_cache` — [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md) ファミリーのテーブルエンジンで使用される [marks](/ja/development/architecture#merge-tree) のキャッシュ。
* `uncompressed_cache` — [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md) ファミリーのテーブルエンジンで使用される非圧縮データのキャッシュ。
* オペレーティングシステムのページキャッシュ (実データを含むファイルに対して間接的に使用されます) 。

このほかにも、さまざまなキャッシュがあります。

* DNS キャッシュ。
* [Regexp](/ja/interfaces/formats/Regexp) キャッシュ。
* コンパイル済み式のキャッシュ。
* [Vector similarity index](../engines/table-engines/mergetree-family/annindexes.md) キャッシュ。
* [Text index](../engines/table-engines/mergetree-family/textindexes.md#caching) キャッシュ。
* [Avro format](/ja/interfaces/formats/Avro) のスキーマキャッシュ。
* [Dictionaries](../sql-reference/statements/create/dictionary/overview.md) のデータキャッシュ。
* スキーマ推論キャッシュ。
* S3、Azure、Local、その他のディスク上の [ファイルシステムキャッシュ](storing-data.md)。
* [Userspace page cache](/ja/operations/userspace-page-cache)
* [Query cache](query-cache.md)。
* [Query condition cache](query-condition-cache.md)。
* フォーマットスキーマキャッシュ。

パフォーマンスチューニング、トラブルシューティング、またはデータ整合性上の理由で
いずれかのキャッシュをクリアしたい場合は、[`SYSTEM CLEAR ... CACHE`](../sql-reference/statements/system.md) ステートメントを使用できます。