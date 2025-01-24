---
slug: /ja/operations/caches
sidebar_position: 65
sidebar_label: キャッシュ
title: "キャッシュタイプ"
description: クエリを実行する際、ClickHouseは異なるキャッシュを使用します。
---

クエリを実行する際、ClickHouseは異なるキャッシュを使用します。

主なキャッシュタイプ：

- `mark_cache` — [MergeTree](../engines/table-engines/mergetree-family/mergetree.md)ファミリのテーブルエンジンで使用されるマークのキャッシュ。
- `uncompressed_cache` — [MergeTree](../engines/table-engines/mergetree-family/mergetree.md)ファミリのテーブルエンジンで使用される、非圧縮データのキャッシュ。
- オペレーティングシステムのページキャッシュ（実際のデータを含むファイルに対して間接的に使用されます）。

追加のキャッシュタイプ：

- DNSキャッシュ。
- [Regexp](../interfaces/formats.md#data-format-regexp)キャッシュ。
- コンパイルされた式のキャッシュ。
- [Avroフォーマット](../interfaces/formats.md#data-format-avro)スキーマのキャッシュ。
- [Dictionary](../sql-reference/dictionaries/index.md)データキャッシュ。
- スキーマ推論キャッシュ。
- S3、Azure、ローカルディスクなどに対する[ファイルシステムキャッシュ](storing-data.md)。
- [クエリキャッシュ](query-cache.md)。

キャッシュの一つを削除するには、[SYSTEM DROP ... CACHE](../sql-reference/statements/system.md#drop-mark-cache)ステートメントを使用します。
