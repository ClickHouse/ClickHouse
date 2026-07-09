---
description: 'OSのページキャッシュに依存せず、
プロセス内メモリにデータをキャッシュできるキャッシュ機構。'
sidebar_label: 'ユーザー空間ページキャッシュ'
sidebar_position: 65
slug: /operations/userspace-page-cache
title: 'ユーザー空間ページキャッシュ'
doc_type: 'reference'
---

<div id="overview">
  ## 概要
</div>

> ユーザー空間ページキャッシュは、OSページキャッシュに依存せず、
> プロセス内メモリにデータをキャッシュできる新しいキャッシュ機構です。

ClickHouse にはすでに、Amazon S3、Google
Cloud ストレージ (GCS) 、Azure Blob Storage などのリモートオブジェクトストレージ向けのキャッシュ方式として、[ファイルシステムキャッシュ](/ja/docs/operations/storing-data)
があります。ユーザー空間ページキャッシュは、通常の OS キャッシュでは十分な効果が得られない場合に、リモートデータへのアクセスを高速化するために設計されています。

ファイルシステムキャッシュとの違いは次のとおりです。

| Filesystem Cache                  | Userspace page cache |
| --------------------------------- | -------------------- |
| データをローカルファイルシステムに書き込む             | メモリ内にのみ存在する          |
| ディスク容量を消費する (tmpfs 上に設定することも可能)   | ファイルシステムに依存しない       |
| サーバーの再起動後も保持される                   | サーバーの再起動後は保持されない     |
| サーバーのメモリ使用量には表示されない               | サーバーのメモリ使用量に表示される    |
| ディスク上とメモリ内 (OSページキャッシュ) の両方に適している | **ディスクレスサーバーに適している** |

<div id="configuration-settings-and-usage">
  ## 設定と使用状況
</div>

<div id="usage">
  ### 使い方
</div>

ユーザー空間ページキャッシュを有効にするには、まずサーバー側で設定を行います。

```bash
cat config.d/page_cache.yaml
page_cache_max_size: 100G
```

:::note
ユーザー空間ページキャッシュは、指定された量までのメモリを使用できますが、
このメモリ量が予約されるわけではありません。サーバーの他の用途で必要になった場合は、
このメモリは解放されます。
:::

次に、クエリレベルでこれを有効にします。

```sql
SET use_page_cache_for_disks_without_file_cache=1;
```

<div id="settings">
  ### 設定
</div>

| Setting                                                 | Description                                                                                                                                                                                        | Default     |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `use_page_cache_for_disks_without_file_cache`           | ファイルシステムキャッシュが有効でないリモートディスクに対して、ユーザー空間ページキャッシュを使用します。                                                                                                                                              | `0`         |
| `use_page_cache_with_distributed_cache`                 | 分散キャッシュを使用している場合に、ユーザー空間ページキャッシュを使用します。                                                                                                                                                            | `0`         |
| `read_from_page_cache_if_exists_otherwise_bypass_cache` | [`read_from_filesystem_cache_if_exists_otherwise_bypass_cache`](/ja/docs/operations/settings/settings#read_from_filesystem_cache_if_exists_otherwise_bypass_cache) と同様に、パッシブモードでユーザー空間ページキャッシュを使用します。 | `0`         |
| `page_cache_inject_eviction`                            | ユーザー空間ページキャッシュが、ページの一部をランダムに無効化することがあります。テスト用途を想定した設定です。                                                                                                                                           | `0`         |
| `page_cache_block_size`                                 | ユーザー空間ページキャッシュに保存するファイル chunk のサイズ (バイト単位) です。キャッシュを経由するすべての読み取りは、このサイズの倍数に切り上げられます。                                                                                                               | `1048576`   |
| `page_cache_history_window_ms`                          | 解放されたメモリをユーザー空間ページキャッシュで再利用できるようになるまでの遅延です。                                                                                                                                                        | `1000`      |
| `page_cache_policy`                                     | ユーザー空間ページキャッシュのポリシー名です。                                                                                                                                                                            | `SLRU`      |
| `page_cache_size_ratio`                                 | ユーザー空間ページキャッシュ内の保護キューのサイズが、キャッシュ全体のサイズに対して占める比率です。                                                                                                                                                 | `0.5`       |
| `page_cache_min_size`                                   | ユーザー空間ページキャッシュの最小サイズです。                                                                                                                                                                            | `104857600` |
| `page_cache_max_size`                                   | ユーザー空間ページキャッシュの最大サイズです。キャッシュを無効にするには 0 に設定します。`page_cache_min_size` より大きい場合は、総メモリ使用量を制限値 (`max_server_memory_usage`[`_to_ram_ratio`]) 未満に保ちながら使用可能なメモリの大部分を使えるよう、この範囲内でキャッシュサイズが継続的に調整されます。        | `0`         |
| `page_cache_free_memory_ratio`                          | ユーザー空間ページキャッシュで使用せずに空けておくメモリ制限の割合です。Linux の min&#95;free&#95;kbytes 設定に相当します。                                                                                                                      | `0.15`      |
| `page_cache_lookahead_blocks`                           | ユーザー空間ページキャッシュでキャッシュミスが発生した場合、基盤ストレージから、キャッシュにも存在しない連続した最大この数の block を一度に読み取ります。各 block は `page_cache_block_size` バイトです。                                                                           | `16`        |
| `page_cache_shards`                                     | mutex の競合を減らすため、ユーザー空間ページキャッシュをこの数の分片にストライプします。実験的な機能であり、パフォーマンス改善につながる可能性は高くありません。                                                                                                                | `4`         |

<div id="related-content">
  ## 関連コンテンツ
</div>

* [ファイルシステムキャッシュ](/ja/docs/operations/storing-data)
* [ClickHouse v25.3 リリースウェビナー](https://www.youtube.com/live/iCKEzp0_Z2Q?feature=shared\&t=1320)