---
description: 'caching mechanism that allows for caching of 
data in in-process memory rather than relying on the OS page cache.'
sidebar_label: 'Userspace page cache'
sidebar_position: 65
slug: /operations/userspace-page-cache
title: 'Userspace page cache'
---

# Userspace page cache

## Overview {#overview}

> The userspace page cache is a new caching mechanism that allows for caching of 
data in in-process memory rather than relying on the OS page cache.

ClickHouse already offers the [Filesystem cache](/docs/operations/storing-data) 
as a way of caching on top of remote object storage such as Amazon S3, Google 
Cloud Storage (GCS) or Azure Blob Storage. The userspace page cache is designed 
to speed up access to remote data when the normal OS caching isn't doing a good 
enough job. 

It differs from the filesystem cache in the following ways:

| Filesystem Cache                                        | Userspace page cache                  |
|---------------------------------------------------------|---------------------------------------|
| Writes data to the local filesystem                     | Present only in memory                |
| Takes up disk space (also configurable on tmpfs)        | Independent of filesystem             |
| Survives server restarts                                | Does not survive server restarts      |
| Does not show up in the server's memory usage           | Shows up in the server's memory usage |
| Suitable for both on-disk and in-memory (OS page cache) | **Good for disk-less servers**        |

## Configuration settings and usage {#configuration-settings-and-usage}

### Usage {#usage}

To enable the userspace page cache, first configure it on the server:

```bash
cat config.d/page_cache.yaml
page_cache_max_size: 100G
```

:::note
The userspace page cache will use up to the specified amount of memory, but
this memory amount is not reserved. The memory will be evicted when it is needed
for other server needs.
:::

Next, enable its usage on the query-level:

```sql
SET use_page_cache_for_disks_without_file_cache=1;
```

### Settings {#settings}

| Setting                                                  | Description                                                                                                                                                                                                                                                                                                            | Default     |
|----------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| `use_page_cache_for_disks_without_file_cache`            | Use userspace page cache for remote disks that don't have filesystem cache enabled.                                                                                                                                                                                                                                    | `0`         |
| `use_page_cache_with_distributed_cache`                  | Use userspace page cache when distributed cache is used.                                                                                                                                                                                                                                                               | `0`         |
| `read_from_page_cache_if_exists_otherwise_bypass_cache`  | Use userspace page cache in passive mode, similar to [`read_from_filesystem_cache_if_exists_otherwise_bypass_cache`](/docs/operations/settings/settings#read_from_filesystem_cache_if_exists_otherwise_bypass_cache).                                                                                                  | `0`         |
| `page_cache_inject_eviction`                             | Userspace page cache will sometimes invalidate some pages at random. Intended for testing.                                                                                                                                                                                                                             | `0`         |
| `page_cache_block_size`                                  | Size of file chunks to store in the userspace page cache, in bytes. All reads that go through the cache will be rounded up to a multiple of this size.                                                                                                                                                                 | `1048576`   |
| `page_cache_history_window_ms`                           | Delay before freed memory can be used by userspace page cache.                                                                                                                                                                                                                                                         | `1000`      |
| `page_cache_policy`                                      | Userspace page cache policy name.                                                                                                                                                                                                                                                                                      | `SLRU`      |
| `page_cache_size_ratio`                                  | The size of the protected queue in the userspace page cache relative to the cache\'s total size.                                                                                                                                                                                                                       | `0.5`       |
| `page_cache_min_size`                                    | Minimum size of the userspace page cache.                                                                                                                                                                                                                                                                              | `104857600` |
| `page_cache_max_size`                                    | Maximum size of the userspace page cache. Set to 0 to disable the cache. If greater than page_cache_min_size, the cache size will be continuously adjusted within this range, to use most of the available memory while keeping the total memory usage below the limit (`max_server_memory_usage`\[`_to_ram_ratio`\]). | `0`         |
| `page_cache_free_memory_ratio`                           | Fraction of the memory limit to keep free from the userspace page cache. Analogous to Linux min_free_kbytes setting.                                                                                                                                                                                                   | `0.15`      |
| `page_cache_lookahead_blocks`                            | On userspace page cache miss, read up to this many consecutive blocks at once from the underlying storage, if they\'re also not in the cache. Each block is page_cache_block_size bytes.                                                                                                                               | `16`        |
| `page_cache_shards`                                      | Stripe userspace page cache over this many shards to reduce mutex contention. Experimental, not likely to improve performance.                                                                                                                                                                                         | `4`         |

## Related content {#related-content}
- [Filesystem cache](/docs/operations/storing-data)
- [ClickHouse v25.3 Release Webinar](https://www.youtube.com/live/iCKEzp0_Z2Q?feature=shared&t=1320)
