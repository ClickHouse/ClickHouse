---
description: '一种缓存机制，可将
数据缓存在进程内内存中，而非依赖操作系统页缓存。'
sidebar_label: '用户态页缓存'
sidebar_position: 65
slug: /operations/userspace-page-cache
title: '用户态页缓存'
doc_type: '参考'
---

<div id="overview">
  ## 概述
</div>

> 用户态页缓存是一种新的缓存机制，可将
> 数据缓存在进程内内存中，而不是依赖 OS 页缓存。

ClickHouse 已经提供了[文件系统缓存](/zh/docs/operations/storing-data)，
可作为远程对象存储 (如亚马逊 S3、Google
Cloud Storage (GCS) 或 Azure Blob 存储) 之上的一层缓存。用户态页缓存旨在
当常规 OS 缓存效果不够理想时，加快对远程数据的访问。

它与文件系统缓存的区别如下：

| 文件系统缓存                  | 用户态页缓存        |
| ----------------------- | ------------- |
| 将数据写入本地文件系统             | 仅存在于内存中       |
| 占用磁盘空间 (也可配置在 tmpfs 上)  | 不依赖文件系统       |
| 在服务器重启后仍然保留             | 服务器重启后不会保留    |
| 不会显示在服务器的内存使用中          | 会显示在服务器的内存使用中 |
| 适用于磁盘和内存 (OS 页缓存)       | **适合无盘服务器**   |

<div id="configuration-settings-and-usage">
  ## 配置设置与使用
</div>

<div id="usage">
  ### 使用方法
</div>

要启用用户态页缓存，首先需要在服务器上进行配置：

```bash
cat config.d/page_cache.yaml
page_cache_max_size: 100G
```

:::note
用户态页缓存最多会使用指定数量的内存，但
这部分内存不会被预留。当 server 因其他用途需要内存时，
这些内存会被回收。
:::

接下来，在查询级别启用它：

```sql
SET use_page_cache_for_disks_without_file_cache=1;
```

<div id="settings">
  ### 设置
</div>

| Setting                                                 | Description                                                                                                                                                                        | Default     |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `use_page_cache_for_disks_without_file_cache`           | 对未启用文件系统缓存的远程磁盘使用用户态页缓存。                                                                                                                                                           | `0`         |
| `use_page_cache_with_distributed_cache`                 | 使用分布式缓存时启用用户态页缓存。                                                                                                                                                                  | `0`         |
| `read_from_page_cache_if_exists_otherwise_bypass_cache` | 以被动模式使用用户态页缓存，类似于 [`read_from_filesystem_cache_if_exists_otherwise_bypass_cache`](/zh/docs/operations/settings/settings#read_from_filesystem_cache_if_exists_otherwise_bypass_cache)。 | `0`         |
| `page_cache_inject_eviction`                            | 用户态页缓存有时会随机使部分页面失效。用于测试。                                                                                                                                                           | `0`         |
| `page_cache_block_size`                                 | 存储到用户态页缓存中的文件块大小，以字节为单位。所有经过缓存的读取都会向上取整到该大小的整数倍。                                                                                                                                   | `1048576`   |
| `page_cache_history_window_ms`                          | 已释放内存在可供用户态页缓存使用前的延迟时间。                                                                                                                                                            | `1000`      |
| `page_cache_policy`                                     | 用户态页缓存策略的名称。                                                                                                                                                                       | `SLRU`      |
| `page_cache_size_ratio`                                 | 用户态页缓存中受保护队列的大小占缓存总大小的比例。                                                                                                                                                          | `0.5`       |
| `page_cache_min_size`                                   | 用户态页缓存的最小大小。                                                                                                                                                                       | `104857600` |
| `page_cache_max_size`                                   | 用户态页缓存的最大大小。设为 0 可禁用该缓存。如果大于 page&#95;cache&#95;min&#95;size，则缓存大小会在此范围内持续调整，在尽量利用可用内存的同时，将总内存使用量控制在限制以下 (`max_server_memory_usage`[`_to_ram_ratio`]) 。                            | `0`         |
| `page_cache_free_memory_ratio`                          | 需要保留、不供用户态页缓存使用的 memory limit 比例。类似于 Linux 的 min&#95;free&#95;kbytes 设置。                                                                                                           | `0.15`      |
| `page_cache_lookahead_blocks`                           | 发生用户态页缓存未命中时，如果后续连续块也不在缓存中，则会一次性从底层存储读取最多这么多个连续块。每个块的大小为 page&#95;cache&#95;block&#95;size 字节。                                                                                     | `16`        |
| `page_cache_shards`                                     | 将用户态页缓存分散到这么多个分片上，以减少 mutex 争用。Experimental，预计不太可能提升性能。                                                                                                                            | `4`         |

<div id="related-content">
  ## 相关内容
</div>

* [文件系统缓存](/zh/docs/operations/storing-data)
* [ClickHouse v25.3 版本在线研讨会](https://www.youtube.com/live/iCKEzp0_Z2Q?feature=shared\&t=1320)