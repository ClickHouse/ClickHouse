#pragma once

#include "config.h"

#include <Common/CurrentMetrics.h>

#include <memory>
#include <string>

#if USE_ROCKSDB

#include <Common/CacheBase.h>
#include <Common/SipHash.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeySSTProbe.h>

#include <base/types.h>


namespace DB
{

/// Read buffer of a cached reader: it only does `readBigAt`, but local
/// buffers are allocated at construction.
static constexpr size_t SST_READER_BUFFER_SIZE = 4096;

/// Weight: the reader's resident size (`index_size + filter_size`, preloaded at open
/// because `no_block_cache` forces `cache_index_and_filter_blocks` off) plus a fixed
/// per-reader cost. If a shared RocksDB block cache (`UniqueKeyIndexCache`) is ever
/// wired in with `cache_index_and_filter_blocks = true`, readers become lightweight
/// and this weight should drop to the fixed cost.
struct UniqueKeySSTReaderWeightFunction
{
    /// Fixed per-reader cost not in `memoryUsage`: the read buffer plus the
    /// RocksDB reader objects.
    static constexpr size_t UNIQUE_KEY_SST_READER_CACHE_OVERHEAD = SST_READER_BUFFER_SIZE + 4096;

    size_t operator()(const SSTFileReader & reader) const
    {
        return reader.memoryUsage() + UNIQUE_KEY_SST_READER_CACHE_OVERHEAD;
    }
};

/// Cache key: the part's physical location (`disk:path` - a background move
/// preserves the UUID but not the location) plus the SST's content checksum.
/// A rebuilt SST gets a new hash, so stale entries are unreachable LRU garbage.
struct UniqueKeySSTReaderCacheKey
{
    String part_id;
    MergeTreeDataPartChecksum::uint128 file_hash;

    bool operator==(const UniqueKeySSTReaderCacheKey & other) const
    {
        return part_id == other.part_id && file_hash == other.file_hash;
    }
};

struct UniqueKeySSTReaderCacheKeyHash
{
    size_t operator()(const UniqueKeySSTReaderCacheKey & key) const
    {
        SipHash hash;
        hash.update(key.part_id.size());
        hash.update(key.part_id.data(), key.part_id.size());
        hash.update(key.file_hash);
        return hash.get64();
    }
};

/// Whether a caller with `caller` settings may use a cached reader opened with
/// the `shared` (server-level) settings. Compares every `ReadSettings` field a
/// shared reader cannot honor per caller (throttling, cache toggles and quotas,
/// read cancellation); buffer sizes and read-method knobs are fixed by the
/// loader and skipped. `remote_disk`: only remote parts use `remote_throttler`,
/// but they may also read through the local fs cache.
inline bool canShareSSTReaderSettings(const ReadSettings & caller, const ReadSettings & shared, bool remote_disk)
{
    if (caller.local_throttler != shared.local_throttler
        || (remote_disk && caller.remote_throttler != shared.remote_throttler))
        return false;

    if (caller.enable_filesystem_cache != shared.enable_filesystem_cache
        || caller.filesystem_cache_settings.read_if_exists_otherwise_bypass != shared.filesystem_cache_settings.read_if_exists_otherwise_bypass
        || caller.filesystem_cache_settings.allow_background_download != shared.filesystem_cache_settings.allow_background_download
        || caller.filesystem_cache_settings.max_download_size_per_query != shared.filesystem_cache_settings.max_download_size_per_query
        || caller.filesystem_cache_settings.skip_download_if_exceeds_per_query_cache_write_limit != shared.filesystem_cache_settings.skip_download_if_exceeds_per_query_cache_write_limit)
        return false;

    if (caller.use_page_cache_for_disks_without_file_cache != shared.use_page_cache_for_disks_without_file_cache
        || caller.use_page_cache_with_distributed_cache != shared.use_page_cache_with_distributed_cache
        || caller.use_page_cache_for_local_disks != shared.use_page_cache_for_local_disks
        || caller.use_page_cache_for_object_storage != shared.use_page_cache_for_object_storage
        || caller.page_cache_settings.read_if_exists_otherwise_bypass != shared.page_cache_settings.read_if_exists_otherwise_bypass)
        return false;

    return caller.read_through_distributed_cache == shared.read_through_distributed_cache
        && caller.remote_fs_settings.interruptible_reads == shared.remote_fs_settings.interruptible_reads;
}

/// Process-wide cache of opened UNIQUE KEY SST readers, keyed by part identity ⊕ SST checksum.
class UniqueKeySSTReaderCache
    : public CacheBase<UniqueKeySSTReaderCacheKey, const SSTFileReader, UniqueKeySSTReaderCacheKeyHash, UniqueKeySSTReaderWeightFunction>
{
private:
    using Base = CacheBase<UniqueKeySSTReaderCacheKey, const SSTFileReader, UniqueKeySSTReaderCacheKeyHash, UniqueKeySSTReaderWeightFunction>;

public:
    UniqueKeySSTReaderCache(
        const String & cache_policy,
        CurrentMetrics::Metric size_in_bytes_metric,
        CurrentMetrics::Metric count_metric,
        size_t max_size_in_bytes,
        double size_ratio)
        : Base(cache_policy, size_in_bytes_metric, count_metric, max_size_in_bytes, /*max_count=*/0, size_ratio)
    {
    }

    static UniqueKeySSTReaderCacheKey makeKey(const String & part_id, MergeTreeDataPartChecksum::uint128 file_hash)
    {
        return UniqueKeySSTReaderCacheKey{part_id, file_hash};
    }
};

using UniqueKeySSTReaderCachePtr = std::shared_ptr<UniqueKeySSTReaderCache>;

}

#else

namespace DB
{

/// Stub type so Context.h can forward-declare a consistent shape.
class UniqueKeySSTReaderCache;
using UniqueKeySSTReaderCachePtr = std::shared_ptr<UniqueKeySSTReaderCache>;

}

#endif
