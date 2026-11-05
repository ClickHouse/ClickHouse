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

/// Weight: the reader's resident size (`index_size + filter_size`, preloaded at open
/// because `no_block_cache` forces `cache_index_and_filter_blocks` off) plus a fixed
/// per-reader cost. If a shared RocksDB block cache (`UniqueKeyIndexCache`) is ever
/// wired in with `cache_index_and_filter_blocks = true`, readers become lightweight
/// and this weight should drop to the fixed cost.
struct UniqueKeySSTReaderWeightFunction
{
    /// Fixed per-reader cost not in `memoryUsage`: the RocksDB reader objects. A
    /// reader holds no read buffer.
    static constexpr size_t UNIQUE_KEY_SST_READER_CACHE_OVERHEAD = 4096;

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
