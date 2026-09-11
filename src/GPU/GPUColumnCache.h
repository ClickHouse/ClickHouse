#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUAggregation.h>

#include <base/UUID.h>
#include <Common/CacheBase.h>
#include <Storages/IStorage_fwd.h>

#include <memory>

namespace DB
{

/// Forward declared rather than included: this cache holds a part only to keep it alive and to
/// compare its address, and neither needs the definition - a `shared_ptr` can be copied and
/// destroyed with an incomplete element type. It also keeps `MergeTreeData.h`, which is one of the
/// heaviest headers in the tree, out of everything that reaches for the cache.
class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

/// Which column of which part of which table an entry holds.
///
/// The key does not decide whether a lookup is a hit - `GPUColumnCache::getForPart` also demands
/// that the entry was filled from the very part object the query is about to read. That is what
/// makes the key's uniqueness a performance question rather than a correctness one: a table
/// without a UUID (an `Ordinary` database) has `Nil` here, so two such tables can name the same
/// key with their `all_1_1_0`, and the consequence is that they evict each other rather than that
/// one of them answers with the other's data.
struct GPUColumnCacheKey
{
    UUID table_uuid;
    String part_name;
    String column_name;

    bool operator==(const GPUColumnCacheKey & other) const = default;
};

struct GPUColumnCacheKeyHash
{
    size_t operator()(const GPUColumnCacheKey & key) const;
};

/// One column of one part, in device memory.
///
/// The entry owns the part it was filled from, which is the whole invalidation protocol: a
/// `MergeTree` part never changes after it is written, so what is on the device cannot go stale
/// while that part exists - and holding the `DataPartPtr` is what keeps it existing. A merge, a
/// mutation or a TTL does not have to be detected, because it does not touch this part; it produces
/// a different one, which is a different object and therefore a different lookup.
///
/// The price is that the part's files stay on disk until the entry is evicted, even after the part
/// has been replaced by a merged one and every query has stopped looking at it. Eviction is what
/// releases them, and `gpu_column_cache_size` is what forces eviction. The alternative was a
/// protocol that told the cache when a part goes away - a subscription to `MergeTreeData`'s part
/// set, or a check of the part's state on every lookup - and both of those are ways to get a wrong
/// answer if they are ever incomplete, where this one cannot be.
struct GPUResidentColumn
{
    /// Allocates room for `num_rows_` values of `element_size` bytes on the device. The caller
    /// fills `buffer` afterwards; nothing here reads the part.
    GPUResidentColumn(ConstStoragePtr storage_, DataPartPtr data_part_, size_t num_rows_, size_t element_size);

    /// The table the part belongs to, held for as long as the part is - because destroying a part
    /// calls back into its `MergeTreeData` (`releaseSharedPartColumns`, the state metrics), and an
    /// entry can easily outlive the query that filled it and the table being dropped. This is the
    /// same reason `MergeTreeData::SnapshotData` holds a `ConstStoragePtr` next to its parts.
    ConstStoragePtr storage;

    /// The part these values came from, held so that it cannot be destroyed while they are cached -
    /// and so that its address, which is what a lookup compares, cannot be reused by another part.
    DataPartPtr data_part;

    /// How many values are in the buffer. The part's whole row count: this path caches whole parts
    /// or nothing.
    size_t num_rows;

    GPU::DeviceBuffer buffer;
};

struct GPUResidentColumnWeight
{
    /// Device bytes, which is what `gpu_column_cache_size` limits. The host-side overhead of an
    /// entry - the key's two strings, the part pointer, the cache's own nodes - is not counted,
    /// because it is host memory of a few hundred bytes against a column of megabytes, and
    /// counting it would make the limit mean something other than "device memory".
    size_t operator()(const GPUResidentColumn & column) const { return column.buffer.size(); }
};

/// Columns of `MergeTree` parts, held in GPU device memory between queries, so that a repeated
/// aggregation over them reads nothing: no disk, no decompression, no transfer over the link.
///
/// Sized by the server setting `gpu_column_cache_size`, in device bytes. That setting is the only
/// bound: device memory is not in `max_memory_usage`, not in `max_server_memory_usage`, and not in
/// the memory tracker at all, so nothing else in the server will notice this growing.
///
/// LRU rather than SLRU, unlike the mark cache. An entry here is one whole column of one whole
/// part, so the "scan once, never again" traffic that SLRU's probationary queue protects against
/// does not exist: every insertion is something a query asked to be able to sum again.
class GPUColumnCache : public CacheBase<GPUColumnCacheKey, GPUResidentColumn, GPUColumnCacheKeyHash, GPUResidentColumnWeight>
{
private:
    using Base = CacheBase<GPUColumnCacheKey, GPUResidentColumn, GPUColumnCacheKeyHash, GPUResidentColumnWeight>;

public:
    explicit GPUColumnCache(size_t max_size_in_bytes);

    /// Hidden in favour of `getForPart`, which is the only lookup that is safe: a hit has to be
    /// checked against the part it is about to answer for.
    MappedPtr get(const Key & key) = delete;

    /// The entry under `key` if it holds the values of this very `data_part` object, and nothing
    /// otherwise - which the caller treats as a miss and replaces.
    ///
    /// The comparison is of addresses, not of names. A part's name is not unique over time: drop a
    /// table and create it again, detach and attach a part, and `all_1_1_0` is a different part
    /// with the same name and possibly different data. Its address cannot be the same, because the
    /// entry holds a reference to the old part and so keeps that address taken.
    MappedPtr getForPart(const Key & key, const DataPartPtr & data_part);

    /// Puts `column` under `key`, replacing whatever was there. Do not hold the device lock across
    /// this: an insertion evicts, an eviction frees device buffers, and freeing takes that lock.
    ///
    /// Nothing coordinates two queries that miss on the same column at the same time: both read
    /// the part, both upload it, and the second insertion replaces the first - which costs a
    /// second read and, for as long as the first query is still summing, a second copy in device
    /// memory. `CacheBase::getOrSet` would serialize them, at the cost of the loser waiting for a
    /// read it could have done itself; that trade is worth making when it is a problem, and a
    /// prototype should not guess which way it goes.
    void setForPart(const Key & key, const MappedPtr & column);

private:
    void onEntryRemoval(size_t weight_loss, const MappedPtr & mapped_ptr) override;
};

using GPUColumnCachePtr = std::shared_ptr<GPUColumnCache>;

}

#endif
