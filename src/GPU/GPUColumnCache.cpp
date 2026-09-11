#include <GPU/GPUColumnCache.h>

#if USE_GPU

#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>

namespace ProfileEvents
{
    extern const Event GPUColumnCacheHits;
    extern const Event GPUColumnCacheMisses;
    extern const Event GPUColumnCacheEvictedBytes;
    extern const Event GPUColumnCacheEvictions;
}

namespace CurrentMetrics
{
    extern const Metric GPUColumnCacheBytes;
    extern const Metric GPUColumnCacheColumns;
}

namespace DB
{

size_t GPUColumnCacheKeyHash::operator()(const GPUColumnCacheKey & key) const
{
    SipHash hash;
    hash.update(key.table_uuid);
    hash.update(key.part_name);
    hash.update(key.column_name);
    return hash.get64();
}

GPUResidentColumn::GPUResidentColumn(ConstStoragePtr storage_, DataPartPtr data_part_, size_t num_rows_, size_t element_size)
    : storage(std::move(storage_))
    , data_part(std::move(data_part_))
    , num_rows(num_rows_)
    /// The part's whole column at once, rather than a buffer that grows as blocks arrive. The row
    /// count is known before the first block is read - it is the part's - so there is nothing to
    /// discover by growing, and growing would mean copying on the device every time the buffer
    /// doubled.
    , buffer(num_rows_ * element_size)
{
}

GPUColumnCache::GPUColumnCache(size_t max_size_in_bytes)
    : Base("LRU", CurrentMetrics::GPUColumnCacheBytes, CurrentMetrics::GPUColumnCacheColumns, max_size_in_bytes, NO_MAX_COUNT, /*size_ratio=*/0.0)
{
}

GPUColumnCache::MappedPtr GPUColumnCache::getForPart(const Key & key, const DataPartPtr & data_part)
{
    MappedPtr column = Base::get(key);

    /// Not the part this query is about to read, so not an answer to it. Left in place rather than
    /// removed here: the caller replaces it with the new part's column under the same key, and a
    /// removal in between would only give up the device memory that replacement is about to reuse.
    if (column && column->data_part.get() != data_part.get())
        column = nullptr;

    if (column)
        ProfileEvents::increment(ProfileEvents::GPUColumnCacheHits);
    else
        ProfileEvents::increment(ProfileEvents::GPUColumnCacheMisses);

    return column;
}

void GPUColumnCache::setForPart(const Key & key, const MappedPtr & column)
{
    Base::set(key, column);
}

void GPUColumnCache::onEntryRemoval(const size_t weight_loss, const MappedPtr &)
{
    /// The device memory is not released here: the entry is, and its `DeviceBuffer` frees the
    /// memory when the last reference to the entry goes - which can be later, because a query that
    /// is summing this column right now holds one.
    ProfileEvents::increment(ProfileEvents::GPUColumnCacheEvictedBytes, weight_loss);
    ProfileEvents::increment(ProfileEvents::GPUColumnCacheEvictions);
}

}

#endif
