#include <Interpreters/DiskSetBuilder.h>
#include <Interpreters/DiskSetImpl.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Defines.h>
#include <Interpreters/DiskSetKeySorter.h>
#include <Interpreters/ProcessList.h>
#include <Common/FailPoint.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace FailPoints
{
extern const char disk_set_builder_reach_time_limit[];
}

namespace
{

/// With runs of about 16 MiB, the final merge stays narrow even for large sets, as with external `DISTINCT`.
constexpr size_t DEFAULT_BYTES_IN_RUN = DEFAULT_BLOCK_SIZE * 256;

}

size_t DiskSetBuilder::getMinBytesInRun(size_t max_bytes_before_external_set)
{
    /// Smaller runs would make the final merge wide, so only a spill threshold below the default
    /// run size makes runs smaller.
    return max_bytes_before_external_set
        ? std::min(max_bytes_before_external_set, DEFAULT_BYTES_IN_RUN) : DEFAULT_BYTES_IN_RUN;
}

size_t
DiskSetBuilder::estimateMemoryToWriteRun(size_t max_bytes_before_external_set, size_t max_block_size, size_t key_bytes, size_t buffer_size)
{
    return estimateMemoryToSortRun(getMinBytesInRun(max_bytes_before_external_set), max_block_size, key_bytes, buffer_size);
}

/// The builder adds each column of keys to a `DiskSetKeySorter`, which writes runs of sorted distinct keys to
/// temporary files while tracked query memory exceeds `max_bytes_before_external_set`, and merges them into a
/// `DiskSetImpl` in batches of `max_block_size` keys. The builder checks the size limits after each batch. In
/// the `break` mode, it stops the merge at the batch that reaches the limits. It checks the time limit of the
/// query after each column of keys and after each batch of every merge. The builder also counts the
/// distinct keys as they arrive. The sorter passes on no keys until all have arrived, so the builder creates
/// the set before the merge, with filters sized for these keys, or for fewer when the size limits let the set
/// write fewer. The merge counts the memory of the filters towards the spill threshold.
template <typename Key>
class DiskSetBuilderImpl final : public DiskSetBuilder
{
public:
    DiskSetBuilderImpl(
        TemporaryDataOnDiskScopePtr tmp_data_,
        const SizeLimits & limits_,
        size_t max_bytes_before_external_set,
        size_t max_block_size_,
        size_t min_free_disk_space_,
        size_t max_bytes_for_bloom_filter_,
        size_t max_bytes_for_cache_,
        QueryStatusPtr process_list_element_);

    void add(ColumnPtr keys) override;
    std::unique_ptr<DiskSet> finish() override;
    [[nodiscard]] bool isTruncated() const override { return truncated; }

private:
    /// Checks the size limits, which apply to the distinct keys of the set and to their bytes (see
    /// `DiskSetImpl::getTotalByteCount`). In the `break` mode, a set that reaches the limits keeps the keys
    /// that reached them. A set in memory does the same with the block that reaches its limits.
    void checkLimits();

    /// Returns the number of keys that the set is expected to write: the distinct keys that the builder
    /// counted, but at most the keys that the set writes before the size limits stop the merge in the `break`
    /// mode or make it throw in the `throw` mode.
    [[nodiscard]] size_t estimateKeysToWrite() const;

    /// Checks the time limit of the query, which throws in the `throw` overflow mode and when the query is
    /// cancelled. In the `break` mode, the limit stops the query without an exception, as the failpoint does,
    /// but a set without all its keys would give wrong results, so the builder throws instead.
    void checkTimeLimit() const;

    TemporaryDataOnDiskScopePtr tmp_data;
    SizeLimits limits;
    size_t max_block_size;
    size_t min_free_disk_space;
    size_t max_bytes_for_bloom_filter;
    size_t max_bytes_for_cache;
    QueryStatusPtr process_list_element;

    /// Becomes true when the set reaches its size limits in the `break` overflow mode and keeps no more keys.
    bool truncated = false;

    /// Counts the distinct keys for the filters of the set. It is null when the budget of the filters is 0.
    std::unique_ptr<typename DiskSetBlockFilters<Key>::DistinctKeyCounter> distinct_key_counter;

    DiskSetKeySorter<Key> sorter;

    /// The set is created with its file before the merge.
    std::unique_ptr<DiskSetImpl<Key>> set;
};

template <typename Key>
DiskSetBuilderImpl<Key>::DiskSetBuilderImpl(
    TemporaryDataOnDiskScopePtr tmp_data_,
    const SizeLimits & limits_,
    size_t max_bytes_before_external_set,
    size_t max_block_size_,
    size_t min_free_disk_space_,
    size_t max_bytes_for_bloom_filter_,
    size_t max_bytes_for_cache_,
    QueryStatusPtr process_list_element_)
    : tmp_data(std::move(tmp_data_))
    , limits(limits_)
    , max_block_size(max_block_size_)
    , min_free_disk_space(min_free_disk_space_)
    , max_bytes_for_bloom_filter(max_bytes_for_bloom_filter_)
    , max_bytes_for_cache(max_bytes_for_cache_)
    , process_list_element(std::move(process_list_element_))
    , sorter(
          tmp_data,
          getMinBytesInRun(max_bytes_before_external_set),
          max_bytes_before_external_set,
          min_free_disk_space,
          [this] { checkTimeLimit(); })
{
    if (max_bytes_for_bloom_filter)
        distinct_key_counter = std::make_unique<typename DiskSetBlockFilters<Key>::DistinctKeyCounter>();
}

template <typename Key>
void DiskSetBuilderImpl<Key>::add(ColumnPtr keys)
{
    const auto & values = assert_cast<const ColumnVector<Key> &>(*keys).getData();
    if (distinct_key_counter)
        distinct_key_counter->add({values.data(), values.size()});
    sorter.add({values.data(), values.size()});

    /// A set that spills passes all the keys that it holds in memory to `add` in one loop, which no pipeline
    /// interrupts, so the builder checks the time limit after each column itself.
    checkTimeLimit();
}

template <typename Key>
void DiskSetBuilderImpl<Key>::checkLimits()
{
    if (!limits.check(set->getTotalRowCount(), set->getTotalByteCount(), "IN-set", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED))
    {
        LOG_TRACE(getLogger("DiskSetBuilder"), "The set reached its size limits with {} keys", set->getTotalRowCount());
        truncated = true;
    }
}

template <typename Key>
size_t DiskSetBuilderImpl<Key>::estimateKeysToWrite() const
{
    const size_t distinct_keys = distinct_key_counter->getCount();
    size_t max_keys = distinct_keys;
    if (limits.max_rows)
        max_keys = std::min<size_t>(max_keys, limits.max_rows);

    /// The set counts `sizeof(Key)` bytes for each key (see `DiskSetImpl::getTotalByteCount`).
    if (limits.max_bytes)
        max_keys = std::min<size_t>(max_keys, limits.max_bytes / sizeof(Key));

    /// The merge checks the limits after each batch, so the set writes at most one batch past them.
    return std::min(distinct_keys, max_keys + max_block_size);
}

template <typename Key>
void DiskSetBuilderImpl<Key>::checkTimeLimit() const
{
    bool within_time_limit = !process_list_element || process_list_element->checkTimeLimit();
    fiu_do_on(FailPoints::disk_set_builder_reach_time_limit, { within_time_limit = false; });
    if (!within_time_limit)
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled before the set of IN was written to disk");
}

template <typename Key>
std::unique_ptr<DiskSet> DiskSetBuilderImpl<Key>::finish()
{
    checkTimeLimit();
    if (sorter.empty())
        return std::make_unique<DiskSetImpl<Key>>(); /// A set without keys has no file

    set = std::make_unique<DiskSetImpl<Key>>(
        tmp_data,
        min_free_disk_space,
        distinct_key_counter ? estimateKeysToWrite() : 0,
        max_bytes_for_bloom_filter,
        max_bytes_for_cache);
    sorter.merge(max_block_size, [&](std::span<const Key> keys)
    {
        set->add(keys);
        checkLimits();
        if (truncated) /// The set keeps no more keys
            return false;
        checkTimeLimit();
        return true;
    });
    set->finishWriting();
    return std::move(set);
}

template <typename Key>
std::unique_ptr<DiskSetBuilder> createDiskSetBuilder(
    TemporaryDataOnDiskScopePtr tmp_data,
    const SizeLimits & limits,
    size_t max_bytes_before_external_set,
    size_t max_block_size,
    size_t min_free_disk_space,
    size_t max_bytes_for_bloom_filter,
    size_t max_bytes_for_cache,
    QueryStatusPtr process_list_element)
{
    return std::make_unique<DiskSetBuilderImpl<Key>>(
        std::move(tmp_data),
        limits,
        max_bytes_before_external_set,
        max_block_size,
        min_free_disk_space,
        max_bytes_for_bloom_filter,
        max_bytes_for_cache,
        std::move(process_list_element));
}

template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt8>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, size_t, size_t, QueryStatusPtr);
template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt16>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, size_t, size_t, QueryStatusPtr);
template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt32>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, size_t, size_t, QueryStatusPtr);
template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt64>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, size_t, size_t, QueryStatusPtr);
template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt128>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, size_t, size_t, QueryStatusPtr);
template std::unique_ptr<DiskSetBuilder>
createDiskSetBuilder<UInt256>(TemporaryDataOnDiskScopePtr, const SizeLimits &, size_t, size_t, size_t, size_t, size_t, QueryStatusPtr);

}
