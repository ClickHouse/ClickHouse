#include <Interpreters/DiskSetKeySorter.h>

#include <Interpreters/DiskSetRunMerge.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/RadixSort.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <base/sort.h>

#include <algorithm>

namespace ProfileEvents
{
extern const Event ExternalSetMerge;
extern const Event ExternalSetTailSpilledKeys;
extern const Event ExternalSetTailKeptKeys;
}

namespace DB
{

namespace
{

/// Sorts `keys` and drops their repeats.
template <typename Key>
void sortAndDeduplicate(PODArray<Key> & keys)
{
    if constexpr (sizeof(Key) <= sizeof(UInt64)) /// One pass per byte, or none for keys that come sorted
        RadixSort<RadixSortUIntTraits<Key>>::executeLSDWithTrySort(keys.data(), keys.size());
    else
        ::sort(keys.begin(), keys.end()); /// Radix sort would take 16 or 32 passes
    keys.resize(std::unique(keys.begin(), keys.end()) - keys.begin()); /// Sorting makes repeats adjacent
}

/// Returns the number of keys of `runs`.
template <typename Key>
size_t countKeys(std::span<const PODArray<Key>> runs)
{
    size_t keys = 0;
    for (const auto & run : runs)
        keys += run.size();
    return keys;
}

}

size_t estimateMemoryToSortRun(size_t bytes_in_run, size_t column_keys, size_t key_bytes, size_t buffer_size)
{
    const size_t buffer_bytes = bytes_in_run + column_keys * key_bytes; /// The buffer holds a run and one column
    const size_t sort_bytes = key_bytes <= sizeof(UInt64) ? buffer_bytes : 0; /// Radix sort copies the buffer
    return buffer_bytes + sort_bytes + 3 * buffer_size; /// Writing a temporary file takes three buffers
}

template <typename Key>
DiskSetKeySorter<Key>::DiskSetKeySorter(
    TemporaryDataOnDiskScopePtr tmp_data_,
    size_t bytes_in_run,
    size_t max_bytes_before_external_set_,
    size_t min_free_disk_space_,
    std::function<void()> check_time_limit_)
    : tmp_data(std::move(tmp_data_))
    , keys_in_run(std::max<size_t>(1, bytes_in_run / sizeof(Key))) /// A threshold below one key would give 0
    , max_bytes_before_external_set(max_bytes_before_external_set_)
    , min_free_disk_space(min_free_disk_space_)
    , check_time_limit(std::move(check_time_limit_))
{
}

template <typename Key>
DiskSetKeySorter<Key>::~DiskSetKeySorter() = default;

template <typename Key>
void DiskSetKeySorter<Key>::add(std::span<const Key> keys)
{
    /// Between calls, the buffer holds fewer keys than a run, so it takes a run and a column at once
    /// instead of growing to the next power of two.
    if (pending.size() + keys.size() > pending.capacity())
        pending.reserve_exact(keys_in_run + keys.size());
    pending.insert(keys.begin(), keys.end());

    if (pending.size() < keys_in_run)
        return; /// Waits until the buffer holds a run

    sortAndDeduplicate(pending);
    if (pending.size() * 2 < keys_in_run)
        return; /// Fewer than half a run stay, so the next sort follows at least half a run of new keys

    memory_runs.emplace_back().swap(pending); /// The buffer becomes a run and starts empty
    if (getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_external_set))
        writeMemoryRuns(memory_runs.size()); /// Moves the runs to disk to free their memory
}

template <typename Key>
void DiskSetKeySorter<Key>::writeMemoryRuns(size_t count)
{
    const std::span<const PODArray<Key>> written(memory_runs.data(), count);
    const size_t keys = countKeys(written); /// An upper bound for the file: the merge drops repeats across runs

    using Run = DiskSetRunMerge::Run<Key>;
    std::vector<Run> runs;
    runs.reserve(count);
    for (const auto & run : written)
        runs.push_back(Run::inMemory({run.data(), run.size()}));
    const Key first = std::ranges::min(runs, {}, &Run::first).first;
    const Key last = std::ranges::max(runs, {}, &Run::last).last;

    auto file = std::make_unique<TemporaryDataBuffer>(tmp_data, keys * sizeof(Key) + min_free_disk_space);
    auto chains = DiskSetRunMerge::makeChains(std::move(runs));
    DiskSetRunMerge::merge(std::move(chains), DiskSetRunMerge::keys_per_batch<Key>, [&](std::span<const Key> batch)
    {
        file->write(reinterpret_cast<const char *>(batch.data()), batch.size_bytes()); /// The file holds raw keys
        check_time_limit(); /// Throws to stop the merge of a cancelled query
        return true;
    });
    file->finishWriting(); /// Flushes the file and frees its write buffers
    LOG_TRACE(
        getLogger("DiskSetKeySorter"),
        "Wrote {} runs with {} keys of a set of IN to temporary file {}",
        count,
        keys,
        file->describeFilePath());

    file_runs.push_back({std::move(file), first, last});
    memory_runs.erase(memory_runs.begin(), memory_runs.begin() + count); /// Frees the runs that the file holds
}

template <typename Key>
size_t DiskSetKeySorter<Key>::countRunsToWriteBeforeMerge(size_t batch_size) const
{
    const size_t buffer_size = tmp_data->getSettings().buffer_size;
    const size_t query_memory = std::max<Int64>(0, getCurrentQueryMemoryUsage()); /// Can briefly be negative
    if (query_memory + DiskSetRunMerge::estimateMemoryToMerge<Key>(file_runs.size(), batch_size, buffer_size)
        <= max_bytes_before_external_set)
        return 0;

    /// The runs written to a file free their memory, and the merge reads the file with one more reader.
    const size_t memory_after_write
        = query_memory + DiskSetRunMerge::estimateMemoryToMerge<Key>(file_runs.size() + 1, batch_size, buffer_size);
    size_t freed_bytes = 0;
    for (size_t count = 1; count < memory_runs.size(); ++count)
    {
        freed_bytes += memory_runs[count - 1].allocated_bytes();
        if (memory_after_write <= max_bytes_before_external_set + freed_bytes)
            return count;
    }
    return memory_runs.size();
}

template <typename Key>
void DiskSetKeySorter<Key>::merge(size_t batch_size, const std::function<bool(std::span<const Key>)> & consume)
{
    if (!pending.empty())
    {
        sortAndDeduplicate(pending);
        memory_runs.emplace_back().swap(pending); /// The last keys make a run, however few
    }

    /// A sorter without keys has no runs.
    if (file_runs.empty() && memory_runs.empty())
        return;

    /// A write frees the memory of its runs and adds a reader, so the budget measures query memory again, as
    /// external `DISTINCT` does, until the merge fits or no runs stay in memory.
    while (const size_t count = countRunsToWriteBeforeMerge(batch_size))
    {
        ProfileEvents::increment(ProfileEvents::ExternalSetTailSpilledKeys, countKeys<Key>({memory_runs.data(), count}));
        LOG_TRACE(
            getLogger("DiskSetKeySorter"),
            "Writing {} of {} runs of a set of IN in memory to a temporary file before the merge (query memory: {}, spill threshold: {})",
            count,
            memory_runs.size(),
            formatReadableSizeWithBinarySuffix(getCurrentQueryMemoryUsage()),
            formatReadableSizeWithBinarySuffix(max_bytes_before_external_set));
        writeMemoryRuns(count);
    }
    ProfileEvents::increment(ProfileEvents::ExternalSetTailKeptKeys, countKeys<Key>(memory_runs));

    using Run = DiskSetRunMerge::Run<Key>;
    std::vector<Run> runs;
    runs.reserve(file_runs.size() + memory_runs.size());
    for (const auto & file_run : file_runs)
        runs.push_back(Run::inFile(*file_run.file, file_run.first, file_run.last));
    for (const auto & run : memory_runs)
        runs.push_back(Run::inMemory({run.data(), run.size()}));
    auto chains = DiskSetRunMerge::makeChains(std::move(runs));

    if (!file_runs.empty())
    {
        ProfileEvents::increment(ProfileEvents::ExternalSetMerge);
        LOG_TRACE(
            getLogger("DiskSetKeySorter"),
            "Merging {} runs of a set of IN in temporary files and {} in memory, in {} chains of runs that do not overlap",
            file_runs.size(),
            memory_runs.size(),
            chains.size());
    }

    DiskSetRunMerge::merge(std::move(chains), batch_size, consume);
}

template class DiskSetKeySorter<UInt8>;
template class DiskSetKeySorter<UInt16>;
template class DiskSetKeySorter<UInt32>;
template class DiskSetKeySorter<UInt64>;
template class DiskSetKeySorter<UInt128>;
template class DiskSetKeySorter<UInt256>;

}
