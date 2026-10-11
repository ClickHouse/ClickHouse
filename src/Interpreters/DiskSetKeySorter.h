#pragma once

#include <Common/PODArray.h>

#include <functional>
#include <memory>
#include <span>
#include <vector>

namespace DB
{

class TemporaryDataBuffer;
class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;

/// Sorts the keys of a set on disk and drops their repeats, in memory while tracked query memory stays within
/// the spill threshold of the set, and in runs written to temporary files once it does not. `Key` is an
/// unsigned integer of 8 to 256 bits.
///
/// Keys arrive in any order and collect in a buffer. Once the buffer holds `bytes_in_run` bytes, it is sorted
/// as a whole and its repeats are dropped: by radix sort for keys of up to 64 bits and by comparisons for
/// wider keys. If at least half of `bytes_in_run` bytes remain, they become a run, and otherwise they stay
/// and are sorted again with the keys that follow, so that repeated keys do not make small runs. The runs
/// stay in memory until query memory exceeds `max_bytes_before_external_set`, and then the runs in memory
/// merge into one temporary file. `merge` merges all runs, in memory and in files. A merge reads runs that do
/// not overlap, or share only the key at their boundary, one after another as one run, so the runs of keys
/// that arrive in order need no comparisons between them.
///
/// An external sort of blocks sorts each block as it arrives and merges sorted blocks into each run, and
/// again runs into the result, comparing rows of columns. Sorting a run as a whole and merging plain keys
/// cost several times less.
template <typename Key>
class DiskSetKeySorter
{
public:
    DiskSetKeySorter(
        TemporaryDataOnDiskScopePtr tmp_data_, size_t bytes_in_run, size_t max_bytes_before_external_set_, size_t min_free_disk_space_);
    ~DiskSetKeySorter();

    /// Adds keys in any order and with repeats.
    void add(std::span<const Key> keys);

    /// Passes each distinct key once, in ascending order, to `consume`, in batches of `batch_size` keys but
    /// the last one, and stops when `consume` returns false. A batch is valid only until `consume` returns.
    /// Call once, after the last `add`.
    void merge(size_t batch_size, const std::function<bool(std::span<const Key>)> & consume);

private:
    /// Merges the runs in memory into one temporary file.
    void writeMemoryRuns();

    TemporaryDataOnDiskScopePtr tmp_data;
    const size_t keys_in_run;
    const size_t max_bytes_before_external_set;
    const size_t min_free_disk_space;

    /// The keys that are not in a run yet.
    PODArray<Key> pending;

    /// A run that left memory for a temporary file, with its first and last keys, which tell a merge whether
    /// the run overlaps other runs.
    struct FileRun
    {
        std::unique_ptr<TemporaryDataBuffer> file;
        Key first;
        Key last;
    };

    /// Runs of sorted distinct keys.
    std::vector<PODArray<Key>> memory_runs;
    std::vector<FileRun> file_runs;
};

/// Returns the memory that a `DiskSetKeySorter` of keys of `key_bytes` bytes needs to sort and write a run of
/// `bytes_in_run` bytes that a column of `column_keys` keys completes: the buffer of keys, which holds the
/// run and the column, the copy of the buffer that radix sort takes for keys of up to 64 bits, and three
/// buffers of temporary files of `buffer_size` bytes.
size_t estimateMemoryToSortRun(size_t bytes_in_run, size_t column_keys, size_t key_bytes, size_t buffer_size);

}
