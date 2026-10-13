#pragma once

#include <Columns/IColumn_fwd.h>
#include <QueryPipeline/SizeLimits.h>

#include <memory>

namespace DB
{

class DiskSet;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;
class TemporaryDataOnDiskScope;
using TemporaryDataOnDiskScopePtr = std::shared_ptr<TemporaryDataOnDiskScope>;

/// Builds a `DiskSet` from columns of keys that arrive in any order and with repeats, and keeps
/// each distinct key once. The finished set owns its temporary file independently of the builder.
///
/// The size limits of the set apply to its distinct keys and to the memory of the finished set.
/// Exceeding them throws in the `throw` overflow mode. In the `break` mode, the set keeps the smallest
/// keys, compared as unsigned integers, whatever order they arrive in: the merge writes the keys in
/// ascending order and stops after the batch of at most `max_block_size` keys that reaches the limits.
class DiskSetBuilder
{
public:
    virtual ~DiskSetBuilder() = default;

    /// Adds a column of keys of the builder's key type.
    virtual void add(ColumnPtr keys) = 0;

    /// Writes each distinct key once, up to the size limits. Call once, after the last `add`.
    virtual std::unique_ptr<DiskSet> finish() = 0;

    /// After `finish`, returns whether the set stopped at its size limits in the `break` overflow mode.
    virtual bool isTruncated() const = 0;

    /// Returns the smallest run that a builder writes, which is the smaller of 16 MiB and
    /// `max_bytes_before_external_set`. The builder writes no keys to disk before it buffers that many
    /// bytes of them.
    static size_t getMinBytesInRun(size_t max_bytes_before_external_set);

    /// Returns the memory that a builder needs to write its first run of keys of `key_bytes` bytes, with
    /// columns of at most `max_block_size` keys and temporary files with buffers of `buffer_size` bytes.
    static size_t estimateMemoryToWriteRun(
        size_t max_bytes_before_external_set, size_t max_block_size, size_t key_bytes, size_t buffer_size);
};

/// Creates a builder of a `DiskSet` of `Key` keys, where `Key` is an unsigned integer of 8 to 256 bits.
/// `tmp_data` holds the runs of the sorter and the finished set, and `limits` are the size limits of the set.
/// `max_bytes_before_external_set` is the spill threshold of the set: the builder writes a run of buffered
/// keys only while tracked query memory exceeds it.
template <typename Key>
std::unique_ptr<DiskSetBuilder> createDiskSetBuilder(
    TemporaryDataOnDiskScopePtr tmp_data,
    const SizeLimits & limits,
    size_t max_bytes_before_external_set,
    size_t max_block_size,
    size_t min_free_disk_space,
    QueryStatusPtr process_list_element);

}
