#pragma once
#include <Core/SortDescription.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Common/PODArray.h>

#include <boost/smart_ptr/atomic_shared_ptr.hpp>

namespace DB
{

class PartialSortingTransform;

/// The `GlobalThresholdColumns` struct provides a thread-safe structure for managing
/// and updating the TopN threshold state across multiple threads. It uses
/// `boost::atomic_shared_ptr` to allow concurrent updates to the threshold state
/// (via `updateThreshold`) and thread-safe reads (via `getCurrentThreshold`).
///
/// This design is necessary because:
/// - Multiple `PartialSortingTransform` instances may concurrently update the threshold
///   as they refine the TopN state.
/// - Multiple `MergeTreeSource` instances rely on this shared threshold state for
///   filtering during the read phase.
///
/// By leveraging `boost::atomic_shared_ptr`, the implementation avoids locking,
/// ensuring high performance and safe access across threads. Each update to the
/// threshold creates a new `Node`, encapsulating the updated columns.
///
/// However, it is important to note that this implementation is not truly lock-free:
/// - The `atomic_shared_ptr` in `boost` and `libstdc++` uses mutexes for synchronization.
/// - `libc++` does not provide an implementation for `atomic_shared_ptr`.
/// - Alternatives like `Folly` and https://github.com/anthonywilliams/atomic_shared_ptr
///   implement `atomic_shared_ptr` using a packed pointer with a split reference count,
///   which is not portable and requires gcc `libatomic`.
///
/// It's still is acceptable because the contention on the shared threshold
/// state is expected to be low, occurring only across tens or hundreds of
/// threads, and at the chunk-based level. This means the performance overhead
/// from locking is minimal and does not significantly impact throughput.
struct GlobalThresholdColumns
{
    struct Node
    {
        Columns columns;
        const PartialSortingTransform * sorting_transform = nullptr;
        Node() = default;
        Node(Columns columns_, const PartialSortingTransform * sorting_transform_)
            : columns(std::move(columns_)), sorting_transform(sorting_transform_)
        {
        }
    };

    boost::atomic_shared_ptr<Node> current;

    GlobalThresholdColumns();

    /// Updates the current threshold state based on new column values and a specified row.
    void updateThreshold(const ColumnRawPtrs & new_columns, size_t row_to_compare, const PartialSortingTransform * sorting_transform);

    /// Provides a snapshot of the current threshold state for readers such as `MergeTreeSource`.
    boost::shared_ptr<Node> getCurrentThreshold() const { return current.load(); }
};

using GlobalThresholdColumnsPtr = std::shared_ptr<GlobalThresholdColumns>;

/** Sorts each block individually by the values of the specified columns.
  */
class PartialSortingTransform : public ISimpleTransform
{
public:
    /// limit - if not 0, then you can sort each block not completely, but only `limit` first rows by order.
    PartialSortingTransform(
        SharedHeader header_,
        const SortDescription & description_,
        UInt64 limit_ = 0);

    String getName() const override { return "PartialSortingTransform"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { read_rows.swap(counter); }

    size_t getFilterMask(
        const ColumnRawPtrs & raw_block_columns,
        const Columns & threshold_columns,
        size_t num_rows,
        IColumn::Filter & filter,
        PaddedPODArray<UInt64> * rows_to_compare,
        PaddedPODArray<Int8> & compare_results,
        bool include_equal_row) const;

    void setGlobalThresholdColumnsPtr(GlobalThresholdColumnsPtr global_threshold_columns_ptr_)
    {
        global_threshold_columns_ptr = std::move(global_threshold_columns_ptr_);
    }

protected:
    void transform(Chunk & chunk) override;

private:
    const SortDescription description;
    SortDescriptionWithPositions description_with_positions;
    const UInt64 limit;
    RowsBeforeStepCounterPtr read_rows;

    Columns sort_description_threshold_columns;
    GlobalThresholdColumnsPtr global_threshold_columns_ptr;

    /// This are just buffers which reserve memory to reduce the number of allocations.
    PaddedPODArray<UInt64> rows_to_compare;
    PaddedPODArray<Int8> compare_results;
    IColumn::Filter filter;

    /// If limit < min_limit_for_partial_sort_optimization, skip optimization with threshold_block.
    /// Because for small LIMIT partial sorting may be very faster then additional work
    /// which is made if optimization is enabled (comparison with threshold, filtering).
    static constexpr size_t min_limit_for_partial_sort_optimization = 1500;

    friend struct GlobalThresholdColumns;
};

}
