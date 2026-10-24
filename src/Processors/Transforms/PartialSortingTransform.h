#pragma once
#include <Core/SortDescription.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Processors/TopKThresholdTracker.h>
#include <Common/PODArray.h>

#include <mutex>

namespace DB
{

/** One per sort, shared by the PartialSortingTransforms of all its streams. Publishes to the tracker the
  * `limit`-th best first sort column value among the rows of all streams. Every row passes exactly one
  * partial sort, so the published value is backed by `limit` distinct rows.
  */
class TopKThresholdCollector
{
public:
    TopKThresholdCollector(TopKThresholdTrackerPtr tracker_, const SortColumnDescription & description_, size_t limit_);

    static bool isSupportedType(const IDataType & type);

    /// Above it the values copied per block make the streams contend for `mutex`.
    static constexpr size_t max_limit = 1000;

    /// `sorted` is the first sort column of a sorted block; its first `rows` values are added.
    void add(const IColumn & sorted, size_t rows);

private:
    int compare(const IColumn & lhs, size_t lhs_row, const IColumn & rhs, size_t rhs_row) const;

    const TopKThresholdTrackerPtr tracker;
    const SortColumnDescription description;
    const size_t limit;

    std::mutex mutex;
    MutableColumnPtr values TSA_GUARDED_BY(mutex);
    /// One row: the value published by the last merge, null before it.
    ColumnPtr boundary TSA_GUARDED_BY(mutex);
    /// A thread is sorting the values it took out of `values`; it puts the `limit` best back.
    bool merging TSA_GUARDED_BY(mutex) = false;
};

using TopKThresholdCollectorPtr = std::shared_ptr<TopKThresholdCollector>;

/** Sorts each block individually by the values of the specified columns.
  */
class PartialSortingTransform final : public ISimpleTransform
{
public:
    /// limit - if not 0, then you can sort each block not completely, but only `limit` first rows by order.
    PartialSortingTransform(
        SharedHeader header_,
        const SortDescription & description_,
        UInt64 limit_ = 0,
        TopKThresholdTrackerPtr threshold_tracker_ = nullptr,
        TopKThresholdCollectorPtr threshold_collector_ = nullptr);

    String getName() const override { return "PartialSortingTransform"; }

    void setRowsBeforeLimitCounter(RowsBeforeStepCounterPtr counter) override { read_rows.swap(counter); }

protected:
    void transform(Chunk & chunk) override;

private:
    const SortDescription description;
    SortDescriptionWithPositions description_with_positions;
    const UInt64 limit;
    RowsBeforeStepCounterPtr read_rows;

    Columns sort_description_threshold_columns;

    /// This are just buffers which reserve memory to reduce the number of allocations.
    PaddedPODArray<UInt64> rows_to_compare;
    PaddedPODArray<Int8> compare_results;
    IColumn::Filter filter;

    TopKThresholdTrackerPtr threshold_tracker;
    TopKThresholdCollectorPtr threshold_collector;

    /// If limit < min_limit_for_partial_sort_optimization, skip optimization with threshold_block.
    /// Because for small LIMIT partial sorting may be very faster then additional work
    /// which is made if optimization is enabled (comparison with threshold, filtering).
    static constexpr size_t min_limit_for_partial_sort_optimization = 1500;
};

}
