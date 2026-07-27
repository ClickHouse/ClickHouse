#pragma once

#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/VectorWithMemoryTracking.h>
#include <DataTypes/DataTypeTuple.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
struct Settings;

/** Adaptor for aggregate functions.
  * Adding -Tuple suffix to aggregate function
  *  will convert that aggregate function to a function, accepting Tuples,
  *  and applies aggregation for each element of the Tuple independently,
  *  returning a Tuple of aggregated values.
  *
  * Example: avgTuple of:
  *  (1, 2.0, 3.0),
  *  (3, 4.0, 5.0),
  *  (6, 7.0, 8.0)
  * will return:
  *  (avg(1,3,6), avg(2.0,4.0,7.0), avg(3.0,5.0,8.0))
  *
  * An aggregate function with several arguments takes one Tuple per argument, all of the same
  * size, and element i aggregates the i-th element of every tuple:
  *  corrTuple((x1, x2), (y1, y2)) returns (corr(x1, y1), corr(x2, y2)).
  *
  * Since each tuple element may have a different type, the factory resolves
  * a separate nested aggregate function instance per element.
  */
class AggregateFunctionTuple final : public IAggregateFunctionHelper<AggregateFunctionTuple>
{
private:
    /// One nested aggregate function per tuple element (may be different instantiations).
    VectorWithMemoryTracking<AggregateFunctionPtr> nested_functions;
    /// Precomputed byte offsets of each nested state within the aggregation data block.
    VectorWithMemoryTracking<size_t> state_offsets;
    size_t total_state_size = 0;
    size_t max_state_align = 1;
    String nested_func_name;

    /// Result type: a Tuple of the nested result types, preserving explicit element names.
    static DataTypePtr deriveResultType(
        const VectorWithMemoryTracking<AggregateFunctionPtr> & nested_functions,
        const DataTypes & arguments);

public:
    /// Receives one nested aggregate function per tuple element, resolved by the factory.
    AggregateFunctionTuple(
        const String & nested_name,
        VectorWithMemoryTracking<AggregateFunctionPtr> nested_functions_,
        const DataTypes & arguments,
        const Array & params);

    String getName() const override { return nested_func_name + "Tuple"; }

    bool isVersioned() const override;
    size_t getDefaultVersion() const override;
    size_t getVersionFromRevision(size_t revision) const override;

    size_t sizeOfData() const override { return total_state_size; }
    size_t alignOfData() const override { return max_state_align; }

    void create(AggregateDataPtr __restrict place) const override;
    void destroy(AggregateDataPtr __restrict place) const noexcept override;
    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override;
    bool hasTrivialDestructor() const override;

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override;
    void addBatch( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override;
    void addBatchSinglePlace( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override;
    void addBatchSinglePlaceNotNull( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override;
    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override;

    bool isParallelizeMergePrepareNeeded() const override;
    bool isAbleToParallelizeMerge() const override;
    bool canOptimizeEqualKeysRanges() const override;
    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override;
    void mergeImpl(
        AggregateDataPtr __restrict place,
        ConstAggregateDataPtr rhs,
        ThreadPool & thread_pool,
        std::atomic<bool> & is_cancelled,
        Arena * arena) const override;
    void parallelizeMergeMulti(
        AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override;
    void mergeBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        ThreadPool & thread_pool,
        std::atomic<bool> & is_cancelled,
        Arena * arena) const override;
    void mergeAndDestroyBatch(
        AggregateDataPtr * dst_places,
        AggregateDataPtr * rhs_places,
        size_t size,
        size_t offset,
        ThreadPool & thread_pool,
        std::atomic<bool> & is_cancelled,
        Arena * arena) const override;

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override;
    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override;

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override;
    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override;

    bool allocatesMemoryInArena() const override;
    bool isState() const override;

    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override;
    DataTypePtr getNormalizedStateType() const override;

    AggregateFunctionStateVariant getStateVariant() const override;
    bool canMergeStateFromDifferentVariant(const IAggregateFunction & rhs) const override;
    void mergeStateFromDifferentVariant(
        AggregateDataPtr __restrict place, const IAggregateFunction & rhs, ConstAggregateDataPtr rhs_place, Arena * arena) const override;

private:
    template <bool for_merge>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        auto & tuple_to = assert_cast<ColumnTuple &>(to);
        for (size_t i = 0; i < nested_functions.size(); ++i)
        {
            if constexpr (for_merge)
                nested_functions[i]->insertMergeResultInto(place + state_offsets[i], tuple_to.getColumn(i), arena);
            else
                nested_functions[i]->insertResultInto(place + state_offsets[i], tuple_to.getColumn(i), arena);
        }
    }

    /// Shared implementation of the batch add overrides. Hoists the per-element column pointers, so
    /// no per-row unwrapping work remains in the row loop.
    /// `get_place` returns the aggregation state for a row, or nullptr when the row has none.
    template <bool has_null_map, typename GetPlace>
    void addBatchImpl(
        size_t row_begin,
        size_t row_end,
        const IColumn ** columns,
        const UInt8 * null_map,
        ssize_t if_argument_pos,
        Arena * arena,
        GetPlace && get_place) const;
};

}
