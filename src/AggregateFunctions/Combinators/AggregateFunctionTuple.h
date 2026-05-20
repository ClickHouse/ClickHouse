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
  * Since each tuple element may have a different type, we create
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
    size_t num_elements;
    String nested_func_name;

    /// Build one nested aggregate function per tuple element and derive the result type.
    /// Returns both so the constructor can reuse the functions without recreating them.
    static std::pair<VectorWithMemoryTracking<AggregateFunctionPtr>, DataTypePtr> initNested(
        const String & base_name,
        const DataTypes & arguments,
        const Array & params);

public:
    AggregateFunctionTuple(
        const AggregateFunctionPtr & representative_nested_func,
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
    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t length, Arena * arena) const override;
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
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override;
    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override;
    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override;

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override;
    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override;

    bool allocatesMemoryInArena() const override;
    bool isState() const override;

    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override;

private:
    /// Delegating constructor: receives the pre-built nested functions and result type
    /// produced by initNested(), so each nested function is created exactly once.
    AggregateFunctionTuple(
        const String & func_name,
        const DataTypes & arguments,
        const Array & params,
        std::pair<VectorWithMemoryTracking<AggregateFunctionPtr>, DataTypePtr> && nested_and_type);

    template <bool merge>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        auto & tuple_to = assert_cast<ColumnTuple &>(to);
        for (size_t i = 0; i < num_elements; ++i)
        {
            if constexpr (merge)
                nested_functions[i]->insertMergeResultInto(place + state_offsets[i], tuple_to.getColumn(i), arena);
            else
                nested_functions[i]->insertResultInto(place + state_offsets[i], tuple_to.getColumn(i), arena);
        }
    }
};

}
