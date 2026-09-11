#include <Processors/Transforms/GPUAggregatingTransform.h>

#if USE_GPU

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>

#include <utility>

namespace DB
{

GPUAggregatingTransform::GPUAggregatingTransform(
    const SharedHeader & input_header_,
    const SharedHeader & output_header_,
    const Aggregator::Params & params,
    size_t batch_bytes)
    : IAccumulatingTransform(input_header_, output_header_)
    , empty_result_for_empty_set(params.empty_result_for_aggregation_by_empty_set)
{
    argument_positions.reserve(params.aggregates.size());

    DataTypes argument_types;
    DataTypes result_types;
    argument_types.reserve(params.aggregates.size());
    result_types.reserve(params.aggregates.size());

    for (const auto & aggregate : params.aggregates)
    {
        /// Exactly one argument, of a type the device sums - `GPUAggregatingStep::canRunOnDevice`
        /// is what let this transform be built at all.
        const String & argument_name = aggregate.argument_names.front();

        argument_positions.push_back(input_header_->getPositionByName(argument_name));
        argument_types.push_back(input_header_->getByName(argument_name).type);
        result_types.push_back(aggregate.function->getResultType());
    }

    if (params.keys.empty())
    {
        accumulators.reserve(params.aggregates.size());
        for (size_t i = 0; i < argument_types.size(); ++i)
            accumulators.emplace_back(*argument_types[i], *result_types[i], batch_bytes);

        return;
    }

    DataTypes key_types;
    key_positions.reserve(params.keys.size());
    key_types.reserve(params.keys.size());

    for (const auto & key : params.keys)
    {
        key_positions.push_back(input_header_->getPositionByName(key));
        key_types.push_back(input_header_->getByName(key).type);
    }

    /// Constructed in place because it owns a handle to a partial result on the device and so has
    /// no copy - see `GPU::GroupBySumAccumulator`.
    group_by_accumulator.emplace(key_types, argument_types, result_types, batch_bytes);
}

void GPUAggregatingTransform::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    total_rows += num_rows;

    const Columns & columns = chunk.getColumns();

    /// A column is a `ColumnVector` of its type already, unless the pipeline handed over a
    /// constant, sparse or replicated one - a representation around such a vector - and the device
    /// needs the values laid out one after another in every case. `LowCardinality` is not among the
    /// wrappers to strip because a `LowCardinality` key or argument is not eligible for this path,
    /// so one cannot arrive here.
    if (!group_by_accumulator)
    {
        for (size_t i = 0; i < accumulators.size(); ++i)
        {
            const ColumnPtr column = columns[argument_positions[i]]->convertToFullIfWrapped();
            accumulators[i].add(*column);
        }

        return;
    }

    /// The full columns are held for the whole call rather than converted one at a time, because
    /// `add` stages the bytes of all of them together and a temporary would be gone by then.
    Columns key_columns;
    key_columns.reserve(key_positions.size());
    for (const size_t position : key_positions)
        key_columns.push_back(columns[position]->convertToFullIfWrapped());

    Columns value_columns;
    value_columns.reserve(argument_positions.size());
    for (const size_t position : argument_positions)
        value_columns.push_back(columns[position]->convertToFullIfWrapped());

    group_by_accumulator->add(key_columns, value_columns);
}

Chunk GPUAggregatingTransform::generate()
{
    /// An empty chunk is how `IAccumulatingTransform` learns that there is nothing more to come,
    /// so this can only produce its row once.
    if (generated)
        return {};

    generated = true;

    const Block & header = getOutputPort().getHeader();

    if (group_by_accumulator)
        return generateGroups(header);

    /// An aggregation without `GROUP BY` over an empty table still returns one row - holding the
    /// sum of nothing, zero - unless `empty_result_for_aggregation_by_empty_set` says otherwise.
    if (total_rows == 0 && empty_result_for_empty_set)
        return {};

    Columns columns;
    columns.reserve(accumulators.size());

    for (size_t i = 0; i < accumulators.size(); ++i)
    {
        auto column = header.getByPosition(i).type->createColumn();
        column->insert(accumulators[i].finalize());
        columns.push_back(std::move(column));
    }

    return Chunk(std::move(columns), 1);
}

Chunk GPUAggregatingTransform::generateGroups(const Block & header)
{
    const size_t num_groups = group_by_accumulator->finalize();

    /// A keyed aggregation over no rows returns no rows, and `empty_result_for_aggregation_by_empty_set`
    /// does not come into it: that setting is about the one row a keyless aggregation returns for
    /// the empty set - the sum of nothing - and there is no such row here, since every row of this
    /// result stands for a group that was actually read. The `Aggregator` draws the same line, in
    /// `AggregatingTransform::initGenerate`: it aggregates an empty block to get that row only when
    /// `keys_size == 0`.
    if (num_groups == 0)
        return {};

    /// `Aggregator::Params::getHeader` builds the output header as the key columns, in `params.keys`
    /// order, followed by one column per aggregate - so the columns are filled in that order here,
    /// and each one is created from the header rather than from a type of this transform's own.
    MutableColumns key_columns;
    key_columns.reserve(key_positions.size());
    for (size_t i = 0; i < key_positions.size(); ++i)
        key_columns.push_back(header.getByPosition(i).type->createColumn());

    MutableColumns value_columns;
    value_columns.reserve(argument_positions.size());
    for (size_t i = 0; i < argument_positions.size(); ++i)
        value_columns.push_back(header.getByPosition(key_positions.size() + i).type->createColumn());

    group_by_accumulator->copyGroupsTo(key_columns, value_columns);

    Columns columns;
    columns.reserve(key_columns.size() + value_columns.size());
    for (auto & column : key_columns)
        columns.push_back(std::move(column));
    for (auto & column : value_columns)
        columns.push_back(std::move(column));

    /// Every group in one chunk, however many there are: the partial result is one table on the
    /// device and copying it out in pieces would mean either holding it past `finalize` or slicing
    /// it there, and nothing downstream of an aggregation needs a particular chunk size.
    return Chunk(std::move(columns), num_groups);
}

}

#endif
