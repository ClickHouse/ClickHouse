#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnNullable.h>
#include <Common/ProfileEvents.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>

#include <string_view>
#include <unordered_set>

namespace ProfileEvents
{
    extern const Event AggregationRollupSkippedInputMerges;
}

namespace DB
{

namespace
{

/// Aggregates whose state merge is exactly associative and commutative: merging the same multiset
/// of states in a different order gives the same state. No aggregate-function property expresses
/// that, so the set lists registered names, and a name that is absent - including every other
/// combinator spelling - counts as order-sensitive.
const std::unordered_set<std::string_view> exactly_commutative_merges
{
    "count", "countIf",
    "sum", "sumIf",
    "min", "minIf",
    "max", "maxIf",
    "uniqExact", "uniqExactIf",
    "groupBitAnd", "groupBitAndIf",
    "groupBitOr", "groupBitOrIf",
    "groupBitXor", "groupBitXorIf",
};

/// Floating-point addition and comparison are not associative, so the merges above stay exact only
/// for these argument types. The set is enumerated instead of rejecting floats, because a float can
/// also sit inside an Array, Tuple, Map or Variant argument.
bool isExactMergeArgumentType(const DataTypePtr & type)
{
    switch (removeLowCardinalityAndNullable(type)->getTypeId())
    {
        case TypeIndex::Nothing:
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
        case TypeIndex::UInt256:
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
        case TypeIndex::String:
        case TypeIndex::FixedString:
        case TypeIndex::UUID:
        case TypeIndex::IPv4:
        case TypeIndex::IPv6:
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
            return true;
        default:
            return false;
    }
}

bool hasExactlyCommutativeMerge(const IAggregateFunction & function)
{
    const auto & name = function.getName();

    auto properties = AggregateFunctionFactory::instance().tryGetProperties(name, NullsAction::EMPTY);
    if (!properties || properties->is_order_dependent)
        return false;

    if (!exactly_commutative_merges.contains(std::string_view{name}))
        return false;

    for (const auto & argument_type : function.getArgumentTypes())
    {
        if (!isExactMergeArgumentType(argument_type))
            return false;
    }

    return true;
}

bool allMergesAreExactlyCommutative(const AggregateDescriptions & aggregates)
{
    for (const auto & aggregate : aggregates)
    {
        if (!aggregate.function || !hasExactlyCommutativeMerge(*aggregate.function))
            return false;
    }

    return true;
}

/// Each source chunk is released as soon as it is appended, so only one copy of the set is held.
Chunk finalizeAndConcatenate(Chunks && chunks, const ColumnsMask & aggregates_mask)
{
    if (chunks.size() == 1)
    {
        auto chunk = std::move(chunks.front());
        finalizeChunk(chunk, aggregates_mask);
        return chunk;
    }

    size_t total_rows = 0;
    for (const auto & chunk : chunks)
        total_rows += chunk.getNumRows();

    MutableColumns result_columns;
    for (auto & chunk : chunks)
    {
        auto num_rows = chunk.getNumRows();
        finalizeChunk(chunk, aggregates_mask);
        auto columns = chunk.detachColumns();

        if (result_columns.empty())
        {
            result_columns.reserve(columns.size());
            for (auto & column : columns)
            {
                result_columns.emplace_back(IColumn::mutate(std::move(column)));
                result_columns.back()->reserve(total_rows);
            }
        }
        else
        {
            for (size_t i = 0; i < result_columns.size(); ++i)
                result_columns[i]->insertRangeFrom(*columns[i], 0, num_rows);
        }

        columns.clear();
    }

    return Chunk(std::move(result_columns), total_rows);
}

}

GroupByModifierTransform::GroupByModifierTransform(SharedHeader header, AggregatingTransformParamsPtr params_, bool use_nulls_)
    : IAccumulatingTransform(std::move(header), std::make_shared<const Block>(generateOutputHeader(params_->getHeader(), params_->params.keys, use_nulls_)))
    , params(std::move(params_))
    , use_nulls(use_nulls_)
{
    keys.reserve(params->params.keys_size);
    for (const auto & key : params->params.keys)
        keys.emplace_back(input.getHeader().getPositionByName(key));

    intermediate_header = getOutputPort().getHeader();
    intermediate_header.erase(0);

    if (use_nulls)
    {
        auto output_aggregator_params = params->params;
        output_aggregator = std::make_unique<Aggregator>(intermediate_header, output_aggregator_params);
    }
}

void GroupByModifierTransform::consume(Chunk chunk)
{
    consumed_chunks.emplace_back(std::move(chunk));
}

void GroupByModifierTransform::mergeConsumed()
{
    if (consumed_chunks.size() > 1)
        current_chunk = merge(std::move(consumed_chunks), true, false);
    else
        current_chunk = std::move(consumed_chunks.front());

    size_t rows = current_chunk.getNumRows();
    auto columns = current_chunk.getColumns();
    if (use_nulls)
    {
        for (auto key : keys)
            columns[key] = makeNullableOrLowCardinalityNullableSafe(columns[key]);
    }
    current_chunk = Chunk{ columns, rows };

    consumed_chunks.clear();
}

Chunk GroupByModifierTransform::merge(Chunks && chunks, bool is_input, bool final)
{
    auto header = is_input ? getInputPort().getHeader() : intermediate_header;

    Aggregator::AggregatedChunks agg_chunks;
    for (auto & chunk : chunks)
        agg_chunks.emplace_back(std::move(chunk));

    auto & aggregator = is_input ? params->aggregator : *output_aggregator;
    auto result = aggregator.mergeBlocks(agg_chunks, final, is_cancelled, /* dataflow_cache_updater= */ nullptr);
    return std::move(result.chunk);
}

MutableColumnPtr GroupByModifierTransform::getColumnWithDefaults(size_t key, size_t n) const
{
    auto const & col = intermediate_header.getByPosition(key);
    auto result_column = col.column->cloneEmpty();
    col.type->insertManyDefaultsInto(*result_column, n);
    return result_column;
}

RollupTransform::RollupTransform(SharedHeader header, AggregatingTransformParamsPtr params_, bool use_nulls_)
    : GroupByModifierTransform(std::move(header), params_, use_nulls_)
    , aggregates_mask(getAggregatesMask(params->getHeader(), params->params.aggregates))
    , input_merge_is_redundant(allMergesAreExactlyCommutative(params->params.aggregates))
{}

Chunk RollupTransform::generate()
{
    /// The grouping set to emit, as the chunks it is spread over. After the first call it is always
    /// the single chunk the previous call's merge produced.
    Chunks gen_chunks;

    if (!consumed_chunks.empty())
    {
        if (input_merge_is_redundant)
        {
            /// The input is the output of an aggregating or a merge-aggregating step, so every group
            /// key occurs in exactly one input chunk and merging the input can only copy it. Rebuild
            /// each chunk so that the emitted set carries no chunk info, as a merged one would not.

            /// `mergeConsumed()` passes a single chunk through unmerged too, so one chunk is no skip.
            if (consumed_chunks.size() > 1)
                ProfileEvents::increment(ProfileEvents::AggregationRollupSkippedInputMerges);

            for (auto & chunk : consumed_chunks)
            {
                auto num_rows = chunk.getNumRows();
                auto columns = chunk.detachColumns();
                if (use_nulls)
                {
                    for (auto key : keys)
                        columns[key] = makeNullableOrLowCardinalityNullableSafe(columns[key]);
                }
                gen_chunks.emplace_back(std::move(columns), num_rows);
            }
            consumed_chunks.clear();
        }
        else
        {
            mergeConsumed();
        }

        last_removed_key = keys.size();
    }

    if (gen_chunks.empty())
        gen_chunks.emplace_back(std::move(current_chunk));

    if (last_removed_key)
    {
        --last_removed_key;
        auto key = keys[last_removed_key];

        Chunks chunks;
        chunks.reserve(gen_chunks.size());
        for (const auto & gen_chunk : gen_chunks)
        {
            auto num_rows = gen_chunk.getNumRows();
            auto columns = gen_chunk.getColumns();
            columns[key] = getColumnWithDefaults(key, num_rows);
            chunks.emplace_back(std::move(columns), num_rows);
        }

        current_chunk = merge(std::move(chunks), !use_nulls, false);
    }

    /// Only after the merge above: finalizing mutates the aggregate state columns it reads.
    auto gen_chunk = finalizeAndConcatenate(std::move(gen_chunks), aggregates_mask);
    if (!gen_chunk.empty())
        gen_chunk.addColumn(0, ColumnUInt64::create(gen_chunk.getNumRows(), set_counter++));
    return gen_chunk;
}

}
