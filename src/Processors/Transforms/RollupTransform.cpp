#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Columns/ColumnNullable.h>
#include <Common/CurrentMetrics.h>
#include <Common/HashTable/Hash.h>
#include <Common/MapToRange.h>
#include <Common/ThreadPool.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>

namespace CurrentMetrics
{
    extern const Metric AggregatorThreads;
    extern const Metric AggregatorThreadsActive;
    extern const Metric AggregatorThreadsScheduled;
}

namespace DB
{

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
{}

namespace
{

/// Below this, merging a level on one thread is cheaper than starting threads. The same threshold as
/// `Aggregator::mergeBlocks` uses for its parallel merge.
constexpr size_t min_rows_to_merge_level_in_parallel = 100000;

/// The parallel merge is only measured to pay off on aarch64 (Graviton3 and Graviton4). On a 96-core x86_64 Xeon,
/// splitting a level into `max_threads` parts made levels of a few hundred thousand rows up to 2.5 times slower.
#if defined(__aarch64__)
constexpr bool merge_levels_in_parallel = true;
#else
constexpr bool merge_levels_in_parallel = false;
#endif

}

Chunks RollupTransform::mergeLevel(Chunks && chunks, size_t num_keys, bool is_input)
{
    size_t rows = 0;
    for (const auto & chunk : chunks)
        rows += chunk.getNumRows();

    const size_t num_parts = params->params.max_threads;
    /// The last level has no keys: all its rows merge into one.
    if (!merge_levels_in_parallel || num_keys == 0 || num_parts <= 1 || rows < min_rows_to_merge_level_in_parallel)
    {
        Chunks result;
        result.emplace_back(merge(std::move(chunks), is_input, false));
        return result;
    }

    /// What the jobs share. Declared before the runner and held by the jobs, so it outlives every job.
    struct State
    {
        Chunks input;
        /// `parts[part][chunk]`: the rows of input chunk `chunk` whose keys hash to `part`.
        VectorWithMemoryTracking<Chunks> parts;
        Chunks merged;
    };
    auto state = std::make_shared<State>();
    state->input = std::move(chunks);
    state->parts.resize(num_parts);
    for (auto & part : state->parts)
        part.resize(state->input.size());
    state->merged.resize(num_parts);

    ThreadPool pool(
        CurrentMetrics::AggregatorThreads, CurrentMetrics::AggregatorThreadsActive, CurrentMetrics::AggregatorThreadsScheduled, num_parts);
    ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::AGGREGATOR_POOL);

    /// Split each input chunk by the hash of the level's keys, so that equal keys land in the same part.
    for (size_t chunk_idx = 0; chunk_idx < state->input.size(); ++chunk_idx)
    {
        runner.enqueueAndKeepTrack(
            [this, state, chunk_idx, num_keys, num_parts]
            {
                const Chunk & chunk = state->input[chunk_idx];
                const size_t num_rows = chunk.getNumRows();
                if (num_rows == 0)
                    return;

                const auto & columns = chunk.getColumns();
                PaddedPODArray<UInt32> hash(num_rows, WEAK_HASH32_INITIAL_VALUE);
                for (size_t i = 0; i < num_keys; ++i)
                    columns[keys[i]]->computeHashInto(0, num_rows, hash.data(), false);

                IColumn::Selector selector(num_rows);
                mapToRange(hash.data(), num_rows, static_cast<UInt32>(num_parts), selector.data());

                VectorWithMemoryTracking<MutableColumns> part_columns(num_parts);
                for (const auto & column : columns)
                {
                    auto scattered = column->scatter(num_parts, selector);
                    for (size_t part = 0; part < num_parts; ++part)
                        part_columns[part].emplace_back(std::move(scattered[part]));
                }

                for (size_t part = 0; part < num_parts; ++part)
                {
                    const size_t part_rows = part_columns[part].front()->size();
                    state->parts[part][chunk_idx] = Chunk(std::move(part_columns[part]), part_rows);
                }
            });
    }
    runner.waitForAllToFinishAndRethrowFirstError();
    state->input.clear();

    /// Each key is in one part only, so merging the parts independently merges every group exactly once.
    for (size_t part = 0; part < num_parts; ++part)
    {
        runner.enqueueAndKeepTrack(
            [this, state, part, is_input]
            {
                Chunks to_merge;
                for (auto & chunk : state->parts[part])
                    if (chunk.getNumRows())
                        to_merge.emplace_back(std::move(chunk));
                state->parts[part].clear();
                if (!to_merge.empty())
                    state->merged[part] = merge(std::move(to_merge), is_input, false);
            });
    }
    runner.waitForAllToFinishAndRethrowFirstError();

    Chunks result;
    for (auto & chunk : state->merged)
        if (chunk.getNumRows())
            result.emplace_back(std::move(chunk));
    return result;
}

Chunk RollupTransform::generate()
{
    if (!consumed_chunks.empty())
    {
        /// A single chunk is already merged.
        if (consumed_chunks.size() > 1)
            current_level = mergeLevel(std::move(consumed_chunks), keys.size(), /*is_input=*/ true);
        else
            current_level.emplace_back(std::move(consumed_chunks.front()));
        consumed_chunks.clear();

        if (use_nulls)
        {
            for (auto & chunk : current_level)
            {
                const size_t rows = chunk.getNumRows();
                auto columns = chunk.detachColumns();
                for (auto key : keys)
                    columns[key] = makeNullableOrLowCardinalityNullableSafe(columns[key]);
                chunk.setColumns(std::move(columns), rows);
            }
        }

        has_current_level = true;
        last_removed_key = keys.size();
    }

    while (ready_chunks.empty() && has_current_level)
    {
        Chunks level = std::move(current_level);
        current_level.clear();
        has_current_level = false;

        /// The next level is merged from this one's states, before they are finalized.
        if (last_removed_key)
        {
            --last_removed_key;
            const auto key = keys[last_removed_key];

            Chunks next_input;
            next_input.reserve(level.size());
            for (const auto & chunk : level)
            {
                const size_t rows = chunk.getNumRows();
                auto columns = chunk.getColumns();
                columns[key] = getColumnWithDefaults(key, rows);
                next_input.emplace_back(std::move(columns), rows);
            }

            current_level = mergeLevel(std::move(next_input), last_removed_key, !use_nulls);
            has_current_level = true;
        }

        for (auto & chunk : level)
        {
            finalizeChunk(chunk, aggregates_mask);
            chunk.addColumn(0, ColumnUInt64::create(chunk.getNumRows(), set_counter));
            ready_chunks.push_back(std::move(chunk));
        }
        ++set_counter;
    }

    if (ready_chunks.empty())
        return {};

    Chunk chunk = std::move(ready_chunks.front());
    ready_chunks.pop_front();
    return chunk;
}

}
