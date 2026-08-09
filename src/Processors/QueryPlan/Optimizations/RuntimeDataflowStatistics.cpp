#include <Core/ProtocolDefines.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/NullWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <optional>


namespace ProfileEvents
{
extern const Event RuntimeDataflowStatisticsInputBytes;
extern const Event RuntimeDataflowStatisticsOutputBytes;
}

namespace DB
{

std::optional<RuntimeDataflowStatisticsCache::Entry> RuntimeDataflowStatisticsCache::getStats(size_t key) const
{
    if (const auto entry = stats_cache->get(key))
        return *entry;
    return std::nullopt;
}

void RuntimeDataflowStatisticsCache::update(size_t key, RuntimeDataflowStatistics stats)
{
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsInputBytes, stats.input_bytes);
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsOutputBytes, stats.output_bytes);
    stats_cache->set(key, std::make_shared<RuntimeDataflowStatistics>(stats));
}

RuntimeDataflowStatisticsCacheUpdater::~RuntimeDataflowStatisticsCacheUpdater()
{
    if (unsupported_case)
    {
        LOG_DEBUG(getLogger("RuntimeDataflowStatisticsCacheUpdater"), "Unsupported case encountered, skipping statistics update.");
        return;
    }

    auto log_stats = [](const auto & stats, auto type) TSA_REQUIRES(stats.mutex)
    {
        LOG_TEST(
            getLogger("RuntimeDataflowStatisticsCacheUpdater"),
            "{} bytes={}, sample_bytes={}, compressed_bytes={}, compression_ratio={}, elapsed_microseconds={}",
            type,
            stats.bytes,
            stats.sample_bytes,
            stats.compressed_bytes,
            static_cast<double>(stats.sample_bytes) / static_cast<double>(stats.compressed_bytes),
            stats.elapsed_microseconds);
    };

    RuntimeDataflowStatistics res{.total_rows_to_read = total_rows_to_read};
    for (size_t i = 0; i < InputStatisticsType::MaxInputType; ++i)
    {
        const auto & stats = input_bytes_statistics[i];
        if (stats.compressed_bytes)
        {
            log_stats(stats, toString(static_cast<InputStatisticsType>(i)));
            const auto compression_ratio = static_cast<double>(stats.sample_bytes) / static_cast<double>(stats.compressed_bytes);
            res.input_bytes += static_cast<size_t>(static_cast<double>(stats.bytes) / compression_ratio);
        }
    }
    for (size_t i = 0; i < OutputStatisticsType::MaxOutputType; ++i)
    {
        const auto & stats = output_bytes_statistics[i];
        if (stats.compressed_bytes)
        {
            log_stats(stats, toString(static_cast<OutputStatisticsType>(i)));
            const auto compression_ratio = static_cast<double>(stats.sample_bytes) / static_cast<double>(stats.compressed_bytes);
            res.output_bytes += static_cast<size_t>(static_cast<double>(stats.bytes) / compression_ratio);
        }
    }

    LOG_DEBUG(
        getLogger("RuntimeDataflowStatisticsCacheUpdater"),
        "Collected statistics: input bytes={}, output bytes={}",
        res.input_bytes,
        res.output_bytes);

    if (res.input_bytes == 0 && res.output_bytes == 0)
    {
        LOG_DEBUG(getLogger("RuntimeDataflowStatisticsCacheUpdater"), "No statistics collected, skipping statistics update.");
        return;
    }

    auto & dataflow_cache = getRuntimeDataflowStatisticsCache();
    dataflow_cache.update(cache_key, res);
}

/// Tries to estimate compressed size of a column by serializing a sample of it.
static std::pair<size_t, size_t> estimateCompressedColumnSize(const ColumnWithTypeAndName & column)
{
    NullWriteBuffer null_buf;
    CompressedWriteBuffer compressed_buf(null_buf);
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    // To avoid spending too much time on serialization, we limit the number of rows to serialize.
    const auto limit = std::max<size_t>(std::min(8192ul, column_to_write->size()), column_to_write->size() / 10);
    NativeWriter::writeData(*serialization, column_to_write, compressed_buf, std::nullopt, 0, limit, DBMS_TCP_PROTOCOL_VERSION);
    compressed_buf.finalize();
    // Return pair of (sample size, compressed size), note that both sizes are based on limited number of rows.
    return std::make_pair(compressed_buf.count(), null_buf.count());
}

/// Final `-State` results can reach the output wrapped in carrier columns - `prepareOutputBlockColumns`
/// recurses through the subcolumns of `isState()` results to attach the shared arenas to nested
/// `ColumnAggregateFunction` leaves, so e.g. `SELECT tuple(uniqExactState(x))` emits a `ColumnTuple` around
/// an aggregate-state leaf. Visit every such leaf, whether the column is one itself or wraps some.
static void forEachAggregateStateLeaf(const IColumn & column, const std::function<void(const ColumnAggregateFunction &)> & callback)
{
    if (const auto * aggregate_column = typeid_cast<const ColumnAggregateFunction *>(&column))
    {
        callback(*aggregate_column);
        return;
    }
    column.forEachSubcolumnRecursively(
        [&](const IColumn & subcolumn)
        {
            if (const auto * aggregate_column = typeid_cast<const ColumnAggregateFunction *>(&subcolumn))
                callback(*aggregate_column);
        });
}

bool RuntimeDataflowStatisticsCacheUpdater::shouldSampleBlock(Statistics & statistics, size_t block_rows)
{
    // Empty blocks produced during planning, when we calculate output headers. Skip them.
    if (!block_rows)
        return false;
    const auto counter = statistics.counter.fetch_add(1, std::memory_order_relaxed);
    return counter % 5 == 0 && counter < 25;
}

void RuntimeDataflowStatisticsCacheUpdater::recordColumns(Statistics & statistics, size_t num_rows, const ColumnsWithTypeAndName & cols)
{
    Stopwatch watch;

    const bool sample_block = shouldSampleBlock(statistics, num_rows);

    /// The uncompressed size of a column for the purpose of the estimate. `byteSize` of a
    /// `ColumnAggregateFunction` counts the state pointers plus the arena the column owns, and a column of
    /// states assembled by `AggregatingInOrderTransform` (see `addArenasToAggregateColumns`) does not own
    /// the arena its states live in, so `byteSize` degenerates to one pointer per row there. Size such
    /// columns from their serialized states instead - only the states, which is what
    /// `SerializationAggregateFunction` puts on the wire and what `Aggregator::estimateSizeOfCompressedState`
    /// measures - sampling as many of them as that function does per hash table, so that both producers of
    /// the `AggregationState` statistic measure the same thing. Serializing states is not cheap, and when
    /// `aggregation_in_order_max_block_bytes` splits large states into many small blocks, doing it per block
    /// would serialize every state of every block; so only the sampled blocks serialize, and the rest are
    /// extrapolated from the per-row figure the sampled blocks give, like the compression ratio is.
    /// Aggregate-state leaves of every column, top-level or wrapped: the wrappers' `byteSize` just sums the
    /// nested `byteSize`, so a wrapped leaf drops its shared-arena payload the same way a top-level one does.
    std::vector<const ColumnAggregateFunction *> state_leaves;
    /// Whether cols[i] contains (or is) an aggregate-state leaf.
    std::vector<UInt8> col_has_states(cols.size(), 0);
    size_t plain_bytes = 0;
    for (size_t i = 0; i < cols.size(); ++i)
    {
        const auto & col = cols[i];
        size_t state_leaves_byte_size = 0;
        forEachAggregateStateLeaf(
            *col.column,
            [&](const ColumnAggregateFunction & leaf)
            {
                col_has_states[i] = 1;
                state_leaves.push_back(&leaf);
                state_leaves_byte_size += leaf.byteSize();
            });
        /// The carrier's own payload (null maps, sibling tuple elements, array offsets, ...) is sized by
        /// `byteSize` like any plain column; the state leaves are sized from their serialized states below.
        plain_bytes += col.column->byteSize() - state_leaves_byte_size;
    }
    const bool has_aggregate_states = !state_leaves.empty();

    /// Until the first sampled block lands there is no per-row figure to extrapolate from, so blocks
    /// racing with the first sampled one serialize their states too and count as extra samples.
    const bool serialize_states = has_aggregate_states
        && (sample_block || statistics.serialized_state_rows.load(std::memory_order_relaxed) == 0);
    size_t serialized_state_bytes = 0;
    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    if (serialize_states)
    {
        /// The same ~1000-sample target as `Aggregator::estimateSizeOfCompressedState` uses for a
        /// single-level hash table: serialized state sizes can be arbitrarily skewed, and a sample of 100
        /// swings the extrapolation several-fold depending on whether the few giant states land on the
        /// sampled positions. Blocks of up to 1000 states are measured exactly.
        static constexpr size_t max_states_to_serialize = 1000;
        for (const auto * aggregate_column : state_leaves)
        {
            /// One periodic sample yields both the uncompressed figure and the compression sample, so
            /// the `bytes / (sample_bytes / compressed_bytes)` estimate is derived from a single
            /// population of states even when state size or compressibility changes with key order
            /// inside the block, and the compression side stays behind the same cap instead of
            /// serializing a prefix of up to `min(8192, rows)` states a second time. The clamp of the
            /// compressed size to the sample size - a sample of tiny states must read as
            /// incompressible, not as expanding - lives in `sampledStateSizes`.
            const auto sizes = aggregate_column->sampledStateSizes(max_states_to_serialize);
            serialized_state_bytes += sizes.bytes;
            sample_bytes += sizes.sample_bytes;
            compressed_bytes += sizes.compressed_bytes;
        }
    }

    if (sample_block)
    {
        for (size_t i = 0; i < cols.size(); ++i)
        {
            /// Columns holding aggregate-state leaves are measured above, from the same sample as their
            /// uncompressed figure; serializing them here would write every state a second time.
            if (col_has_states[i])
                continue;
            auto [sample, compressed] = estimateCompressedColumnSize(cols[i]);
            sample_bytes += sample;
            compressed_bytes += compressed;
        }
    }

    std::lock_guard lock(statistics.mutex);
    size_t block_bytes = plain_bytes;
    if (serialize_states)
    {
        statistics.serialized_state_bytes += serialized_state_bytes;
        statistics.serialized_state_rows += num_rows;
        block_bytes += serialized_state_bytes;
    }
    else if (has_aggregate_states)
    {
        /// Every block of one statistics stream has the same layout, so the block's rows are a sound base
        /// for the per-row figure even when the block holds several aggregate-state columns.
        block_bytes += static_cast<size_t>(
            static_cast<double>(statistics.serialized_state_bytes) * static_cast<double>(num_rows)
            / static_cast<double>(statistics.serialized_state_rows.load(std::memory_order_relaxed)));
    }
    statistics.bytes += block_bytes;
    if (compressed_bytes)
    {
        statistics.sample_bytes += sample_bytes;
        statistics.compressed_bytes += compressed_bytes;
    }
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
}

void RuntimeDataflowStatisticsCacheUpdater::recordOutputChunk(const Chunk & chunk, const Block & header)
{
    chassert(chunk.getNumColumns() == header.columns());
    const auto & columns = chunk.getColumns();
    ColumnsWithTypeAndName cols;
    cols.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        cols.emplace_back(columns[i], header.getByPosition(i).type, "");
    recordColumns(output_bytes_statistics[OutputStatisticsType::OutputChunk], chunk.getNumRows(), cols);
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationStateSizes(AggregatedDataVariants & variant, ssize_t bucket)
{
    Stopwatch watch;

    /// We want to avoid situations when there is a single very large state (think of `SELECT uniqExact(col) FROM t`).
    /// Then we will spend a lot of time serializing it, and the overhead will be too high.
    if (variant.type == AggregatedDataVariants::Type::without_key
        && std::ranges::any_of(
            variant.aggregator->getParams().aggregates, [](auto agg_func) { return !agg_func.function->hasTrivialDestructor(); }))
    {
        markUnsupportedCase();
        return;
    }

    const auto estimate = variant.aggregator->estimateSizeOfCompressedState(variant, bucket);

    auto & statistics = output_bytes_statistics[OutputStatisticsType::AggregationState];
    std::lock_guard lock(statistics.mutex);
    statistics.bytes += estimate.bytes;
    statistics.sample_bytes += estimate.sample_bytes;
    statistics.compressed_bytes += estimate.compressed_bytes;
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationKeySizes(
    const Chunk & chunk, const ColumnNumbers & keys_positions, const DataTypes & key_types)
{
    const auto & columns = chunk.getColumns();
    ColumnsWithTypeAndName cols;
    cols.reserve(keys_positions.size());
    for (size_t i = 0; i < keys_positions.size(); ++i)
        cols.emplace_back(columns[keys_positions[i]], key_types[i], "");
    recordColumns(output_bytes_statistics[OutputStatisticsType::AggregationKeys], chunk.getNumRows(), cols);
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationStateColumnSizes(
    const Chunk & chunk, const ColumnNumbers & keys_positions, const Block & header)
{
    const auto & columns = chunk.getColumns();

    /// Mark key columns so we can skip them — only non-key columns are aggregate states.
    std::vector<bool> is_key(columns.size(), false);
    for (auto pos : keys_positions)
        is_key[pos] = true;

    ColumnsWithTypeAndName cols;
    cols.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (is_key[i])
            continue;
        cols.emplace_back(columns[i], header.getByPosition(i).type, "");
    }
    recordColumns(output_bytes_statistics[OutputStatisticsType::AggregationState], chunk.getNumRows(), cols);
}

void RuntimeDataflowStatisticsCacheUpdater::recordInputColumns(
    const ColumnsWithTypeAndName & input_columns,
    const NamesAndTypesList & part_columns,
    const ColumnSizeByName & column_sizes,
    size_t read_bytes,
    std::optional<bool> & should_continue_sampling)
{
    Stopwatch watch;

    const auto type = read_bytes ? InputStatisticsType::WithByteHint : InputStatisticsType::WithoutByteHint;
    if (type == InputStatisticsType::WithoutByteHint)
    {
        for (const auto & column : input_columns)
            read_bytes += column.column->byteSize();
    }

    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    auto & statistics = input_bytes_statistics[type];
    if (read_bytes && !input_columns.empty())
    {
        if (!column_sizes.empty())
        {
            for (const auto & column : input_columns)
            {
                if (column_sizes.contains(column.name))
                {
                    const auto compressed_ratio = column_sizes.at(column.name).data_uncompressed
                        ? (static_cast<double>(column_sizes.at(column.name).data_compressed)
                           / static_cast<double>(column_sizes.at(column.name).data_uncompressed))
                        : 1.0;
                    sample_bytes += column.column->byteSize();
                    compressed_bytes += static_cast<size_t>(static_cast<double>(column.column->byteSize()) * compressed_ratio);
                }
            }
        }
        else
        {
            if (!should_continue_sampling.has_value())
                should_continue_sampling = shouldSampleBlock(statistics, input_columns[0].column->size());

            // We don't have individual column size info, likely because it is a compact part. Let's try to estimate it.
            if (*should_continue_sampling)
            {
                for (const auto & column : input_columns)
                {
                    // Paranoid check in case some, e.g., prewhere filter columns are present among the input columns
                    if (part_columns.contains(column.name))
                    {
                        const auto [sample, compressed] = estimateCompressedColumnSize(column);
                        sample_bytes += sample;
                        compressed_bytes += compressed;
                    }
                }
            }
        }
    }

    std::lock_guard lock(statistics.mutex);
    statistics.bytes += read_bytes;
    if (compressed_bytes)
    {
        statistics.sample_bytes += sample_bytes;
        statistics.compressed_bytes += compressed_bytes;
    }
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
}

RuntimeDataflowStatisticsCache & getRuntimeDataflowStatisticsCache()
{
    static RuntimeDataflowStatisticsCache stats_cache;
    return stats_cache;
}

RuntimeDataflowStatisticsCollector::RuntimeDataflowStatisticsCollector(
    SharedHeader header_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : ISimpleTransform(header_, header_, /*skip_empty_chunks=*/false)
    , updater(std::move(updater_))
{
}

void RuntimeDataflowStatisticsCollector::transform(Chunk & chunk)
{
    if (updater)
        updater->recordOutputChunk(chunk, getOutputPort().getHeader());
}
}
