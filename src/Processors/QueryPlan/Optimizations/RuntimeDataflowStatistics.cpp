#include <Core/ProtocolDefines.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/SortDescription.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/NullWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/sortBlock.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <optional>


namespace ProfileEvents
{
extern const Event RuntimeDataflowStatisticsInputBytes;
extern const Event RuntimeDataflowStatisticsOutputBytes;
extern const Event RuntimeDataflowStatisticsInputNanoseconds;
extern const Event RuntimeDataflowStatisticsOutputNanoseconds;
}

namespace DB
{

std::optional<RuntimeDataflowStatisticsCache::Entry> RuntimeDataflowStatisticsCache::getStats(size_t key) const
{
    if (const auto entry = stats_cache->get(key))
        return *entry;
    return std::nullopt;
}

std::optional<RuntimeDataflowStatisticsCache::Boundary> RuntimeDataflowStatisticsCache::getBoundary(UInt64 root_key) const
{
    if (const auto boundary = boundary_cache->get(root_key))
        return *boundary;
    return std::nullopt;
}

void RuntimeDataflowStatisticsCache::setBoundary(UInt64 root_key, Boundary boundary)
{
    boundary_cache->set(root_key, std::make_shared<Boundary>(boundary));
}

void RuntimeDataflowStatisticsCache::update(size_t key, RuntimeDataflowStatistics stats)
{
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsInputBytes, stats.input_bytes);
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsOutputBytes, stats.output_bytes);
    stats_cache->set(key, std::make_shared<RuntimeDataflowStatistics>(stats));
}

RuntimeDataflowStatisticsBlock::~RuntimeDataflowStatisticsBlock()
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

    RuntimeDataflowStatistics res{
        .input_bytes = input_bytes_read.load(std::memory_order_relaxed),
        .duplicated_bytes = duplicated_bytes_read.load(std::memory_order_relaxed),
        .total_rows_to_read = total_rows_to_read};
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
        "Collected statistics: input bytes={}, duplicated bytes={}, output bytes={}",
        res.input_bytes,
        res.duplicated_bytes,
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
static std::pair<size_t, size_t> estimateCompressedColumnSize(const ColumnWithTypeAndName & column, const CompressionCodecPtr & codec)
{
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    NullWriteBuffer null_buf;
    CompressedWriteBuffer compressed_buf(null_buf, codec);
    // To avoid spending too much time on serialization, we limit the number of rows to serialize.
    const auto limit = std::max<size_t>(std::min(8192ul, column_to_write->size()), column_to_write->size() / 10);
    NativeWriter::writeData(*serialization, column_to_write, compressed_buf, std::nullopt, 0, limit, DBMS_TCP_PROTOCOL_VERSION);
    compressed_buf.finalize();
    // Return pair of (sample size, compressed size), note that both sizes are based on limited number of rows.
    return std::make_pair(compressed_buf.count(), null_buf.count());
}

/// The replicas merge their aggregation output with memory-bound merging, which sorts it by the group
/// by keys; the single-node merge this sample is taken from leaves it in hash-table order. So what a
/// replica puts on the wire is a sorted block, and pricing an unsorted sample of the very same rows
/// misses by the whole difference between the two - a monotonic `UInt64` key column compresses 7.8x
/// sorted and 3.0-3.9x in hash order.
///
/// The description is rebuilt ascending here instead of carrying the replica's own
/// `group_by_sort_description` down: what the ratio responds to is equal and neighbouring keys
/// ending up adjacent, which every direction and null placement gives equally.
static IColumn::Permutation keyOrderPermutation(
    const Columns & columns, const ColumnNumbers & keys_positions, const DataTypes & key_types)
{
    ColumnsWithTypeAndName key_columns;
    SortDescription description;
    key_columns.reserve(keys_positions.size());
    description.reserve(keys_positions.size());
    for (size_t i = 0; i < keys_positions.size(); ++i)
    {
        auto name = "__key_" + toString(i);
        key_columns.emplace_back(columns[keys_positions[i]], key_types[i], name);
        description.emplace_back(std::move(name), 1);
    }

    IColumn::Permutation permutation;
    stableGetPermutation(Block(key_columns), description, permutation);
    return permutation;
}

bool RuntimeDataflowStatisticsCacheUpdater::shouldSampleBlock(Statistics & statistics, size_t block_rows)
{
    // Empty blocks produced during planning, when we calculate output headers. Skip them.
    if (!block_rows)
        return false;
    const auto counter = statistics.counter.fetch_add(1, std::memory_order_relaxed);
    return counter % 5 == 0 && counter < 25;
}

void RuntimeDataflowStatisticsCacheUpdater::recordColumns(
    Statistics & statistics,
    size_t num_rows,
    const ColumnsWithTypeAndName & cols,
    std::optional<size_t> full_bytes,
    const KeyOrderProvider & key_order)
{
    Stopwatch watch;

    size_t block_bytes = 0;
    if (full_bytes)
    {
        block_bytes = *full_bytes;
    }
    else
    {
        for (const auto & col : cols)
            block_bytes += col.column->byteSize();
    }

    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    if (shouldSampleBlock(statistics, num_rows))
    {
        /// Only output columns get here, and they model what a replica sends to the initiator: the transfer codec is
        /// `network_compression_method`, whose default the default codec matches.
        const auto codec = CompressionCodecFactory::instance().getDefaultCodec();
        /// Built only for a block that is actually sampled - one in five, and at most five per query.
        ColumnsWithTypeAndName sorted;
        if (key_order)
        {
            const auto permutation = key_order();
            sorted = cols;
            for (auto & col : sorted)
                col.column = col.column->permute(permutation, 0);
        }
        for (const auto & col : (key_order ? sorted : cols))
        {
            auto [sample, compressed] = estimateCompressedColumnSize(col, codec);
            sample_bytes += sample;
            compressed_bytes += compressed;
        }
    }

    std::lock_guard lock(statistics.mutex);
    statistics.bytes += block_bytes;
    if (compressed_bytes)
    {
        statistics.sample_bytes += sample_bytes;
        statistics.compressed_bytes += compressed_bytes;
    }
    /// Reported in nanoseconds: a call often takes well under a microsecond, and rounding each one down would add up to nothing.
    const auto elapsed_nanoseconds = watch.elapsedNanoseconds();
    statistics.elapsed_microseconds += elapsed_nanoseconds / 1000;
    /// Only output columns get here, see above.
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsOutputNanoseconds, elapsed_nanoseconds);
}

static DataTypes getKeyTypesFrom(const Block & header, const ColumnNumbers & keys_positions)
{
    DataTypes types;
    types.reserve(keys_positions.size());
    for (auto pos : keys_positions)
        types.push_back(header.getByPosition(pos).type);
    return types;
}

RuntimeDataflowStatisticsCacheUpdater::KeyOrderProvider RuntimeDataflowStatisticsCacheUpdater::keyOrderProviderFor(
    const Columns & columns, const ColumnNumbers & keys_positions, const DataTypes & key_types) const
{
    if (!replicas_send_output_in_key_order || keys_positions.empty())
        return {};
    return [&columns, &keys_positions, key_types] { return keyOrderPermutation(columns, keys_positions, key_types); };
}

void RuntimeDataflowStatisticsCacheUpdater::recordOutputChunk(const Chunk & chunk, const Block & header)
{
    chassert(chunk.getNumColumns() == header.columns());
    const auto & columns = chunk.getColumns();
    ColumnsWithTypeAndName cols;
    cols.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
        cols.emplace_back(columns[i], header.getByPosition(i).type, "");
    recordColumns(block->output_bytes_statistics[OutputStatisticsType::OutputChunk], chunk.getNumRows(), cols);
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

    size_t res = variant.aggregator->estimateSizeOfCompressedState(variant, bucket);

    auto & statistics = block->output_bytes_statistics[OutputStatisticsType::AggregationState];
    std::lock_guard lock(statistics.mutex);
    statistics.bytes += res;
    statistics.sample_bytes += res;
    statistics.compressed_bytes += res;
    const auto elapsed_nanoseconds = watch.elapsedNanoseconds();
    statistics.elapsed_microseconds += elapsed_nanoseconds / 1000;
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsOutputNanoseconds, elapsed_nanoseconds);
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationKeySizes(
    const Chunk & chunk, const ColumnNumbers & keys_positions, const DataTypes & key_types)
{
    const auto & columns = chunk.getColumns();
    ColumnsWithTypeAndName cols;
    cols.reserve(keys_positions.size());
    for (size_t i = 0; i < keys_positions.size(); ++i)
        cols.emplace_back(columns[keys_positions[i]], key_types[i], "");
    recordColumns(
        block->output_bytes_statistics[OutputStatisticsType::AggregationKeys],
        chunk.getNumRows(),
        cols,
        /*full_bytes=*/{},
        keyOrderProviderFor(columns, keys_positions, key_types));
}

void RuntimeDataflowStatisticsCacheUpdater::recordAggregationKeySizes(
    const Chunk & chunk,
    const ColumnNumbers & keys_positions,
    const DataTypes & key_types,
    size_t full_key_bytes,
    const Columns & untruncated_sample_columns)
{
    ColumnsWithTypeAndName cols;
    cols.reserve(keys_positions.size());
    size_t num_rows = chunk.getNumRows();
    /// Which columns carry the keys below, and where: the sample holds the key columns alone, the chunk
    /// holds them at their own positions. The key order the replicas send in is taken from whichever of
    /// the two is priced.
    KeyOrderProvider key_order;
    ColumnNumbers sample_positions;
    if (!untruncated_sample_columns.empty())
    {
        /// The byte count describes the untruncated keys, so the ratio it is divided by has to come from
        /// them as well - the chunk holds the kept groups only, and their keys compress differently.
        /// When every group was rejected the chunk holds nothing at all, and without a ratio the byte
        /// count is dropped rather than estimated. The conversion kept a bounded sample for both cases.
        chassert(untruncated_sample_columns.size() == keys_positions.size());
        for (size_t i = 0; i < untruncated_sample_columns.size(); ++i)
            cols.emplace_back(untruncated_sample_columns[i], key_types[i], "");
        num_rows = untruncated_sample_columns.front()->size();

        sample_positions.resize(untruncated_sample_columns.size());
        for (size_t i = 0; i < sample_positions.size(); ++i)
            sample_positions[i] = i;
        key_order = keyOrderProviderFor(untruncated_sample_columns, sample_positions, key_types);
    }
    else
    {
        const auto & columns = chunk.getColumns();
        for (size_t i = 0; i < keys_positions.size(); ++i)
            cols.emplace_back(columns[keys_positions[i]], key_types[i], "");
        key_order = keyOrderProviderFor(columns, keys_positions, key_types);
    }

    recordColumns(
        block->output_bytes_statistics[OutputStatisticsType::AggregationKeys], num_rows, cols, full_key_bytes, key_order);
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
    recordColumns(
        block->output_bytes_statistics[OutputStatisticsType::AggregationState],
        chunk.getNumRows(),
        cols,
        /*full_bytes=*/{},
        keyOrderProviderFor(columns, keys_positions, getKeyTypesFrom(header, keys_positions)));
}

void RuntimeDataflowStatisticsCacheUpdater::recordInputBytesRead(std::optional<size_t> compressed_bytes_read)
{
    Stopwatch watch;
    if (!compressed_bytes_read)
        markUnsupportedCase();
    else
        (duplicated ? block->duplicated_bytes_read : block->input_bytes_read).fetch_add(*compressed_bytes_read, std::memory_order_relaxed);
    ProfileEvents::increment(ProfileEvents::RuntimeDataflowStatisticsInputNanoseconds, watch.elapsedNanoseconds());
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
