#include <Core/ProtocolDefines.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/SortDescription.h>
#include <base/unit.h>
#include <Core/Defines.h>
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

/// How many rows to gather before the compression ratio is taken off them, and the memory that is
/// allowed to cost. A codec measures a ratio only as good as the window it is given: on one and the same
/// `String` key column `ZSTD(3)` reaches 1.46x over 2.8 KB and 3.30x over 553 KB. Compressing each output
/// block on its own therefore measured whatever window the output happened to be split into - a
/// single-stream aggregation emits a few large blocks, a multi-stream one many small blocks - and made the
/// estimate of a `GROUP BY` on a `String` key differ 2.3x between one thread and two. The budget is a
/// block's worth of rows, so the window matches what a replica compresses when it sends one.
static constexpr size_t sample_rows_budget = DEFAULT_BLOCK_SIZE;
static constexpr size_t sample_memory_budget = 4_MiB;
/// What the destructor may compress of a sample that never reached the budget, see the call there.
static constexpr size_t leftover_sample_rows_cap = 8192;

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

    /// A sample that filled was compressed by the thread that filled it and leaves nothing to do here. One
    /// that did not - an output smaller than the budget, which is most of them - is compressed here, on
    /// the query's teardown path, so cap what that can cost: `leftover_sample_rows_cap` rows is a window
    /// wide enough for a ratio and a couple of milliseconds of work at the very most. The samples that
    /// reach here in practice are far smaller than the cap anyway: a few thousand rows where the bucket
    /// Top-K conversion bounds what each call offers, a few hundred where the output has that few groups.
    for (size_t i = 0; i < OutputStatisticsType::MaxOutputType; ++i)
        compressSample(output_bytes_statistics[i], leftover_sample_rows_cap);

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

bool isSerializedAsSingleStreamOfColumnType(const ISerialization & serialization, const DataTypePtr & type)
{
    size_t num_streams = 0;
    bool stream_is_column_itself = false;
    serialization.enumerateStreams(
        [&](const auto & substream_path)
        {
            ++num_streams;
            const auto & substream_type = substream_path.back().data.type;
            stream_is_column_itself
                = ISerialization::isSpecialCompressionAllowed(substream_path) && substream_type && substream_type->equals(*type);
        },
        type);
    return num_streams == 1 && stream_is_column_itself;
}

/// Tries to estimate compressed size of a column by serializing a sample of it.
static std::pair<size_t, size_t>
estimateCompressedColumnSize(const ColumnWithTypeAndName & column, const ColumnCodecs & codecs, bool compress_all_rows = false)
{
    auto [serialization, _, column_to_write] = NativeWriter::getSerializationAndColumn(DBMS_TCP_PROTOCOL_VERSION, column);
    /// What the sample buffer holds is only known here: the serialization is picked from the column at
    /// hand, so a column stored `Sparse` (or `Replicated`) arrives as several substreams funnelled into
    /// the one buffer below, and the column read from the part keeps the pre-`ALTER` type until the
    /// mutation that rewrites it has run. A type-specific codec then measures a stream it was never
    /// resolved for. Some codecs merely mismeasure it, but one that validates its input rejects it
    /// outright - `ALP` throws `CANNOT_COMPRESS` on a byte count that is not a whole number of floats.
    const bool sample_matches_type_specific_codec = codecs.type_specific && codecs.type_specific_for->equals(*column.type)
        && isSerializedAsSingleStreamOfColumnType(*serialization, column.type);
    const auto & codec = sample_matches_type_specific_codec ? codecs.type_specific : codecs.generic;
    NullWriteBuffer null_buf;
    CompressedWriteBuffer compressed_buf(null_buf, codec);
    // To avoid spending too much time on serialization, we limit the number of rows to serialize - unless
    // the caller gathered the sample itself and wants all of it compressed as one window.
    const auto limit = compress_all_rows
        ? column_to_write->size()
        : std::max<size_t>(std::min(8192ul, column_to_write->size()), column_to_write->size() / 10);
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
    /// Every block until the sample is full, rather than one in five: a block reaching here can be a few
    /// dozen rows - the bucket Top-K conversion hands over a bounded sample of each bucket - and skipping
    /// four in five of those left the sample short of a block's worth however much output there was.
    statistics.counter.fetch_add(1, std::memory_order_relaxed);
    return !statistics.sample_full.load(std::memory_order_relaxed);
}

bool RuntimeDataflowStatisticsCacheUpdater::appendToSample(
    Statistics & statistics, const ColumnsWithTypeAndName & cols, size_t num_rows)
{
    if (statistics.sample_columns.empty())
    {
        statistics.sample_types.reserve(cols.size());
        statistics.sample_columns.reserve(cols.size());
        for (const auto & col : cols)
        {
            statistics.sample_types.push_back(col.type);
            statistics.sample_columns.push_back(col.type->createColumn());
        }
    }
    else if (statistics.sample_columns.size() != cols.size())
    {
        /// The header changed under us, which the callers of one statistics type do not do. Keep what was
        /// gathered rather than mixing two shapes into one sample.
        statistics.sample_full.store(true, std::memory_order_relaxed);
        return true;
    }

    const size_t rows = std::min(num_rows, sample_rows_budget - statistics.sample_rows);
    for (size_t i = 0; i < cols.size(); ++i)
        statistics.sample_columns[i]->insertRangeFrom(*cols[i].column, 0, rows);
    statistics.sample_rows += rows;

    statistics.sample_memory = 0;
    for (const auto & col : statistics.sample_columns)
        statistics.sample_memory += col->byteSize();

    if (statistics.sample_rows < sample_rows_budget && statistics.sample_memory < sample_memory_budget)
        return false;

    statistics.sample_full.store(true, std::memory_order_relaxed);
    return true;
}

void RuntimeDataflowStatisticsCacheUpdater::compressSample(Statistics & statistics, size_t max_rows)
{
    Stopwatch watch;
    ColumnsWithTypeAndName cols;
    size_t sample_bytes = 0;
    size_t compressed_bytes = 0;
    {
        std::lock_guard lock(statistics.mutex);
        if (statistics.sample_columns.empty() || !statistics.sample_rows)
            return;
        const size_t rows = max_rows ? std::min(max_rows, statistics.sample_rows) : statistics.sample_rows;
        cols.reserve(statistics.sample_columns.size());
        for (size_t i = 0; i < statistics.sample_columns.size(); ++i)
        {
            auto column = rows < statistics.sample_rows ? statistics.sample_columns[i]->cut(0, rows)
                                                        : ColumnPtr(std::move(statistics.sample_columns[i]));
            cols.emplace_back(std::move(column), statistics.sample_types[i], "");
        }
        statistics.sample_columns.clear();
    }

    /// Only output columns get here, and they model what a replica sends to the initiator rather than
    /// anything stored in a part, so there is no column `CODEC` to resolve as in `recordInputColumns`.
    /// The transfer codec is `network_compression_method`, whose default the default codec matches.
    /// It is generic, so it applies to any serialization layout.
    const ColumnCodecs codecs{.generic = CompressionCodecFactory::instance().getDefaultCodec()};
    for (const auto & col : cols)
    {
        auto [sample, compressed] = estimateCompressedColumnSize(col, codecs, /*compress_all_rows=*/true);
        sample_bytes += sample;
        compressed_bytes += compressed;
    }

    std::lock_guard lock(statistics.mutex);
    statistics.sample_bytes += sample_bytes;
    statistics.compressed_bytes += compressed_bytes;
    statistics.elapsed_microseconds += watch.elapsedMicroseconds();
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

    ColumnsWithTypeAndName sorted;
    const bool sampling = shouldSampleBlock(statistics, num_rows);
    if (sampling && key_order)
    {
        /// Built only for a block that is actually sampled - one in five, until the sample is full.
        const auto permutation = key_order();
        sorted = cols;
        for (auto & col : sorted)
            col.column = col.column->permute(permutation, 0);
    }

    bool sample_just_filled = false;
    {
        std::lock_guard lock(statistics.mutex);
        statistics.bytes += block_bytes;
        if (sampling)
            sample_just_filled = appendToSample(statistics, key_order ? sorted : cols, num_rows);
        statistics.elapsed_microseconds += watch.elapsedMicroseconds();
    }

    /// Compress it here, on the thread that filled it and while the query is still running, rather than
    /// leaving a block's worth of rows for whichever thread tears the updater down at the end.
    if (sample_just_filled)
        compressSample(statistics);
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

    size_t res = variant.aggregator->estimateSizeOfCompressedState(variant, bucket);

    auto & statistics = output_bytes_statistics[OutputStatisticsType::AggregationState];
    std::lock_guard lock(statistics.mutex);
    statistics.bytes += res;
    statistics.sample_bytes += res;
    statistics.compressed_bytes += res;
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
    recordColumns(
        output_bytes_statistics[OutputStatisticsType::AggregationKeys],
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
        output_bytes_statistics[OutputStatisticsType::AggregationKeys], num_rows, cols, full_key_bytes, key_order);
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
        output_bytes_statistics[OutputStatisticsType::AggregationState],
        chunk.getNumRows(),
        cols,
        /*full_bytes=*/{},
        keyOrderProviderFor(columns, keys_positions, getKeyTypesFrom(header, keys_positions)));
}

void RuntimeDataflowStatisticsCacheUpdater::recordInputColumns(
    const ColumnsWithTypeAndName & input_columns,
    const NameSet & partially_read_columns,
    const NamesAndTypesList & part_columns,
    const ColumnSizeByName & column_sizes,
    const ColumnCodecByName & column_codecs,
    const CompressionCodecPtr & default_codec,
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
        else if (std::ranges::any_of(
                     input_columns, [&](const auto & column) { return partially_read_columns.contains(column.name); }))
        {
            /// Partially read columns (e.g. only the offsets of an array whose data is missing from the part)
            /// are internally inconsistent until `fillMissingColumns` completes them, so they cannot be
            /// serialized for the sample below. Excluding just those columns would poison the statistics:
            /// the compression ratio would be derived from the surviving columns only, but applied to
            /// `read_bytes` of the whole block, which includes the bytes of the skipped column. There is no
            /// per-column byte split to subtract here (unlike the `column_sizes` branch above, which never
            /// serializes and handles such columns fine), so give up on the statistics for this query.
            markUnsupportedCase();
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
                        const auto codec_it = column_codecs.find(column.name);
                        /// A column with no `CODEC` of its own is written with the part's default codec,
                        /// which is generic, so it applies to any serialization layout.
                        const auto [sample, compressed] = estimateCompressedColumnSize(
                            column,
                            codec_it == column_codecs.end() ? ColumnCodecs{.generic = default_codec} : codec_it->second);
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
