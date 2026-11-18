#include <Processors/QueryPlan/ReadFromGPUCompressedColumns.h>

#if USE_GPU

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Compression/CompressionFactory.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <GPU/GPUAccumulator.h>
#include <GPU/GPUColumns.h>
#include <GPU/GPUTypeMapping.h>
#include <Storages/ColumnsDescription.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Processors/IAccumulatingTransform.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/ColumnSize.h>
#include <Storages/MergeTree/MergeTreeCompressedColumnsReader.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Common/JSONBuilder.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <numeric>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace
{

using ColumnToReduce = ReadFromGPUCompressedColumns::ColumnToReduce;
using DeviceFilter = ReadFromGPUCompressedColumns::DeviceFilter;
using AggregateToCompute = ReadFromGPUCompressedColumns::AggregateToCompute;
using DerivedColumn = ReadFromGPUCompressedColumns::DerivedColumn;
using PartToRead = ReadFromGPUCompressedColumns::PartToRead;

struct SharedState
{
    std::vector<NameAndTypePair> keys;
    std::vector<ColumnToReduce> columns;
    std::vector<DerivedColumn> derived;
    std::vector<AggregateToCompute> aggregates;
    std::optional<Columns> result_over_no_rows;
    std::optional<DeviceFilter> filter;
    std::optional<ReadFromGPUCompressedColumns::GroupFilter> group_filter;
    std::vector<ReadFromGPUCompressedColumns::PartToRead> parts;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    MergeTreeReaderSettings reader_settings = MergeTreeReaderSettings::createFromSettings();
    size_t batch_bytes;
    size_t num_readers = 1;
    GPUDecompressionMode decompression = GPUDecompressionMode::DEVICE;
    bool collect = false;
    /// Of each column of the filter: the key or the value column of the same name and type, among the keys and then
    /// the values, which is read once for both.
    std::vector<std::optional<uint32_t>> filter_aliases;

    std::shared_ptr<const GPU::GPUCompressedLayout> layout;
    std::shared_ptr<GPU::GPUGroupingDecision> decision;
    /// Whether the groups of the lanes are merged by parts of their keys, each part on its own, see `GPUSplitLanes`;
    /// otherwise all into one lane, see `GPUMergeGroups`.
    bool merge_by_parts = false;
    size_t num_parts = 1;

    std::atomic<size_t> next_part{0};
};

using SharedStatePtr = std::shared_ptr<SharedState>;

/// Whether a column of a part is compressed well enough for the device to expand it. Expanding
/// costs the device about as long per byte as the link takes to carry one, and keeps it from
/// grouping meanwhile, so a column that compression barely shrinks is cheaper sent whole.
bool expandsOnDevice(GPUDecompressionMode mode)
{
    return mode == GPUDecompressionMode::DEVICE;
}

GPU::GPUCodec codecOrThrow(UInt8 method, std::string_view payload, const String & column_name, const IMergeTreeDataPart & part)
{
    const auto codec = GPU::deviceCodecOf(method, payload);
    if (!codec)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Column {} of part {} is compressed with method {:#x}, which the device cannot expand",
            column_name,
            part.name,
            static_cast<UInt16>(method));
    return *codec;
}

const std::vector<NameAndTypePair> & filterColumnsOf(const SharedState & state)
{
    static const std::vector<NameAndTypePair> none;
    return state.filter ? state.filter->columns : none;
}

DataTypes typesOf(const std::vector<NameAndTypePair> & columns)
{
    DataTypes types;
    types.reserve(columns.size());
    for (const auto & column : columns)
        types.push_back(column.type);
    return types;
}

/// Whether every column the read takes is of a fixed width, so that a part can be read in ranges of its marks.
bool readsFixedWidthOnly(const SharedState & state)
{
    const auto fixed = [](const NameAndTypePair & column) { return !isString(*column.type); };
    return std::ranges::all_of(state.keys, fixed) && std::ranges::all_of(state.columns, [&](const auto & column) { return fixed(column.column); })
        && std::ranges::all_of(filterColumnsOf(state), fixed);
}

/// A piece smaller than this is not split: a reader gathers about a batch of rows before the device groups them.
constexpr size_t min_rows_to_split = 1UL << 20;

/// Splits the parts into at least `min_pieces` pieces of ranges of marks, the largest in two each time, so that as many
/// readers as there are have rows to read however few parts the table has: a table of one part after a merge was read
/// by one reader. A piece of several ranges is split between its ranges, one of one range at its middle mark.
void splitPartsAmongReaders(std::vector<PartToRead> & parts, size_t min_pieces)
{
    /// No parts are left of a read whose indexes leave no marks.
    if (parts.empty() || parts.size() >= min_pieces)
        return;

    struct Piece
    {
        DataPartPtr part;
        MarkRanges ranges;
        size_t rows = 0;
    };

    std::vector<Piece> pieces;
    pieces.reserve(min_pieces);
    for (auto & part_to_read : parts)
    {
        const auto & granularity = *part_to_read.part->index_granularity;
        MarkRanges ranges = part_to_read.ranges.empty() ? MarkRanges{MarkRange(0, granularity.getMarksCountWithoutFinal())} : part_to_read.ranges;
        const size_t rows = granularity.getRowsCountInRanges(ranges);
        pieces.push_back({.part = part_to_read.part, .ranges = std::move(ranges), .rows = rows});
    }

    while (pieces.size() < min_pieces)
    {
        auto largest = std::ranges::max_element(pieces, {}, &Piece::rows);
        if (largest->rows < 2 * min_rows_to_split)
            break;

        const auto & granularity = *largest->part->index_granularity;
        Piece second{.part = largest->part, .ranges = {}, .rows = 0};
        if (largest->ranges.size() > 1)
        {
            /// From the range at which the rows so far pass half.
            size_t rows_before = 0;
            size_t split_at = 0;
            while (split_at + 1 < largest->ranges.size() && rows_before + granularity.getRowsCountInRange(largest->ranges[split_at]) <= largest->rows / 2)
                rows_before += granularity.getRowsCountInRange(largest->ranges[split_at++]);
            if (split_at == 0)
                split_at = 1;
            second.ranges.assign(largest->ranges.begin() + split_at, largest->ranges.end());
            largest->ranges.resize(split_at);
        }
        else
        {
            MarkRange & range = largest->ranges.front();
            if (range.end - range.begin < 2)
                break;
            const size_t middle = (range.begin + range.end) / 2;
            second.ranges.push_back(MarkRange(middle, range.end));
            range.end = middle;
        }
        second.rows = granularity.getRowsCountInRanges(second.ranges);
        largest->rows -= second.rows;
        pieces.push_back(std::move(second));
    }

    parts.clear();
    parts.reserve(pieces.size());
    for (auto & piece : pieces)
        parts.push_back({.part = std::move(piece.part), .ranges = std::move(piece.ranges)});
}

/// Readers of parts on a remote disk, such as S3: a reader there waits for each response most of the
/// time, and the read is bound by how many requests are in flight rather than by the link to the
/// device. Over cold TPC-H SF100 on S3 on an A10G, with 8 readers against 16, Q6 took 5.7 s against
/// 2.6 s, Q12 5.8 s against 2.7 s and Q14 5.5 s against 3.1 s. A reader holds about a batch of rows
/// of each column on the device: Q1, of seven columns, took 16 GiB of the device with 16 readers.
constexpr size_t remote_disk_readers = 16;

/// The lanes that group the batches, at most: as Sirius runs 4 to 8 tasks on a device at once, enough that one waiting
/// for the device leaves it to the others, and few, as each keeps groups of its own.
constexpr size_t max_lanes = 8;

/// Two readers fill the link when they only copy compressed blocks into pinned memory; a reader
/// that also expands the columns is bound by the CPU, and a quarter of the cores keep up with the
/// link, where more of them hold more buffers on both sides of it than they gain.
size_t automaticReaders(const SharedState & state)
{
    if (std::ranges::any_of(state.parts, [](const PartToRead & part) { return part.part->isStoredOnRemoteDisk(); }))
        return remote_disk_readers;
    if (expandsOnDevice(state.decompression))
        return 2;
    return std::max<size_t>(2, getNumberOfCPUCoresToUse() / 4);
}

DataTypes argumentTypesOf(const std::vector<ColumnToReduce> & columns)
{
    DataTypes types;
    types.reserve(columns.size());
    for (const auto & column : columns)
        types.push_back(column.column.type);
    return types;
}

std::vector<GPU::GPUGroupByAggregate> groupByAggregatesOf(
    const std::vector<ColumnToReduce> & columns, const std::vector<DerivedColumn> & derived, const std::vector<AggregateToCompute> & aggregates)
{
    std::vector<GPU::GPUGroupByAggregate> result;
    result.reserve(aggregates.size());
    for (const auto & aggregate : aggregates)
    {
        const IDataType * argument_type = nullptr;
        uint32_t column = GPU::GPUGroupByAggregate::no_column;
        if (aggregate.column)
        {
            argument_type = columns.at(*aggregate.column).column.type.get();
            column = static_cast<uint32_t>(*aggregate.column);
        }
        else if (aggregate.derived)
        {
            argument_type = derived.at(*aggregate.derived).type.get();
            column = static_cast<uint32_t>(columns.size() + *aggregate.derived);
        }
        result.push_back({
            .aggregation = aggregate.aggregation,
            .column = column,
            .result_type = GPU::groupByResultTypeOrThrow(argument_type, *aggregate.result_type, aggregate.aggregation),
        });
    }
    return result;
}

std::vector<GPU::GPUDerivedValue> derivedValuesOf(const std::vector<DerivedColumn> & derived)
{
    std::vector<GPU::GPUDerivedValue> result;
    result.reserve(derived.size());
    for (const auto & column : derived)
        result.push_back({.expression = column.expression, .columns = GPU::GPUSpan<uint32_t>(column.columns)});
    return result;
}

/// The read is a pipeline of processors, each a stage of the device side, see `GPUAccumulator.h`:
///
///     GPUCompressedBlocks ×N  ->  GPUUpload ×N  ->  resize N -> L  ->  GPUGrouping ×L  ->  resize L -> 1  ->  GPUMergeGroups
///
/// The chunks between them have no columns: they carry a chunk info, staged blocks or a batch on the device or the
/// groups of a lane. A source and its uploader are a stream each, the lanes take the batches of any stream, as the
/// resize hands them over, and the groups of the lanes are merged at the end. A read that collects the rows its `WHERE`
/// keeps has no merge: each lane emits the rows it collected.

/// The rows a lane collects on the device before it sends them to the host.
constexpr size_t collect_flush_rows = 1UL << 20;

SharedHeader emptyHeader()
{
    return std::make_shared<const Block>();
}

/// Staged blocks of a reader, which go back to its pool once the last chunk that holds them is gone.
struct StagedBatchHolder
{
    StagedBatchHolder(std::unique_ptr<GPU::GPUStagedBatch> batch_, std::shared_ptr<GPU::GPUStagedBatchPool> pool_)
        : batch(std::move(batch_)), pool(std::move(pool_))
    {
    }

    StagedBatchHolder(const StagedBatchHolder &) = delete;
    StagedBatchHolder & operator=(const StagedBatchHolder &) = delete;

    std::unique_ptr<GPU::GPUStagedBatch> batch;
    std::shared_ptr<GPU::GPUStagedBatchPool> pool;

    ~StagedBatchHolder()
    {
        if (batch)
            pool->give(std::move(batch));
    }
};

struct StagedBatchInfo : public ChunkInfoCloneable<StagedBatchInfo>
{
    explicit StagedBatchInfo(std::shared_ptr<StagedBatchHolder> holder_) : holder(std::move(holder_)) { }
    std::shared_ptr<StagedBatchHolder> holder;
};

struct DeviceBatchInfo : public ChunkInfoCloneable<DeviceBatchInfo>
{
    explicit DeviceBatchInfo(std::shared_ptr<GPU::GPUDeviceBatch> batch_) : batch(std::move(batch_)) { }
    std::shared_ptr<GPU::GPUDeviceBatch> batch;
};

struct LaneGroupsInfo : public ChunkInfoCloneable<LaneGroupsInfo>
{
    explicit LaneGroupsInfo(std::shared_ptr<GPU::GPUGroupingLane> lane_) : lane(std::move(lane_)) { }
    std::shared_ptr<GPU::GPUGroupingLane> lane;
};

template <typename Info>
std::shared_ptr<Info> infoOrThrow(Chunk & chunk, std::string_view processor)
{
    auto info = chunk.getChunkInfos().extract<Info>();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A chunk without the chunk info that {} takes", processor);
    return info;
}

/// Reads the parts no other source took yet, one at a time, and emits their compressed blocks, staged in pinned memory,
/// a batch per chunk. It does not use the device.
class GPUCompressedBlocksSource : public ISource
{
public:
    GPUCompressedBlocksSource(SharedStatePtr state_, QueryStatusPtr query_status_)
        : ISource(emptyHeader(), /*enable_auto_progress=*/false)
        , state(std::move(state_))
        , query_status(std::move(query_status_))
        , pool(std::make_shared<GPU::GPUStagedBatchPool>())
        , stager(state->layout, pool)
    {
    }

    String getName() const override { return "GPUCompressedBlocks"; }

protected:
    Chunk generate() override
    {
        while (true)
        {
            if (auto batch = stager.takeSealed())
                return chunkOf(std::move(batch));

            if (!reader && !openNextPart())
                return {};

            if (!reader->readNext(*feed))
            {
                const size_t rows = reader->rowsRead();
                progress(rows, feed->bytes);
                stager.finishPart(rows);
                reader.reset();
                feed.reset();
            }
        }
    }

private:
    /// Hands what the reader reads of a part to the stager, as the layout numbers the columns.
    class Feed final : public MergeTreeCompressedColumnsReader::Consumer
    {
    public:
        using Stream = MergeTreeCompressedColumnsReader::Stream;

        Feed(GPUCompressedBlocksSource & source_, const IMergeTreeDataPart & part_) : source(source_), part(part_) { }

        /// Of each column read: its name, and the layout's column of its values and of its sizes.
        std::vector<String> names;
        std::vector<size_t> columns;
        std::vector<size_t> sizes_columns;
        /// Of the columns read, the bytes they expand to.
        size_t bytes = 0;

        void addCompressedBlock(
            size_t column,
            Stream stream,
            const CompressedReadBufferBase::CompressedBlock & block,
            size_t skipped,
            size_t kept,
            size_t remaining_bytes) override
        {
            codecOrThrow(block.method, block.payload, names[column] + (stream == Stream::Sizes ? ".size" : ""), part);
            source.stager.addCompressedBlock(
                layoutColumnOf(column, stream), block.method, block.payload, block.decompressed_bytes, remaining_bytes, skipped, kept);
            bytes += kept;
        }

        std::span<char> reserveBytes(size_t column, Stream stream, size_t max_bytes, size_t remaining_bytes) override
        {
            return source.stager.reserveRawBytes(layoutColumnOf(column, stream), max_bytes, remaining_bytes);
        }

        void commitBytes(size_t column, Stream stream, size_t committed) override
        {
            source.stager.commitRawBytes(layoutColumnOf(column, stream), committed);
            bytes += committed;
        }

        void checkCancelled() override
        {
            if (source.isCancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
            if (source.query_status)
                source.query_status->checkTimeLimit();
        }

    private:
        size_t layoutColumnOf(size_t column, Stream stream) const { return stream == Stream::Sizes ? sizes_columns[column] : columns[column]; }

        GPUCompressedBlocksSource & source;
        const IMergeTreeDataPart & part;
    };

    /// Opens the next part no source took yet, in the layout's order of columns: the keys, the values, then the columns
    /// of the `WHERE` that are not aliases. The rows of all the ranges of a part are read as the rows of one part.
    bool openNextPart()
    {
        const size_t part_index = state->next_part.fetch_add(1);
        if (part_index >= state->parts.size())
            return false;

        const PartToRead & part_to_read = state->parts[part_index];
        const IMergeTreeDataPart & part = *part_to_read.part;
        const GPU::GPUCompressedLayout & layout = *state->layout;

        feed = std::make_unique<Feed>(*this, part);
        std::vector<NameAndTypePair> columns;
        size_t column_index = 0;
        const auto add_column = [&](const NameAndTypePair & column)
        {
            columns.push_back(column);
            feed->names.push_back(column.name);
            feed->columns.push_back(column_index);
            feed->sizes_columns.push_back(isString(*column.type) ? layout.sizesColumnOf(column_index) : 0);
            ++column_index;
        };
        for (const auto & key : state->keys)
            add_column(key);
        for (const auto & column : state->columns)
            add_column(column.column);
        const std::vector<NameAndTypePair> & filter_columns = filterColumnsOf(*state);
        for (size_t i = 0; i < filter_columns.size(); ++i)
        {
            if (layout.isAliased(column_index))
                ++column_index;
            else
                add_column(filter_columns[i]);
        }

        reader = std::make_unique<MergeTreeCompressedColumnsReader>(
            part, columns, state->reader_settings, expandsOnDevice(state->decompression), part_to_read.ranges);
        return true;
    }

    /// The rows of the chunk are about the rows staged, at least one, so that the pipeline keeps the chunk that ends a
    /// part: the uploader tells how many rows a batch really has.
    Chunk chunkOf(std::unique_ptr<GPU::GPUStagedBatch> batch) const
    {
        const GPU::GPUCompressedLayout & layout = *state->layout;
        size_t rows = 0;
        for (size_t c = 0; c < layout.numColumns(); ++c)
            if (!layout.isAliased(c) && !layout.isVariable(c))
                rows = std::max(rows, batch->columns[c].kept_bytes / layout.elementBytesOf(c));

        Chunk chunk(Columns{}, std::max<size_t>(rows, 1));
        chunk.getChunkInfos().add(std::make_shared<StagedBatchInfo>(
            std::make_shared<StagedBatchHolder>(std::move(batch), pool)));
        return chunk;
    }

    const SharedStatePtr state;
    const QueryStatusPtr query_status;
    const std::shared_ptr<GPU::GPUStagedBatchPool> pool;
    GPU::GPUBlockStager stager;
    /// Of the part being read: the feed before the reader, which reads into it.
    std::unique_ptr<Feed> feed;
    std::unique_ptr<MergeTreeCompressedColumnsReader> reader;
};

/// Sends the staged blocks of one stream to the device and expands them there: a batch on the device per chunk.
class GPUUploadTransform : public ISimpleTransform
{
public:
    explicit GPUUploadTransform(const SharedState & state)
        : ISimpleTransform(emptyHeader(), emptyHeader(), /*skip_empty_chunks=*/true), uploader(state.layout)
    {
    }

    String getName() const override { return "GPUUpload"; }

protected:
    void transform(Chunk & chunk) override
    {
        auto info = infoOrThrow<StagedBatchInfo>(chunk, getName());
        std::optional<GPU::GPUDeviceBatch> batch = uploader.upload(*info->holder->batch);
        info.reset();

        chunk.clear();
        if (!batch)
            return;
        const size_t rows = batch->rows();
        chunk = Chunk(Columns{}, rows);
        chunk.getChunkInfos().add(std::make_shared<DeviceBatchInfo>(std::make_shared<GPU::GPUDeviceBatch>(std::move(*batch))));
    }

private:
    GPU::GPUBatchUploader uploader;
};

/// The groups of a lane after `finalize`, of `num_groups`, that the `HAVING` keeps, as a chunk of the aggregation's header:
/// the keys, then the result of each aggregate.
Chunk chunkOfGroups(const SharedState & state, const Block & header, GPU::GPUGroupingLane & lane, size_t num_groups)
{
    if (num_groups != 0 && state.group_filter)
        num_groups = lane.filterGroups(state.group_filter->expression, state.group_filter->columns);
    if (num_groups == 0)
        return {};

    if (header.columns() != state.keys.size() + state.aggregates.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU aggregation of {} keys and {} aggregates emits {} columns",
            state.keys.size(),
            state.aggregates.size(),
            header.columns());

    MutableColumns key_columns;
    key_columns.reserve(state.keys.size());
    for (size_t i = 0; i < state.keys.size(); ++i)
        key_columns.push_back(header.getByPosition(i).type->createColumn());

    MutableColumns aggregate_columns;
    aggregate_columns.reserve(state.aggregates.size());
    for (size_t i = 0; i < state.aggregates.size(); ++i)
        aggregate_columns.push_back(header.getByPosition(state.keys.size() + i).type->createColumn());

    lane.copyGroupsTo(key_columns, aggregate_columns);

    Columns result_columns;
    result_columns.reserve(header.columns());
    for (auto & column : key_columns)
        result_columns.push_back(std::move(column));
    for (auto & column : aggregate_columns)
        result_columns.push_back(std::move(column));

    return Chunk(std::move(result_columns), num_groups);
}

std::shared_ptr<GPU::GPUGroupingLane> makeLane(const SharedState & state, std::shared_ptr<GPU::GPUGroupingDecision> decision)
{
    return std::make_shared<GPU::GPUGroupingLane>(
        state.layout,
        groupByAggregatesOf(state.columns, state.derived, state.aggregates),
        derivedValuesOf(state.derived),
        state.filter ? std::optional(state.filter->expression) : std::nullopt,
        std::move(decision));
}

/// A lane: groups the batches it takes on a CUDA stream of its own, and then emits its groups, for the merge. Of a read
/// that collects rows, emits the rows its `WHERE` keeps instead, as they come.
class GPUGroupingTransform : public IAccumulatingTransform
{
public:
    GPUGroupingTransform(SharedStatePtr state_, SharedHeader output_header)
        : IAccumulatingTransform(emptyHeader(), std::move(output_header))
        , state(std::move(state_))
        , lane(makeLane(*state, state->decision))
    {
        if (state->merge_by_parts)
            lane->splitIntoParts(state->num_parts);
    }

    String getName() const override { return "GPUGrouping"; }

protected:
    void consume(Chunk chunk) override
    {
        auto info = infoOrThrow<DeviceBatchInfo>(chunk, getName());
        lane->add(std::move(*info->batch));

        if (state->collect && lane->collectedRows() >= collect_flush_rows)
            if (Chunk collected = takeCollected())
                setReadyChunk(std::move(collected));
    }

    Chunk generate() override
    {
        if (generated)
            return {};
        generated = true;

        if (state->collect)
            return takeCollected();

        if (state->merge_by_parts)
            lane->partition(state->num_parts);

        Chunk chunk(Columns{}, 1);
        chunk.getChunkInfos().add(std::make_shared<LaneGroupsInfo>(lane));
        return chunk;
    }

private:
    /// The rows collected so far, in the order of the read's header.
    Chunk takeCollected()
    {
        MutableColumns columns = lane->takeCollected(argumentTypesOf(state->columns));
        if (columns.empty())
            return {};

        const Block & header = getOutputPort().getHeader();
        if (header.columns() != state->columns.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "A GPU read of {} columns emits {} columns", state->columns.size(), header.columns());

        const size_t num_rows = columns.front()->size();
        Columns result_columns(header.columns());
        for (size_t i = 0; i < state->columns.size(); ++i)
            result_columns[header.getPositionByName(state->columns[i].column.name)] = std::move(columns[i]);
        return Chunk(std::move(result_columns), num_rows);
    }

    const SharedStatePtr state;
    const std::shared_ptr<GPU::GPUGroupingLane> lane;
    bool generated = false;
};

/// Merges the groups of the lanes, keeps those the `HAVING` keeps, and sends them to the host.
class GPUMergeGroupsTransform : public IAccumulatingTransform
{
public:
    GPUMergeGroupsTransform(SharedStatePtr state_, SharedHeader output_header)
        : IAccumulatingTransform(emptyHeader(), std::move(output_header)), state(std::move(state_))
    {
    }

    String getName() const override { return "GPUMergeGroups"; }

protected:
    void consume(Chunk chunk) override { lanes.push_back(infoOrThrow<LaneGroupsInfo>(chunk, getName())->lane); }

    Chunk generate() override
    {
        if (generated)
            return {};
        generated = true;

        if (lanes.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation without lanes");

        /// Into the lane with the most groups, grown once for the groups of all: one grown for each lane merged into it
        /// would insert the groups so far again each time.
        std::vector<size_t> groups(lanes.size(), 0);
        bool streaming = false;
        for (size_t i = 0; i < lanes.size(); ++i)
        {
            if (const auto lane_groups = lanes[i]->numGroups())
            {
                groups[i] = *lane_groups;
                streaming = true;
            }
        }
        const size_t largest = static_cast<size_t>(std::ranges::max_element(groups) - groups.begin());
        std::swap(lanes.front(), lanes[largest]);
        std::swap(groups.front(), groups[largest]);
        if (streaming && groups.front() != 0)
            lanes.front()->reserveGroups(std::accumulate(groups.begin(), groups.end(), size_t{0}));

        GPU::GPUGroupingLane & first = *lanes.front();
        for (size_t i = 1; i < lanes.size(); ++i)
            first.absorb(*lanes[i]);
        lanes.resize(1);

        const size_t num_groups = first.finalize();

        /// No row came to the device, and an aggregation without keys emits its results over none, which the `HAVING`
        /// above evaluates.
        if (num_groups == 0 && state->result_over_no_rows)
            return Chunk(*state->result_over_no_rows, 1);

        return chunkOfGroups(*state, getOutputPort().getHeader(), first, num_groups);
    }

private:
    const SharedStatePtr state;
    std::vector<std::shared_ptr<GPU::GPUGroupingLane>> lanes;
    bool generated = false;
};

struct LanePartInfo : public ChunkInfoCloneable<LanePartInfo>
{
    LanePartInfo(std::shared_ptr<const std::vector<std::shared_ptr<GPU::GPUGroupingLane>>> lanes_, size_t part_)
        : lanes(std::move(lanes_)), part(part_)
    {
    }

    std::shared_ptr<const std::vector<std::shared_ptr<GPU::GPUGroupingLane>>> lanes;
    size_t part;
};

/// Takes the lanes, each of its groups split into parts by a hash of the keys, and emits a chunk per part, for the merges
/// of the parts, which run each on its own.
class GPUSplitLanesTransform : public IAccumulatingTransform
{
public:
    explicit GPUSplitLanesTransform(SharedStatePtr state_) : IAccumulatingTransform(emptyHeader(), emptyHeader()), state(std::move(state_)) { }

    String getName() const override { return "GPUSplitLanes"; }

protected:
    void consume(Chunk chunk) override { lanes.push_back(infoOrThrow<LaneGroupsInfo>(chunk, getName())->lane); }

    Chunk generate() override
    {
        if (!all_lanes)
            all_lanes = std::make_shared<const std::vector<std::shared_ptr<GPU::GPUGroupingLane>>>(std::move(lanes));
        if (next_part == state->num_parts)
            return {};

        Chunk chunk(Columns{}, 1);
        chunk.getChunkInfos().add(std::make_shared<LanePartInfo>(all_lanes, next_part++));
        return chunk;
    }

private:
    const SharedStatePtr state;
    std::vector<std::shared_ptr<GPU::GPUGroupingLane>> lanes;
    std::shared_ptr<const std::vector<std::shared_ptr<GPU::GPUGroupingLane>>> all_lanes;
    size_t next_part = 0;
};

/// Merges one part of the groups of every lane, on a CUDA stream of its own, keeps those the `HAVING` keeps, and sends
/// them to the host. The keys of different parts differ, so the parts are the groups of the aggregation.
class GPUMergePartTransform : public ISimpleTransform
{
public:
    GPUMergePartTransform(SharedStatePtr state_, SharedHeader output_header)
        : ISimpleTransform(emptyHeader(), std::move(output_header), /*skip_empty_chunks=*/true), state(std::move(state_))
    {
    }

    String getName() const override { return "GPUMergePart"; }

protected:
    void transform(Chunk & chunk) override
    {
        const auto info = infoOrThrow<LanePartInfo>(chunk, getName());
        chunk.clear();

        const auto merged = makeLane(*state, std::make_shared<GPU::GPUGroupingDecision>());
        for (const auto & lane : *info->lanes)
            merged->absorbPart(*lane, info->part);
        const size_t num_groups = merged->finalize();
        chunk = chunkOfGroups(*state, getOutputPort().getHeader(), *merged, num_groups);
    }

private:
    const SharedStatePtr state;
};

}

ReadFromGPUCompressedColumns::ReadFromGPUCompressedColumns(
    SharedHeader output_header_,
    std::vector<NameAndTypePair> keys_,
    std::vector<ColumnToReduce> columns_,
    std::vector<DerivedColumn> derived_,
    std::vector<AggregateToCompute> aggregates_,
    std::optional<Columns> result_over_no_rows_,
    std::optional<DeviceFilter> filter_,
    std::vector<PartToRead> parts_,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr context_,
    size_t batch_bytes_,
    size_t num_readers_,
    GPUDecompressionMode decompression_,
    bool collect_)
    : ISourceStep(std::move(output_header_))
    , keys(std::move(keys_))
    , columns(std::move(columns_))
    , derived(std::move(derived_))
    , aggregates(std::move(aggregates_))
    , result_over_no_rows(std::move(result_over_no_rows_))
    , filter(std::move(filter_))
    , parts(std::move(parts_))
    , storage_snapshot(std::move(storage_snapshot_))
    , context(std::move(context_))
    , batch_bytes(batch_bytes_)
    , num_readers(num_readers_)
    , decompression(decompression_)
    , collect(collect_)
{
    if (collect && (!keys.empty() || !aggregates.empty() || !derived.empty() || columns.empty()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU read that collects rows of {} columns with keys or aggregates", columns.size());

    if (!collect && aggregates.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A GPU aggregation over compressed columns of {} keys and no aggregates", keys.size());

    if (result_over_no_rows && (!keys.empty() || result_over_no_rows->size() != aggregates.size()))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "A GPU aggregation over compressed columns of {} keys and {} aggregates with {} results over no rows",
            keys.size(),
            aggregates.size(),
            result_over_no_rows->size());
}

void ReadFromGPUCompressedColumns::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto state = std::make_shared<SharedState>();
    state->keys = keys;
    state->columns = columns;
    state->derived = derived;
    state->aggregates = aggregates;
    state->result_over_no_rows = result_over_no_rows;
    state->filter = filter;
    state->group_filter = group_filter;
    state->parts = std::move(parts);
    state->storage_snapshot = storage_snapshot;
    state->context = context;
    state->reader_settings = MergeTreeReaderSettings::createFromContext(context);
    state->decompression = decompression;
    state->batch_bytes = batch_bytes;
    state->num_readers = num_readers != 0 ? num_readers : automaticReaders(*state);
    state->collect = collect;
    if (filter)
    {
        for (const NameAndTypePair & column : filter->columns)
        {
            std::optional<uint32_t> alias;
            for (size_t i = 0; i < keys.size() && !alias; ++i)
                if (keys[i].name == column.name && keys[i].type->equals(*column.type))
                    alias = static_cast<uint32_t>(i);
            for (size_t i = 0; i < columns.size() && !alias; ++i)
                if (columns[i].column.name == column.name && columns[i].column.type->equals(*column.type))
                    alias = static_cast<uint32_t>(keys.size() + i);
            state->filter_aliases.push_back(alias);
        }
    }

    /// A read of columns of a fixed width reads a large part in pieces, a reader each.
    if (readsFixedWidthOnly(*state))
        splitPartsAmongReaders(state->parts, 2 * state->num_readers);

    const DataTypes filter_types = typesOf(filterColumnsOf(*state));
    state->layout = std::make_shared<const GPU::GPUCompressedLayout>(
        typesOf(keys), argumentTypesOf(columns), collect, filter_types, state->filter_aliases, batch_bytes);
    state->decision = std::make_shared<GPU::GPUGroupingDecision>();

    const size_t num_sources = std::max<size_t>(1, std::min(state->num_readers, state->parts.size()));
    const size_t num_lanes = std::min(num_sources, max_lanes);

    /// With keys, the lanes merge by parts of their keys, as many as there are lanes, unless an `avg` is among the
    /// aggregates, whose partial results do not combine: then into one lane.
    state->merge_by_parts = !collect && !keys.empty()
        && std::ranges::none_of(aggregates, [](const AggregateToCompute & aggregate) { return aggregate.aggregation == GPU::GPUAggregationKind::Mean; });
    state->num_parts = num_lanes;

    Pipes sources;
    sources.reserve(num_sources);
    for (size_t i = 0; i < num_sources; ++i)
        sources.emplace_back(std::make_shared<GPUCompressedBlocksSource>(state, settings.process_list_element));
    Pipe pipe = Pipe::unitePipes(std::move(sources));

    pipe.addSimpleTransform([&](const SharedHeader &) { return std::make_shared<GPUUploadTransform>(*state); });
    pipe.resize(num_lanes);
    const SharedHeader lane_header = collect ? getOutputHeader() : emptyHeader();
    pipe.addSimpleTransform([&](const SharedHeader &) { return std::make_shared<GPUGroupingTransform>(state, lane_header); });
    if (state->merge_by_parts)
    {
        pipe.resize(1);
        pipe.addSimpleTransform([&](const SharedHeader &) { return std::make_shared<GPUSplitLanesTransform>(state); });
        pipe.resize(state->num_parts);
        pipe.addSimpleTransform([&](const SharedHeader &) { return std::make_shared<GPUMergePartTransform>(state, getOutputHeader()); });
    }
    else if (!collect)
    {
        pipe.resize(1);
        pipe.addSimpleTransform([&](const SharedHeader &) { return std::make_shared<GPUMergeGroupsTransform>(state, getOutputHeader()); });
    }

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

void ReadFromGPUCompressedColumns::describeActions(FormatSettings & format_settings) const
{
    format_settings.out << format_settings.detail_prefix << "Parts: " << parts.size() << "\n";
    if (collect)
        format_settings.out << format_settings.detail_prefix << "Rows the filter keeps sent back\n";
    if (const size_t ranged = std::ranges::count_if(parts, [](const auto & part) { return !part.ranges.empty(); }))
        format_settings.out << format_settings.detail_prefix << "Parts read in ranges of marks: " << ranged << "\n";
    if (!keys.empty())
    {
        format_settings.out << format_settings.detail_prefix << "Keys: ";
        for (size_t i = 0; i < keys.size(); ++i)
            format_settings.out << (i == 0 ? "" : ", ") << keys[i].name;
        format_settings.out << "\n";
    }
    format_settings.out << format_settings.detail_prefix << "Columns: ";
    for (size_t i = 0; i < columns.size(); ++i)
        format_settings.out << (i == 0 ? "" : ", ") << columns[i].column.name;
    format_settings.out << "\n";
    if (!derived.empty())
    {
        format_settings.out << format_settings.detail_prefix << "Computed on the device: ";
        for (size_t i = 0; i < derived.size(); ++i)
            format_settings.out << (i == 0 ? "" : ", ") << derived[i].description;
        format_settings.out << "\n";
    }
    if (!aggregates.empty())
    {
        format_settings.out << format_settings.detail_prefix << "Aggregates: ";
        for (size_t i = 0; i < aggregates.size(); ++i)
            format_settings.out << (i == 0 ? "" : ", ") << aggregates[i].description;
        format_settings.out << "\n";
    }
    if (group_filter)
        format_settings.out << format_settings.detail_prefix << "Filter of the groups on the device: " << group_filter->description << "\n";

    if (filter)
    {
        format_settings.out << format_settings.detail_prefix << "Filter on the device: " << filter->description << "\n";
        format_settings.out << format_settings.detail_prefix << "Filter columns: ";
        for (size_t i = 0; i < filter->columns.size(); ++i)
            format_settings.out << (i == 0 ? "" : ", ") << filter->columns[i].name;
        format_settings.out << "\n";
    }
}

void ReadFromGPUCompressedColumns::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Parts", parts.size());
    map.add("Collected", collect);
    map.add("Parts Read In Ranges", std::ranges::count_if(parts, [](const auto & part) { return !part.ranges.empty(); }));

    if (!keys.empty())
    {
        auto keys_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & key : keys)
            keys_array->add(key.name);
        map.add("Keys", std::move(keys_array));
    }

    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column.column.name);
    map.add("Columns", std::move(columns_array));

    if (!derived.empty())
    {
        auto derived_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & column : derived)
            derived_array->add(column.description);
        map.add("Computed on the device", std::move(derived_array));
    }

    if (!aggregates.empty())
    {
        auto aggregates_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & aggregate : aggregates)
            aggregates_array->add(aggregate.description);
        map.add("Aggregates", std::move(aggregates_array));
    }

    if (group_filter)
        map.add("Filter of the groups on the device", group_filter->description);

    if (filter)
    {
        map.add("Filter on the device", filter->description);

        auto filter_columns_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & column : filter->columns)
            filter_columns_array->add(column.name);
        map.add("Filter columns", std::move(filter_columns_array));
    }
}

}

#endif
