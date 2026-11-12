#include <Processors/Port.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/TextIndexUtils.h>
#include <Compression/CompressionFactory.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ParallelSyncFiles.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>
#include <IO/ReadBufferFromMemory.h>

#include <algorithm>
#include <array>
#include <bit>
#include <deque>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <utility>

namespace ProfileEvents
{
    extern const Event TextIndexTemporarySegmentsWritten;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int FILE_DOESNT_EXIST;
    extern const int INCORRECT_DATA;
    extern const int SUPPORT_IS_DISABLED;
    extern const int CORRUPTED_DATA;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsMilliseconds background_task_preferred_step_execution_time_ms;
    extern const MergeTreeSettingsNonZeroUInt64 text_index_max_memory_usage_before_flush;
    extern const MergeTreeSettingsNonZeroUInt64 text_index_max_processed_tokens_before_flush;
}

namespace
{

Int64 getCurrentThreadMemoryUsage()
{
    const auto & thread = CurrentThread::get();
    return thread.memory_tracker.get() + thread.untracked_memory.load();
}

CompressionCodecPtr makeMarksCompressionCodec(const String & marks_compression_codec)
{
    return CompressionCodecFactory::instance().get(marks_compression_codec);
}

std::pair<MergeTreeIndexOutputStreams, std::vector<std::unique_ptr<MergeTreeIndexWriterStream>>>
makeOutputStreams(
    const MergeTreeIndexSubstreams & index_substreams,
    const String & index_name,
    const MutableDataPartStoragePtr & data_part_storage,
    const CompressionCodecPtr & default_codec,
    const String & marks_file_extension,
    const MergeTreeWriterSettings & settings)
{
    auto marks_compression_codec = makeMarksCompressionCodec(settings.marks_compression_codec);
    MergeTreeIndexOutputStreams streams;
    std::vector<std::unique_ptr<MergeTreeIndexWriterStream>> streams_holders;

    for (const auto & index_substream : index_substreams)
    {
        auto stream_name = index_name + index_substream.suffix;

        auto stream = std::make_unique<MergeTreeIndexWriterStream>(
            stream_name,
            data_part_storage,
            stream_name,
            index_substream.extension,
            stream_name,
            marks_file_extension,
            default_codec,
            settings.max_compress_block_size,
            marks_compression_codec,
            settings.marks_compress_block_size,
            settings.query_write_settings);

        streams[index_substream.type] = stream.get();
        streams_holders.push_back(std::move(stream));
    }

    return {std::move(streams), std::move(streams_holders)};
}

void writeMarks(MergeTreeIndexOutputStreams & streams, bool can_use_adaptive_granularity)
{
    for (const auto & [_, stream] : streams)
    {
        auto & marks_out = stream->compress_marks ? stream->marks_compressed_hashing : stream->marks_hashing;

        writeBinaryLittleEndian(stream->plain_hashing.count(), marks_out);
        writeBinaryLittleEndian(stream->compressed_hashing.offset(), marks_out);
        if (can_use_adaptive_granularity)
            writeBinaryLittleEndian(1UL, marks_out);
    }
}

}

BuildTextIndexTransform::BuildTextIndexTransform(
    SharedHeader header,
    String index_file_prefix_,
    std::vector<MergeTreeIndexPtr> indexes_,
    MutableDataPartStoragePtr temporary_storage_,
    MergeTreeWriterSettings writer_settings_,
    CompressionCodecPtr default_codec_,
    String marks_file_extension_,
    const MergeTreeSettings & storage_settings)
    : ISimpleTransform(header, header, false)
    , index_file_prefix(std::move(index_file_prefix_))
    , indexes(std::move(indexes_))
    , temporary_storage(std::move(temporary_storage_))
    , writer_settings(std::move(writer_settings_))
    , default_codec(std::move(default_codec_))
    , marks_file_extension(std::move(marks_file_extension_))
    , segment_numbers(indexes.size(), 0)
    , estimated_allocated_bytes(indexes.size(), 0)
    , max_processed_tokens(storage_settings[MergeTreeSetting::text_index_max_processed_tokens_before_flush])
    , max_allocated_bytes(storage_settings[MergeTreeSetting::text_index_max_memory_usage_before_flush])
{

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        auto aggregator = indexes[i]->createIndexAggregator();
        aggregators.push_back(std::move(aggregator));
        index_position_by_name.emplace(indexes[i]->index.name, i);
    }
}

void BuildTextIndexTransform::transform(Chunk & chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
    aggregate(block);
}

IProcessor::Status BuildTextIndexTransform::prepare()
{
    auto status = ISimpleTransform::prepare();
    if (status == Status::Finished)
        finalize();
    return status;
}

void BuildTextIndexTransform::aggregate(const Block & block)
{
    if (block.rows() == 0 || abandoned)
        return;

    if (!max_deferred_rows)
    {
        aggregateBlock(block);
        return;
    }

    Block indexed_columns;
    for (const auto & index : indexes)
    {
        for (const auto & column_name : index->index.column_names)
        {
            if (!indexed_columns.has(column_name))
                indexed_columns.insert(block.getByName(column_name));
        }
    }

    deferred_rows += block.rows();
    deferred_bytes += indexed_columns.bytes();

    if (deferred_rows > *max_deferred_rows || deferred_bytes > max_allocated_bytes)
    {
        abandoned = true;
        deferred_blocks = {};
        return;
    }

    deferred_blocks.push_back(std::move(indexed_columns));
}

void BuildTextIndexTransform::aggregateBlock(const Block & block)
{
    num_processed_rows += block.rows();

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        size_t pos = 0;
        auto & aggregator_text = typeid_cast<MergeTreeIndexAggregatorText &>(*aggregators[i]);
        const auto memory_usage_before_update = getCurrentThreadMemoryUsage();
        aggregator_text.update(block, &pos, block.rows());
        const auto memory_usage_after_update = getCurrentThreadMemoryUsage();

        if (memory_usage_after_update > memory_usage_before_update)
            estimated_allocated_bytes[i] += static_cast<size_t>(memory_usage_after_update - memory_usage_before_update);

        if (aggregator_text.getNumProcessedTokens() > max_processed_tokens || estimated_allocated_bytes[i] > max_allocated_bytes)
            writeTemporarySegment(i);
    }
}

void BuildTextIndexTransform::finalize()
{
    /// A deferred build without rows is abandoned too: the source indexes of a merge that keeps no rows are skipped anyway.
    if (max_deferred_rows && deferred_rows == 0)
        abandoned = true;

    if (abandoned)
        return;

    auto blocks = std::exchange(deferred_blocks, {});
    for (auto & block : blocks)
        aggregateBlock(std::exchange(block, {}));

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        if (!aggregators[i]->empty())
            writeTemporarySegment(i);
    }
}

std::vector<TextIndexSegment> BuildTextIndexTransform::getSegments(const String & index_name, size_t part_idx) const
{
    if (abandoned)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index {} has no segments because its build was abandoned", index_name);

    auto it = index_position_by_name.find(index_name);
    if (it == index_position_by_name.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index {} not found in BuildTextIndexTransform", index_name);

    size_t index_idx = it->second;
    std::vector<TextIndexSegment> segments;

    for (size_t i = 0; i < segment_numbers[index_idx]; ++i)
    {
        auto index_file_name = fmt::format("{}_{}_{}", index_file_prefix, i, indexes[index_idx]->getFileName());
        segments.emplace_back(temporary_storage, std::move(index_file_name), part_idx);
    }

    return segments;
}

void BuildTextIndexTransform::writeTemporarySegment(size_t i)
{
    auto index_file_name = fmt::format("{}_{}_{}", index_file_prefix, segment_numbers[i]++, indexes[i]->getFileName());
    auto index_substreams = indexes[i]->getSubstreams();

    auto & aggregator_text = typeid_cast<MergeTreeIndexAggregatorText &>(*aggregators[i]);
    auto granule = aggregator_text.getGranuleAndReset();
    estimated_allocated_bytes[i] = 0;
    aggregator_text.setCurrentRow(num_processed_rows);

    auto [streams, streams_holders] = makeOutputStreams(
        index_substreams,
        index_file_name,
        temporary_storage,
        default_codec,
        marks_file_extension,
        writer_settings);

    writeMarks(streams, writer_settings.can_use_adaptive_granularity);
    granule->serializeBinaryWithMultipleStreams(streams);

    for (auto & stream : streams_holders)
        stream->finalize();

    ProfileEvents::increment(ProfileEvents::TextIndexTemporarySegmentsWritten);
}

static PostingsSerialization createPostingsSerialization(const IMergeTreeIndex & index)
{
    const auto & text_index = typeid_cast<const MergeTreeIndexText &>(index);
    const auto * codec = text_index.getPostingListCodec();
    auto codec_type = codec ? codec->getType() : IPostingListCodec::Type::None;
    auto codec_copy = PostingListCodecFactory::createPostingListCodec(codec_type);

    return PostingsSerialization(std::move(codec_copy), text_index.getParams().serialization_version);
}

static ALWAYS_INLINE UInt32 checkMergedRowId(UInt64 new_offset)
{
    if (new_offset > std::numeric_limits<UInt32>::max())
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot merge text index: remapped row id {} exceeds the maximum supported row id {}",
            new_offset, std::numeric_limits<UInt32>::max());
    }

    return static_cast<UInt32>(new_offset);
}

static ALWAYS_INLINE UInt32 adjustPartOffset(const MergedPartOffsets & merged_part_offsets, size_t part_index, UInt32 row_id)
{
    return checkMergedRowId(merged_part_offsets[part_index, row_id]);
}

struct MergeTextIndexesTask::PostingsMergeCursor
{
    const TokenSource * source = nullptr;
    size_t source_idx = 0;
    /// Position of the current row id in the row_ids array.
    size_t pos = 0;
    /// Next entry of info.offsets to decode.
    size_t next_segment = 0;
    /// Number of the source's row ids decoded so far, including the ones of rows dropped by the merge.
    /// It is the rank of the next decoded row id in the source posting list.
    size_t num_decoded = 0;
    /// Decoded and remapped row ids of the current segment, or the embedded postings.
    PaddedPODArray<UInt32> row_ids;

    UInt32 current() const { return row_ids[pos]; }
    bool isValid() const { return pos < row_ids.size(); }
    std::span<const UInt32> remaining() const { return {row_ids.data() + pos, row_ids.size() - pos}; }
};

/// Merges the row ids of several postings cursors in the globally sorted order.
///
/// Sources own disjoint row sets but may interleave arbitrarily.
/// The row ids are united through a bitset over aligned windows of WINDOW_ROWS rows:
/// 1. A window starts at the smallest head of the cursors.
/// 2. If the second smallest head is beyond the window, the smallest source is alone in it
///    and its run below the second head is passed to the sink without touching the bitset.
/// 3. Otherwise every cursor sets a bit for each of its row ids inside the window, refilling its segment on the way.
///
/// The cost per row id is a bit set and a bit scan, independent of the interleaving and the number of sources.
/// Row ids are passed to the sink in chunks that are multiples of the posting list encoder granularity,
/// buffered only where a run or a window does not align.
class MergeTextIndexesTask::PostingsMergeQueue
{
public:
    PostingsMergeQueue(MergeTextIndexesTask & task_, size_t max_sources) : task(task_), cursors(max_sources)
    {
        active_cursors.reserve(max_sources);
    }

    void push(const TokenSource & source);
    bool isValid() const { return !active_cursors.empty(); }

    /// Passes the row ids of the active cursors to the sink in the globally sorted order.
    /// Aligned runs of a single source are passed through without copying, the rest is buffered.
    /// If `positions_` is set, it receives the source of every row id in the same order.
    template <typename Sink> void merge(Sink && sink, PositionsMerge * positions_);

private:
    static constexpr size_t WINDOW_ROWS = 4096;
    static constexpr size_t ROWS_TO_BUFFER = 8 * WINDOW_ROWS;

    struct Window
    {
        /// Position of the cursor with the smallest head.
        UInt64 min_pos;
        /// The smallest head among the other cursors.
        UInt64 second_head;
        /// Window bounds.
        UInt64 begin;
        UInt64 end;

        /// Only the smallest source has row ids in the window.
        bool hasOneSource() const { return second_head >= end; }
    };

    /// Selects the window of the smallest head from active cursors.
    Window selectWindow() const;

    /// Sets a bit in window_bits for every row id of every cursor inside the window.
    /// Returns the number of consumed row ids and the mask of non-empty words of window_bits.
    std::pair<UInt64, UInt64> consumeWindow(Window window);

    /// Extracts row ids with set bits inside the window and appends them to the buffer.
    void processWindow(Window window);

    /// Flushes the run of the smallest source below the second head to the sink.
    template <typename Sink>
    void flushRun(Window window, Sink && sink);

    /// Flushes the row ids to the sink directly if they are aligned with append_granularity, otherwise buffers them.
    template <typename Sink>
    void flushDirect(std::span<const UInt32> row_ids, Sink && sink);

    /// Flushes the aligned prefix of the buffered row ids to the sink once enough are accumulated, or all of them at the end.
    template <typename Sink>
    void flushBuffered(bool is_final, Sink && sink);

    /// Loads the next segment of the exhausted cursor or drops it.
    void refill(size_t pos);

    MergeTextIndexesTask & task;
    /// Reusable cursors, one per source.
    std::vector<PostingsMergeCursor> cursors;
    /// Cursors of the current token that still have row ids to merge.
    std::vector<PostingsMergeCursor *> active_cursors;
    /// Bitset of the current window.
    std::array<UInt64, WINDOW_ROWS / 64> window_bits{};
    /// Source of each set bit, allocated and tracked only with positions.
    std::vector<UInt32> window_sources;
    PositionsMerge * positions = nullptr;
    /// Row ids buffered for the sink.
    PaddedPODArray<UInt32> buffer;
};

class MergeTextIndexesTask::PositionsMerge
{
public:
    explicit PositionsMerge(MergeTextIndexesTask & task_) : task(task_) {}

    /// Opens the positions of the current token's sources.
    void start();
    /// Whether the positions of the current token are merged.
    bool isActive() const { return encoder.has_value(); }
    /// Marks the position list with the given rank in the source as belonging to a row dropped by the merge.
    /// Ranks of a source are added in increasing order, before the position lists around them are appended.
    void addDroppedRank(size_t source_idx, size_t rank);
    /// Appends the next `count` position lists of the source, skipping the ones of dropped rows.
    void append(size_t source_idx, size_t count);
    /// Writes the merged position lists; every source must be consumed.
    void finish(TokenPostingsInfo & token_info);

private:
    struct Reader
    {
        ReadBuffer * in = nullptr;
        std::unique_ptr<ReadBufferFromMemory> embedded_in;
        TextIndexBlockedPositionsCodec::Directory directory;
        size_t next_rank = 0;
        /// Decoded block of `next_rank`, split by `offsets`.
        PaddedPODArray<UInt32> offsets;
        PaddedPODArray<UInt32> positions;
        /// Runs [begin, end) of ranks of the position lists of rows dropped by the merge that are not skipped yet.
        std::deque<std::pair<UInt32, UInt32>> dropped_runs;
    };

    MergeTextIndexesTask & task;
    std::vector<Reader> readers;
    /// Set only while a token with positions is merged.
    std::optional<TextIndexBlockedPositionsCodec::Encoder> encoder;
    TextIndexBlockedPositionsCodec::DecodeScratch scratch;
    std::vector<UInt32> block_ranks;
};

void MergeTextIndexesTask::PositionsMerge::start()
{
    const auto & sources = task.current_token_sources;
    readers.resize(sources.size());
    encoder.emplace();

    for (size_t i = 0; i < sources.size(); ++i)
    {
        auto & reader = readers[i];
        reader.in = nullptr;
        reader.next_rank = 0;
        reader.dropped_runs.clear();

        const auto & info = sources[i].info;
        if (info.cardinality == 0)
            continue;

        if (info.header & PostingsSerialization::Flags::EmbeddedPositions)
        {
            reader.embedded_in = std::make_unique<ReadBufferFromMemory>(info.embedded_positions.data(), info.position_bytes);
            reader.directory = TextIndexBlockedPositionsCodec::readDirectory(*reader.embedded_in, 0, info.cardinality, info.position_bytes);
            reader.in = reader.embedded_in.get();
            continue;
        }

        auto * stream = task.input_streams[sources[i].source_num].at(MergeTreeIndexSubstream::Type::TextIndexPositions);
        auto * data_buffer = stream->getDataBuffer();

        validateTokenPositions(info, stream->getFileSize());
        stream->seekToMark({info.position_offset, 0});
        reader.directory = TextIndexBlockedPositionsCodec::readDirectory(*data_buffer, info.position_offset, info.cardinality, info.position_bytes);
        reader.in = data_buffer;
    }
}

void MergeTextIndexesTask::PositionsMerge::addDroppedRank(size_t source_idx, size_t rank)
{
    auto & runs = readers[source_idx].dropped_runs;
    chassert(runs.empty() || runs.back().second <= rank);

    if (!runs.empty() && runs.back().second == rank)
        ++runs.back().second;
    else
        runs.emplace_back(static_cast<UInt32>(rank), static_cast<UInt32>(rank + 1));
}

void MergeTextIndexesTask::PositionsMerge::append(size_t source_idx, size_t count)
{
    constexpr size_t block_docs = TextIndexBlockedPositionsCodec::BLOCK_DOCS;
    auto & reader = readers[source_idx];

    if (!reader.in || reader.next_rank + count > reader.directory.num_docs)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: more posting documents than position lists ({})", reader.directory.num_docs);

    for (; count > 0; ++reader.next_rank)
    {
        /// The position lists of dropped rows are skipped, so the check above does not cover them.
        if (reader.next_rank >= reader.directory.num_docs)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: more posting documents than position lists ({})", reader.directory.num_docs);

        const size_t local_rank = reader.next_rank % block_docs;

        /// Blocks follow the directory back to back, so the stream is at the next block.
        if (local_rank == 0)
        {
            const size_t block_idx = reader.next_rank / block_docs;
            block_ranks.resize(reader.directory.docsInBlock(block_idx));
            std::iota(block_ranks.begin(), block_ranks.end(), UInt32{0});

            reader.offsets.resize(1);
            reader.offsets[0] = 0;
            reader.positions.clear();
            TextIndexBlockedPositionsCodec::decodeBlock(
                *reader.in, reader.directory, block_idx, block_ranks, reader.offsets, reader.positions, scratch);
        }

        /// The row of this position list was dropped by the merge.
        if (!reader.dropped_runs.empty() && reader.dropped_runs.front().first == reader.next_rank)
        {
            auto & run = reader.dropped_runs.front();
            if (++run.first == run.second)
                reader.dropped_runs.pop_front();
            continue;
        }

        const UInt32 begin = reader.offsets[local_rank];
        encoder->addDocument(std::span<const UInt32>(reader.positions.data() + begin, reader.offsets[local_rank + 1] - begin));
        --count;
    }
}

void MergeTextIndexesTask::PositionsMerge::finish(TokenPostingsInfo & token_info)
{
    if (!encoder)
        return;

    for (const auto & reader : readers)
    {
        /// The position lists of rows dropped after the last merged one are not read.
        size_t num_docs_consumed = reader.next_rank;
        for (const auto & [begin, end] : reader.dropped_runs)
            num_docs_consumed += end - begin;

        if (reader.in && num_docs_consumed != reader.directory.num_docs)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: {} posting documents but {} position lists", num_docs_consumed, reader.directory.num_docs);
    }

    if (encoder->numDocuments() == 0)
    {
        encoder.reset();
        return;
    }

    auto * positions_stream = task.output_streams.at(MergeTreeIndexSubstream::Type::TextIndexPositions);
    TextIndexSerialization::serializePositions(token_info, positions_stream->plain_hashing, [&](WriteBuffer & out) { encoder->finalize(out); });
    encoder.reset();
}

MergeTextIndexesTask::MergeTextIndexesTask(
    std::vector<TextIndexSegment> segments_,
    MergeTreeMutableDataPartPtr new_data_part_,
    size_t num_rows_,
    MergeTreeIndexPtr index_ptr_,
    std::shared_ptr<MergedPartOffsets> merged_part_offsets_,
    const MergeTreeReaderSettings & reader_settings_,
    const MergeTreeWriterSettings & writer_settings_,
    bool need_fsync_)
    : segments(std::move(segments_))
    , new_data_part(std::move(new_data_part_))
    , num_rows(num_rows_)
    , index_ptr(std::move(index_ptr_))
    , merged_part_offsets(std::move(merged_part_offsets_))
    , writer_settings(writer_settings_)
    , need_fsync(need_fsync_)
    , step_time_ms((*new_data_part->storage.getSettings())[MergeTreeSetting::background_task_preferred_step_execution_time_ms].totalMilliseconds())
    , postings_queue(std::make_unique<PostingsMergeQueue>(*this, segments.size()))
    , positions_merge(std::make_unique<PositionsMerge>(*this))
    , postings_serialization(createPostingsSerialization(*index_ptr))
{
    tokens_cursors.resize(segments.size());
    inputs.resize(segments.size());
    input_streams.resize(segments.size());
    output_tokens = ColumnString::create();

    const auto & text_index = typeid_cast<const MergeTreeIndexText &>(*index_ptr);
    params = text_index.getParams();
    sparse_index_tokens = ColumnString::create();
    sparse_index_offsets = ColumnUInt64::create();

    /// Registered here rather than inside `makeOutputStreams`: the other caller writes temporary
    /// segments into a different directory and must claim nothing in this part.
    if (writer_settings.stream_base_manifest)
    {
        const String index_file_name = index_ptr->getFileName();
        for (const auto & substream : index_ptr->getSubstreams())
            writer_settings.stream_base_manifest->registerStreamBase(
                index_file_name + substream.suffix,
                {StreamBaseManifest::Kind::SkipIndex, index_ptr->index.name});
    }

    std::tie(output_streams, output_streams_holders) = makeOutputStreams(
        index_ptr->getSubstreams(),
        index_ptr->getFileName(),
        new_data_part->getDataPartStoragePtr(),
        new_data_part->default_codec,
        new_data_part->getMarksFileExtension(),
        writer_settings);

    auto substreams = index_ptr->getSubstreams();

    for (size_t i = 0; i < segments.size(); ++i)
    {
        for (const auto & substream : substreams)
        {
            /// The merge reads the dictionary and the postings of a source part sequentially,
            /// so their streams take the regular read buffer size and keep the page cache lookahead.
            auto stream = makeTextIndexInputStream(
                segments[i].part_storage,
                segments[i].index_file_name,
                substream,
                reader_settings_,
                /*expected_buffer_size=*/ std::nullopt);

            input_streams[i][substream.type] = stream.get();
            input_streams_holders.emplace_back(std::move(stream));
        }
    }

    /// Resolve each source part's codecs (postings + positions) from its own header.
    source_postings_serializations.reserve(segments.size());

    for (size_t i = 0; i < segments.size(); ++i)
    {
        auto * stream = input_streams[i].at(MergeTreeIndexSubstream::Type::Regular);
        stream->seekToStart();
        /// Only the version and codecs are needed here, so skip deserializing the sparse index.
        auto header = TextIndexSerialization::deserializeHeaderPrefix(*stream->getDataBuffer());
        source_postings_serializations.emplace_back(PostingListCodecFactory::createPostingListCodec(header.codec_type), header.version);
    }
}

MergeTextIndexesTask::~MergeTextIndexesTask() noexcept
{
    cancelImpl();
}

Block MergeTextIndexesTask::getHeader() const
{
    return Block{ColumnWithTypeAndName{ColumnString::create(), std::make_shared<DataTypeString>(), "token"}};
}

void MergeTextIndexesTask::initializeTokensQueue()
{
    SortDescription description;
    description.emplace_back("token");

    for (size_t source_num = 0; source_num < inputs.size(); ++source_num)
    {
        tokens_cursors[source_num] = SortCursorImpl(getHeader(), description, source_num);
        readDictionaryBlock(source_num);
    }
}

/// Whether all row ids of the token belong to rows dropped by the merge, decided from the embedded row ids
/// or from the row ranges of the posting list segments, without decoding a posting list.
/// A row id outside of the part is left to the checks of the merge, which report the corruption.
static bool isTokenOfDroppedRows(const TokenPostingsInfo & info, const MergedPartOffsets & merged_part_offsets, size_t part_index)
{
    if (info.cardinality == 0)
        return false;

    const size_t part_rows = merged_part_offsets.getPartRowsCount(part_index);

    if (info.header & PostingsSerialization::Flags::EmbeddedPostings)
    {
        if (info.embedded_postings.empty())
            return false;

        for (UInt32 row_id : info.embedded_postings)
        {
            if (row_id >= part_rows || merged_part_offsets.tryGetNewOffset(part_index, row_id))
                return false;
        }

        return true;
    }

    if (info.ranges.empty())
        return false;

    for (const auto & range : info.ranges)
    {
        if (range.begin > range.end || range.end >= part_rows || !merged_part_offsets.areRowsDropped(part_index, range.begin, range.end))
            return false;
    }

    return true;
}

/// Removes the tokens whose row ids all belong to rows dropped by the merge, so that they do not enter the merge queue.
/// Such tokens are not written to the merged dictionary anyway (see flushPostingList).
static void removeTokensOfDroppedRows(DictionaryBlock & block, const MergedPartOffsets & merged_part_offsets, size_t part_index)
{
    const size_t num_tokens = block.token_infos.size();
    IColumn::Filter filter(num_tokens);
    size_t num_kept = 0;

    for (size_t i = 0; i < num_tokens; ++i)
    {
        filter[i] = !isTokenOfDroppedRows(block.token_infos[i], merged_part_offsets, part_index);
        num_kept += filter[i];
    }

    if (num_kept == num_tokens)
        return;

    block.tokens = block.tokens->filter(filter, num_kept);

    size_t pos = 0;
    for (size_t i = 0; i < num_tokens; ++i)
    {
        if (!filter[i])
            continue;

        if (pos != i)
            block.token_infos[pos] = std::move(block.token_infos[i]);
        ++pos;
    }

    block.token_infos.erase(block.token_infos.begin() + num_kept, block.token_infos.end());
}

void MergeTextIndexesTask::readDictionaryBlock(size_t source_num)
{
    auto * stream = input_streams[source_num].at(MergeTreeIndexSubstream::Type::TextIndexDictionary);
    auto * data_buffer = stream->getDataBuffer();

    while (!data_buffer->eof())
    {
        inputs[source_num] = TextIndexSerialization::deserializeDictionaryBlock(*data_buffer, /*with_postings=*/ true);

        /// Only a part that lost rows can have tokens of dropped rows. A block left without tokens is skipped.
        const size_t part_index = segments[source_num].part_index;
        if (merged_part_offsets && merged_part_offsets->isMappingWithDrops()
            && merged_part_offsets->getPartSurvivingRowsCount(part_index) < merged_part_offsets->getPartRowsCount(part_index))
        {
            removeTokensOfDroppedRows(inputs[source_num], *merged_part_offsets, part_index);
            if (inputs[source_num].tokens->empty())
                continue;
        }

        const auto & tokens = inputs[source_num].tokens;
        tokens_cursors[source_num].reset({tokens}, getHeader(), tokens->size());
        tokens_queue.push(tokens_cursors[source_num]);
        return;
    }
}

void MergeTextIndexesTask::checkRowIdsInPart(std::span<const UInt32> row_ids, size_t part_index) const
{
    if (!merged_part_offsets || row_ids.empty())
        return;

    /// The row ids are sorted by the format, so the last one bounds them all.
    /// The offsets map has an entry per row of the part, and a row id beyond it would be looked up outside of the map.
    size_t part_rows = merged_part_offsets->getPartRowsCount(part_index);

    if (row_ids.back() >= part_rows)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupted data in text index: row id {} exceeds the number of rows {} in the source part",
            row_ids.back(), part_rows);
    }
}

void MergeTextIndexesTask::adjustPartOffsets(PostingsMergeCursor & cursor)
{
    auto & row_ids = cursor.row_ids;
    size_t first_rank = cursor.num_decoded;
    cursor.num_decoded += row_ids.size();

    if (!merged_part_offsets)
        return;

    size_t part_index = segments[cursor.source->source_num].part_index;
    checkRowIdsInPart(row_ids, part_index);

    if (!merged_part_offsets->isMappingWithDrops())
    {
        for (UInt32 & row_id : row_ids)
            row_id = adjustPartOffset(*merged_part_offsets, part_index, row_id);
        return;
    }

    /// Row ids of the rows dropped by the merge are removed together with their position lists.
    size_t num_kept = 0;
    for (size_t i = 0; i < row_ids.size(); ++i)
    {
        auto new_offset = merged_part_offsets->tryGetNewOffset(part_index, row_ids[i]);

        if (!new_offset)
        {
            if (positions_merge->isActive())
                positions_merge->addDroppedRank(cursor.source_idx, first_rank + i);
            continue;
        }

        row_ids[num_kept++] = checkMergedRowId(*new_offset);
    }

    num_dropped_row_ids += row_ids.size() - num_kept;
    row_ids.resize(num_kept);
}

void MergeTextIndexesTask::initPostingsCursor(PostingsMergeCursor & cursor, const TokenSource & source)
{
    /// The cursor is reused across tokens: drop the state of the previous one before anything can fail.
    cursor.source = &source;
    cursor.source_idx = static_cast<size_t>(&source - current_token_sources.data());
    cursor.next_segment = 0;
    cursor.num_decoded = 0;
    cursor.pos = 0;
    cursor.row_ids.clear();

    const auto & info = source.info;

    if (info.embedded_postings.empty())
    {
        if (info.offsets.empty())
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupted data in text index: token with {} row ids has no posting list segments", info.cardinality);
        }

        /// If all rows of the source were dropped by the merge, the cursor stays invalid.
        advancePostingsCursor(cursor);
        return;
    }

    cursor.row_ids.assign(info.embedded_postings.begin(), info.embedded_postings.end());
    adjustPartOffsets(cursor);
    cursor.next_segment = info.offsets.size();
    cursor.pos = 0;
}

void MergeTextIndexesTask::readPostingsSegment(const TokenSource & source, size_t segment_idx, PaddedPODArray<UInt32> & row_ids)
{
    const auto & info = source.info;
    auto * stream = input_streams[source.source_num].at(MergeTreeIndexSubstream::Type::TextIndexPostings);
    stream->seekToMark({info.offsets[segment_idx], 0});
    source_postings_serializations[source.source_num].deserializeToArray(*stream->getDataBuffer(), info, segment_idx, row_ids);
}

bool MergeTextIndexesTask::advancePostingsCursor(PostingsMergeCursor & cursor)
{
    const auto & source = *cursor.source;

    /// A segment loses all its row ids if all its rows were dropped by the merge, then the next one is decoded.
    while (cursor.next_segment != source.info.offsets.size())
    {
        cursor.row_ids.clear();
        readPostingsSegment(source, cursor.next_segment, cursor.row_ids);

        /// Deserialization rejects an empty segment and a segment outside its row range; the order inside is not verified.
        chassert(!cursor.row_ids.empty());
        adjustPartOffsets(cursor);

        ++cursor.next_segment;
        cursor.pos = 0;

        if (!cursor.row_ids.empty())
        {
            chassert(std::is_sorted(cursor.row_ids.begin(), cursor.row_ids.end()));
            return true;
        }
    }

    return false;
}

MergeTextIndexesTask::PostingsMergeQueue::Window MergeTextIndexesTask::PostingsMergeQueue::selectWindow() const
{
    size_t min_pos = 0;
    UInt64 min_head = active_cursors[0]->current();
    UInt64 second_head = std::numeric_limits<UInt64>::max();

    for (size_t i = 1; i < active_cursors.size(); ++i)
    {
        UInt64 head = active_cursors[i]->current();

        if (head < min_head)
        {
            second_head = min_head;
            min_pos = i;
            min_head = head;
        }
        else if (head < second_head)
        {
            second_head = head;
        }
    }

    UInt64 begin = min_head - min_head % WINDOW_ROWS;
    return Window{.min_pos = min_pos, .second_head = second_head, .begin = begin, .end = begin + WINDOW_ROWS};
}

std::pair<UInt64, UInt64> MergeTextIndexesTask::PostingsMergeQueue::consumeWindow(Window window)
{
    chassert(std::ranges::all_of(window_bits, [](UInt64 word) { return word == 0; }));

    UInt64 num_consumed = 0;
    UInt64 bits_summary = 0;

    for (size_t i = 0; i < active_cursors.size();)
    {
        auto & cursor = *active_cursors[i];
        const auto & row_ids = cursor.row_ids;
        size_t pos = cursor.pos;

        while (pos < row_ids.size())
        {
            /// A row id below the window wraps around and stops the scan like a row id beyond the window.
            UInt64 bit = row_ids[pos] - window.begin;
            if (bit >= WINDOW_ROWS)
                break;

            window_bits[bit / 64] |= 1ULL << (bit % 64);
            bits_summary |= 1ULL << (bit / 64);
            if (positions)
                window_sources[bit] = static_cast<UInt32>(cursor.source_idx);
            ++pos;
        }

        /// The window starts at the smallest head, so only an unsorted (corrupted) source has a row id below it.
        /// Such a row id would address the bitset out of bounds, hence the scan above stops on it.
        if (pos < row_ids.size() && row_ids[pos] < window.begin)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupted data in text index: row id {} of a source posting list is below the merge window [{}, {})",
                row_ids[pos], window.begin, window.end);
        }

        num_consumed += pos - cursor.pos;
        cursor.pos = pos;

        /// A refilled segment may still start inside the window, so the same position is scanned again.
        /// If the source is exhausted, another cursor takes the position and is scanned next.
        if (cursor.isValid())
            ++i;
        else
            refill(i);
    }

    return {num_consumed, bits_summary};
}

void MergeTextIndexesTask::PostingsMergeQueue::processWindow(Window window)
{
    auto [num_consumed, bits_summary] = consumeWindow(window);

    size_t old_size = buffer.size();
    buffer.resize(old_size + num_consumed);
    UInt32 * out = buffer.data() + old_size;

    /// Consecutive row ids of one source are passed at once.
    size_t run_source = 0;
    size_t run_length = 0;

    while (bits_summary)
    {
        size_t word_idx = std::countr_zero(bits_summary);
        bits_summary &= bits_summary - 1;

        UInt64 word = std::exchange(window_bits[word_idx], 0);

        while (word)
        {
            size_t bit = word_idx * 64 + std::countr_zero(word);
            *out++ = static_cast<UInt32>(window.begin + bit);
            word &= word - 1;

            if (positions)
            {
                if (run_length != 0 && window_sources[bit] != run_source)
                {
                    positions->append(run_source, run_length);
                    run_length = 0;
                }
                run_source = window_sources[bit];
                ++run_length;
            }
        }
    }

    if (positions && run_length != 0)
        positions->append(run_source, run_length);

    size_t num_distinct = out - (buffer.data() + old_size);

    /// Sources own disjoint row sets, so every consumed row id must have set its own bit.
    if (num_distinct != num_consumed)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Source posting lists have overlapping row ids: {} distinct row ids out of {} in rows [{}, {})",
            num_distinct, num_consumed, window.begin, window.end);
    }
}

template <typename Sink>
void MergeTextIndexesTask::PostingsMergeQueue::flushRun(Window window, Sink && sink)
{
    chassert(window.hasOneSource());
    auto & cursor = *active_cursors[window.min_pos];
    auto remaining = cursor.remaining();

    size_t run_length = remaining.back() < window.second_head
        ? remaining.size()
        : std::lower_bound(remaining.begin(), remaining.end(), window.second_head) - remaining.begin();

    if (positions)
        positions->append(cursor.source_idx, run_length);
    flushDirect(remaining.first(run_length), sink);
    cursor.pos += run_length;

    if (!cursor.isValid())
        refill(window.min_pos);
}

template <typename Sink>
void MergeTextIndexesTask::PostingsMergeQueue::flushDirect(std::span<const UInt32> row_ids, Sink && sink)
{
    constexpr size_t granularity = IPostingListEncoder::append_granularity;

    if (row_ids.size() < granularity)
    {
        buffer.insert(row_ids.begin(), row_ids.end());
        flushBuffered(false, sink);
        return;
    }

    /// The buffered row ids precede the run, so they are completed to the granularity and passed first.
    if (!buffer.empty())
    {
        size_t prefix_to_buffer = granularity - buffer.size() % granularity;
        buffer.insert(row_ids.begin(), row_ids.begin() + prefix_to_buffer);
        sink(std::span<const UInt32>(buffer.data(), buffer.size()));
        row_ids = row_ids.subspan(prefix_to_buffer);
        buffer.clear();
    }

    size_t prefix_to_flush = row_ids.size() - row_ids.size() % granularity;

    if (prefix_to_flush != 0)
    {
        sink(row_ids.first(prefix_to_flush));
        row_ids = row_ids.subspan(prefix_to_flush);
    }

    buffer.insert(row_ids.begin(), row_ids.end());
    flushBuffered(false, sink);
}

template <typename Sink>
void MergeTextIndexesTask::PostingsMergeQueue::flushBuffered(bool is_final, Sink && sink)
{
    size_t count = buffer.size();

    if (!is_final)
    {
        if (count < ROWS_TO_BUFFER)
            return;

        count -= count % IPostingListEncoder::append_granularity;
    }

    if (count == 0)
        return;

    sink(std::span<const UInt32>(buffer.data(), count));
    buffer.erase(buffer.begin(), buffer.begin() + count);
}

void MergeTextIndexesTask::PostingsMergeQueue::refill(size_t pos)
{
    chassert(!active_cursors[pos]->isValid());
    if (task.advancePostingsCursor(*active_cursors[pos]))
        return;

    active_cursors[pos] = active_cursors.back();
    active_cursors.pop_back();
}

void MergeTextIndexesTask::PostingsMergeQueue::push(const TokenSource & source)
{
    chassert(active_cursors.size() < cursors.size());
    auto & cursor = cursors[active_cursors.size()];
    task.initPostingsCursor(cursor, source);

    /// All rows of the source may be dropped by the merge.
    if (cursor.isValid())
        active_cursors.push_back(&cursor);
}

template <typename Sink>
void MergeTextIndexesTask::PostingsMergeQueue::merge(Sink && sink, PositionsMerge * positions_)
{
    chassert(buffer.empty());
    positions = positions_;
    if (positions && window_sources.empty())
        window_sources.resize(WINDOW_ROWS);

    if (active_cursors.size() == 1)
    {
        auto & cursor = *active_cursors.front();

        do
        {
            if (positions)
                positions->append(cursor.source_idx, cursor.remaining().size());
            flushDirect(cursor.remaining(), sink);
        }
        while (task.advancePostingsCursor(cursor));

        active_cursors.clear();
    }

    while (!active_cursors.empty())
    {
        auto window = selectWindow();

        if (window.hasOneSource())
        {
            flushRun(window, sink);
        }
        else
        {
            processWindow(window);
            flushBuffered(false, sink);
        }
    }

    flushBuffered(true, sink);
}

template <typename Sink>
void MergeTextIndexesTask::mergePostings(Sink && sink)
{
    chassert(!postings_queue->isValid());

    const bool has_positions = params.enable_positions && std::ranges::any_of(current_token_sources,
        [](const auto & source) { return source.info.header & PostingsSerialization::Flags::HasPositions; });

    /// Positions are started first, because the cursors record the position lists of dropped rows as they decode postings.
    if (has_positions)
        positions_merge->start();

    for (const auto & source : current_token_sources)
    {
        if (source.info.cardinality != 0)
            postings_queue->push(source);
    }

    postings_queue->merge(sink, has_positions ? positions_merge.get() : nullptr);
}

/// Fills the info of a posting list with at most MAX_CARDINALITY_FOR_RAW_POSTINGS row ids, which are stored without a codec.
static void serializeRawPostingList(std::span<const UInt32> row_ids, MergeTreeIndexWriterStream & postings_stream, TokenPostingsInfo & token_info)
{
    using enum PostingsSerialization::Flags;
    token_info.cardinality = static_cast<UInt32>(row_ids.size());

    /// Embedded postings are serialized into the dictionary block by flushDictionaryBlock.
    if (row_ids.size() <= MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS)
    {
        token_info.header = RawPostings | EmbeddedPostings;
        token_info.embedded_postings.assign(row_ids.begin(), row_ids.end());
    }
    else
    {
        token_info.header = RawPostings | SingleBlock;
        token_info.offsets.emplace_back(postings_stream.plain_hashing.count());
        token_info.ranges.emplace_back(row_ids.front(), row_ids.back());
        TextIndexSerialization::serializeRawPostings(row_ids, postings_stream.plain_hashing);
    }
}

TokenPostingsInfo MergeTextIndexesTask::flushRawPostings(MergeTreeIndexWriterStream & postings_stream, size_t total_cardinality)
{
    TokenPostingsInfo token_info;

    /// Raw postings are fewer than the encoder granularity, so the queue passes all of them in one chunk.
    mergePostings([&](std::span<const UInt32> row_ids)
    {
        if (token_info.cardinality != 0)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Raw postings must be passed in one chunk, got {} row ids after {}",
                row_ids.size(), token_info.cardinality);
        }

        serializeRawPostingList(row_ids, postings_stream, token_info);
    });

    /// Sources own disjoint row sets, so the merged cardinality must equal the sum of source cardinalities.
    if (token_info.cardinality != total_cardinality)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Merged posting list has {} row ids while source posting lists have {} in total",
            token_info.cardinality, total_cardinality);
    }

    return token_info;
}

TokenPostingsInfo MergeTextIndexesTask::flushEncodedPostings(MergeTreeIndexWriterStream & postings_stream, size_t total_cardinality)
{
    const auto * codec = postings_serialization.getPostingListCodec();
    auto encoder = codec->createEncoder(params.posting_list_block_size);

    mergePostings([&](std::span<const UInt32> row_ids)
    {
        encoder->append(row_ids);
    });

    /// Sources own disjoint row sets, so the merged cardinality must equal the sum of source cardinalities.
    if (encoder->cardinality() != total_cardinality)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Merged posting list has {} row ids while source posting lists have {} in total",
            encoder->cardinality(), total_cardinality);
    }

    TokenPostingsInfo token_info;
    token_info.cardinality = static_cast<UInt32>(total_cardinality);
    encoder->finalize(postings_stream.plain_hashing, token_info);
    return token_info;
}

TokenPostingsInfo MergeTextIndexesTask::flushPostingsWithDrops(MergeTreeIndexWriterStream & postings_stream, size_t total_cardinality)
{
    /// The format of a posting list depends on its cardinality, which is known only after the row ids of the dropped rows are removed.
    /// The row ids are buffered until there are more of them than raw postings may hold.
    const auto * codec = postings_serialization.getPostingListCodec();
    decltype(codec->createEncoder(params.posting_list_block_size)) encoder;
    buffered_row_ids.clear();

    mergePostings([&](std::span<const UInt32> row_ids)
    {
        if (!encoder && buffered_row_ids.size() + row_ids.size() <= MAX_CARDINALITY_FOR_RAW_POSTINGS)
        {
            buffered_row_ids.insert(row_ids.begin(), row_ids.end());
            return;
        }

        if (!encoder)
        {
            encoder = codec->createEncoder(params.posting_list_block_size);

            /// The buffered row ids are chunks followed by another one, so their number is a multiple of the append granularity.
            if (!buffered_row_ids.empty())
                encoder->append(std::span<const UInt32>(buffered_row_ids.data(), buffered_row_ids.size()));
            buffered_row_ids.clear();
        }

        encoder->append(row_ids);
    });

    /// Sources own disjoint row sets, so the merged cardinality must equal the sum of source cardinalities without the dropped row ids.
    size_t cardinality = encoder ? encoder->cardinality() : buffered_row_ids.size();
    if (cardinality + num_dropped_row_ids != total_cardinality)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Merged posting list has {} row ids and {} row ids of dropped rows while source posting lists have {} in total",
            cardinality, num_dropped_row_ids, total_cardinality);
    }

    TokenPostingsInfo token_info;

    if (encoder)
    {
        token_info.cardinality = static_cast<UInt32>(cardinality);
        encoder->finalize(postings_stream.plain_hashing, token_info);
    }
    else if (!buffered_row_ids.empty())
    {
        serializeRawPostingList(std::span<const UInt32>(buffered_row_ids.data(), buffered_row_ids.size()), postings_stream, token_info);
    }

    return token_info;
}

void MergeTextIndexesTask::flushPostingList()
{
    chassert(!current_token_sources.empty());

    auto * postings_stream = output_streams.at(MergeTreeIndexSubstream::Type::TextIndexPostings);
    TokenPostingsInfo token_info;

    /// Sources own disjoint row sets, so the cardinality of the merged posting is sum of source cardinalities.
    size_t total_cardinality = 0;
    for (const auto & source : current_token_sources)
        total_cardinality += source.info.cardinality;

    num_dropped_row_ids = 0;

    if (merged_part_offsets && merged_part_offsets->isMappingWithDrops())
        token_info = flushPostingsWithDrops(*postings_stream, total_cardinality);
    else if (total_cardinality <= MAX_CARDINALITY_FOR_RAW_POSTINGS)
        token_info = flushRawPostings(*postings_stream, total_cardinality);
    else
        token_info = flushEncodedPostings(*postings_stream, total_cardinality);

    positions_merge->finish(token_info);
    current_token_sources.clear();

    /// A token whose row ids all belong to rows dropped by the merge is omitted from the dictionary.
    if (token_info.cardinality == 0 && num_dropped_row_ids != 0)
    {
        output_tokens->popBack(1);
        return;
    }

    output_infos.push_back(token_info);
}

void MergeTextIndexesTask::flushDictionaryBlock()
{
    if (output_tokens->size() != output_infos.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tokens size ({}) doesn't match infos size ({})", output_tokens->size(), output_infos.size());

    if (output_infos.empty())
        return;

    auto tokens_format = params.dictionary_block_frontcoding_compression
        ? TextIndexSerialization::TokensFormat::FrontCodedStrings
        : TextIndexSerialization::TokensFormat::RawStrings;

    size_t num_tokens = output_infos.size();
    auto & output_str = assert_cast<ColumnString &>(*output_tokens);
    auto * dictionary_stream = output_streams.at(MergeTreeIndexSubstream::Type::TextIndexDictionary);
    auto & ostr = dictionary_stream->compressed_hashing;

    ostr.next();
    auto current_mark = dictionary_stream->getCurrentMark();
    chassert(current_mark.offset_in_decompressed_block == 0);

    auto first_token = output_tokens->getDataAt(0);
    TextIndexSerialization::checkTokenSize(first_token.size());
    assert_cast<ColumnString &>(*sparse_index_tokens).insertData(first_token.data(), first_token.size());
    assert_cast<ColumnUInt64 &>(*sparse_index_offsets).insertValue(current_mark.offset_in_compressed_file);

    TextIndexSerialization::serializeTokens(output_str, ostr, tokens_format);

    for (size_t i = 0; i < num_tokens; ++i)
    {
        TextIndexSerialization::serializeTokenInfo(ostr, output_infos[i]);

        if (output_infos[i].header & PostingsSerialization::Flags::EmbeddedPostings)
            TextIndexSerialization::serializeRawPostings(output_infos[i].embedded_postings, ostr);
    }

    output_tokens = ColumnString::create();
    output_infos.clear();
}

bool MergeTextIndexesTask::isNewToken(const TokenSortCursor & cursor) const
{
    const auto & input_str = assert_cast<const ColumnString &>(*inputs[cursor->order].tokens);
    const auto & output_str = assert_cast<const ColumnString &>(*output_tokens);

    return output_str.empty() || input_str.compareAt(cursor->getRow(), output_str.size() - 1, output_str, 1) != 0;
}

bool MergeTextIndexesTask::executeStep()
{
    if (!is_initialized)
    {
        is_initialized = true;
        initializeTokensQueue();

        /// Write marks for compatibility with other skip indexes.
        /// An empty part carries no marks at all, exactly like every other skip index on an empty part.
        if (num_rows != 0)
        {
            chassert(new_data_part);
            bool can_use_adaptive_granularity = new_data_part->index_granularity_info.mark_type.adaptive;
            writeMarks(output_streams, can_use_adaptive_granularity);
        }
    }

    if (!tokens_queue.isValid())
    {
        finalize();
        return false;
    }

    Stopwatch watch(CLOCK_MONOTONIC_COARSE);

    do
    {
        auto [current_ptr, batch_size] = tokens_queue.current();
        TokenSortCursor & current = *current_ptr;

        size_t source_num = current->order;
        auto & source_block = inputs[source_num];

        /// All rows of a batch belong to one dictionary block, whose tokens are strictly
        /// increasing. Only the first row of the batch can continue the current token.
        bool first_row_is_new_token = isNewToken(current);
        size_t row = current->getRow();

        for (size_t i = 0; i < batch_size; ++i, ++row)
        {
            if (i > 0 || first_row_is_new_token)
            {
                if (!current_token_sources.empty())
                    flushPostingList();

                if (output_tokens->size() >= params.dictionary_block_size)
                    flushDictionaryBlock();

                auto & output_tokens_str = assert_cast<ColumnString &>(*output_tokens);
                output_tokens_str.insertFrom(*source_block.tokens, row);
            }

            /// Postings and positions are decoded lazily on flush.
            /// Copy the info because the dictionary block it points into may be replaced before that.
            current_token_sources.push_back({source_num, source_block.token_infos[row]});
        }

        if (!current->isLast(batch_size))
        {
            tokens_queue.next(batch_size);
        }
        else
        {
            tokens_queue.removeTop();
            readDictionaryBlock(source_num);
        }
    } while (tokens_queue.isValid() && watch.elapsedMilliseconds() < step_time_ms);

    return true;
}

void MergeTextIndexesTask::finalize()
{
    if (!current_token_sources.empty())
        flushPostingList();

    if (!output_tokens->empty())
        flushDictionaryBlock();

    auto * index_stream = output_streams.at(MergeTreeIndexSubstream::Type::Regular);

    TextIndexHeader header
    {
        .version = params.serialization_version,
        .codec_type = postings_serialization.getPostingListCodec()->getType(),
        .has_positions = params.enable_positions,
        .positions_codec = params.positions_codec,
        .sparse_index = DictionarySparseIndex(std::move(sparse_index_tokens), std::move(sparse_index_offsets)),
    };

    TextIndexSerialization::serializeHeader(header, index_stream->compressed_hashing);

    for (auto & stream : output_streams_holders)
        stream->finalize();

    /// Same as in `MergeTreeDataPartWriterOnDisk::finishSkipIndicesSerialization`
    if (need_fsync)
    {
        std::vector<const MergeTreeWriterStream *> streams_to_sync;
        streams_to_sync.reserve(output_streams_holders.size());
        for (const auto & stream : output_streams_holders)
            streams_to_sync.push_back(stream.get());
        parallelSyncFiles(streams_to_sync);
    }
}

void MergeTextIndexesTask::cancel() noexcept
{
    cancelImpl();
}

void MergeTextIndexesTask::cancelImpl() noexcept
{
    try
    {
        for (auto & stream : output_streams_holders)
            stream->cancel();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void MergeTextIndexesTask::addToChecksums(MergeTreeDataPartChecksums & checksums)
{
    for (const auto & [type, stream] : output_streams)
        stream->addToChecksums(checksums, MergeTreeIndexSubstream::isCompressed(type));
}

MutableDataPartStoragePtr createTemporaryTextIndexStorage(const DiskPtr & disk, const String & part_relative_path)
{
    static constexpr const char * temp_part_dir = "text_index_tmp";
    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_relative_path + "_" + temp_part_dir, disk, 0);
    auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, part_relative_path, temp_part_dir);
    storage->beginTransaction();
    storage->createDirectories();
    return storage;
}

size_t estimatePostingListBufferSize(const TokenPostingsInfo & token_info)
{
    const size_t num_segments = token_info.offsets.size();
    if (num_segments == 0)
        return 0;

    /// Small lists are read in one piece anyway, so do not go below the default buffer size for postings.
    constexpr size_t min_buffer_size = 16 * 1024;
    size_t largest_segment_bytes = 0;

    if (num_segments == 1)
    {
        /// A single segment holds the whole list.
        /// Its row-id deltas are bit-packed to the bit width of the gaps:
        /// take twice the bit width of the average gap per posting to cover larger gaps and block headers.
        const auto & range = token_info.ranges.front();
        const size_t cardinality = std::max<size_t>(1, token_info.cardinality);
        const size_t average_gap = (range.end - range.begin + 1) / cardinality;
        const size_t bits_per_posting = 2 * static_cast<size_t>(std::bit_width(average_gap));
        largest_segment_bytes = cardinality * bits_per_posting / 8;
    }
    else
    {
        /// With several segments all but the last are full.
        /// Estimate the largest segment by the largest gap between offsets.
        for (size_t i = 1; i < num_segments; ++i)
            largest_segment_bytes = std::max(largest_segment_bytes, static_cast<size_t>(token_info.offsets[i] - token_info.offsets[i - 1]));
    }

    return std::max(largest_segment_bytes, min_buffer_size);
}

static MergeTreeReaderSettings makeTextIndexReaderSettings(
    const MergeTreeReaderSettings & reader_settings,
    MergeTreeIndexSubstream::Type substream_type,
    std::optional<size_t> expected_buffer_size,
    bool use_page_cache_lookahead)
{
    using enum MergeTreeIndexSubstream::Type;

    auto settings = reader_settings;
    settings.is_compressed = MergeTreeIndexSubstream::isCompressed(substream_type);

    /// An out-of-order stream's seeks never read the lookahead blocks a cache miss would otherwise
    /// request: the abandoned request leaves data in flight, so the connection (distributed cache,
    /// or S3 HTTP, see `HTTPConnectionPool::EndpointConnectionPool::atConnectionDestroy`) is dropped.
    if (!use_page_cache_lookahead && !reader_settings.use_page_cache_lookahead_for_text_index)
        settings.read_settings.page_cache_settings.lookahead_blocks = 1;

    if (expected_buffer_size && (substream_type == TextIndexDictionary || substream_type == TextIndexPostings))
    {
        constexpr size_t min_buffer_size = 16 * 1024;

        auto adjust = [&](size_t & buffer_size)
        {
            buffer_size = std::clamp(*expected_buffer_size, std::min(min_buffer_size, buffer_size), std::max(min_buffer_size, buffer_size));
        };

        adjust(settings.read_settings.local_fs_settings.buffer_size);
        adjust(settings.read_settings.remote_fs_settings.buffer_size);
    }

    return settings;
}

static std::unique_ptr<MergeTreeReaderStream> makeTextIndexInputStreamImpl(
    DataPartStoragePtr data_part_storage,
    const String & actual_stream_name,
    const String & extension,
    size_t data_file_size,
    const MergeTreeReaderSettings & reader_settings)
{
    static constexpr size_t marks_count = 1;

    /// Use reader stream that doesn't read marks,
    /// because text index always has one mark.
    return std::make_unique<MergeTreeReaderStreamSingleColumnWholePart>(
        data_part_storage,
        actual_stream_name,
        extension,
        marks_count,
        MarkRanges{{0, marks_count}},
        reader_settings,
        /*uncompressed_cache=*/ nullptr,
        data_file_size,
        /*marks_loader=*/ nullptr,
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);
}

std::unique_ptr<MergeTreeReaderStream> makeTextIndexInputStream(
    const IMergeTreeDataPartInfoForReader & data_part_info,
    const String & index_file_name,
    const MergeTreeIndexSubstream & substream,
    const MergeTreeReaderSettings & reader_settings,
    std::optional<size_t> expected_buffer_size,
    bool use_page_cache_lookahead)
{
    const auto stream_name = index_file_name + substream.suffix;
    const auto & extension = substream.extension;

    /// Mirrors IMergeTreeDataPart::getFileSizeOrZeroResolved: the on-disk name (original or hashed)
    /// comes from checksums, and a stream with no checksums entry is resolved and sized via the storage.
    auto data_part_storage = data_part_info.getDataPartStorage();
    std::optional<String> actual_stream_name = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, data_part_info.getChecksums());
    size_t data_file_size = 0;

    if (actual_stream_name)
    {
        data_file_size = data_part_info.getFileSizeOrZero(*actual_stream_name + extension);
    }
    else
    {
        actual_stream_name = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, *data_part_storage);
        if (actual_stream_name)
            data_file_size = data_part_storage->getFileSize(*actual_stream_name + extension);
    }

    if (!actual_stream_name)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File for text index stream {} does not exist", stream_name + extension);

    return makeTextIndexInputStreamImpl(
        std::move(data_part_storage),
        *actual_stream_name,
        extension,
        data_file_size,
        makeTextIndexReaderSettings(reader_settings, substream.type, expected_buffer_size, use_page_cache_lookahead));
}

std::unique_ptr<MergeTreeReaderStream> makeTextIndexInputStream(
    DataPartStoragePtr data_part_storage,
    const String & index_file_name,
    const MergeTreeIndexSubstream & substream,
    const MergeTreeReaderSettings & reader_settings,
    std::optional<size_t> expected_buffer_size,
    bool use_page_cache_lookahead)
{
    const auto stream_name = index_file_name + substream.suffix;
    const auto & extension = substream.extension;

    /// Check for both original and hashed filenames (hashed if the index name is too long)
    auto actual_stream_name = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, *data_part_storage);
    if (!actual_stream_name)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File for text index stream {} does not exist", stream_name + extension);

    return makeTextIndexInputStreamImpl(
        data_part_storage,
        *actual_stream_name,
        extension,
        data_part_storage->getFileSize(*actual_stream_name + extension),
        makeTextIndexReaderSettings(reader_settings, substream.type, expected_buffer_size, use_page_cache_lookahead));
}

}
