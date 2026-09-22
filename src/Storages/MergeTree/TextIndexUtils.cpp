#include <Processors/Port.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/TextIndexUtils.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Compression/CompressionFactory.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Parsers/parseQuery.h>
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
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>

#include <array>
#include <bit>
#include <limits>
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
    ParserCodec codec_parser;
    auto ast = parseQuery(codec_parser, "(" + Poco::toUpper(marks_compression_codec) + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    return CompressionCodecFactory::instance().get(ast, nullptr);
}

/// Merge-path decode of blocked positions: the stream stores per-posting-rank position lists with
/// no document ids, so it is paired with the token's posting lists (its rank space, in pre-remap
/// doc order) to rebuild roaringish entries the merge can remap and re-encode.
void decodeBlockedPositions(
    ReadBuffer & in,
    std::span<const UInt32> doc_ids,
    UInt64 expected_num_docs,
    size_t available_bytes,
    TextIndexBlockedPositionsCodec::DecodeScratch & scratch,
    PODArray<RoaringishEntry> & entries)
{
    PaddedPODArray<UInt32> doc_offsets;
    PaddedPODArray<UInt32> positions;
    TextIndexBlockedPositionsCodec::decodeAll(in, expected_num_docs, available_bytes, doc_offsets, positions, scratch);

    entries.reserve(entries.size() + positions.size());

    size_t rank = 0;
    for (const UInt32 doc : doc_ids)
    {
        if (rank + 1 >= doc_offsets.size())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupt text index positions: more posting documents than position lists ({})", rank);

        for (UInt32 i = doc_offsets[rank]; i < doc_offsets[rank + 1]; ++i)
        {
            const auto entry = RoaringishEntry::make(doc, positions[i]);
            if (!entries.empty() && entries.back().sameBucket(entry))
                entries.back().mergeBitmap(entry);
            else
                entries.push_back(entry);
        }
        ++rank;
    }

    if (rank + 1 != doc_offsets.size())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: {} posting documents but {} position lists", rank, doc_offsets.size() - 1);
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
    for (const auto & [type, stream] : streams)
    {
        /// Per-row substreams get the marks of the part once their bytes are written.
        if (MergeTreeIndexSubstream::isPerRow(type))
            continue;

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
    if (block.rows() == 0)
        return;

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
    for (size_t i = 0; i < indexes.size(); ++i)
    {
        if (!aggregators[i]->empty())
            writeTemporarySegment(i);
    }
}

std::vector<TextIndexSegment> BuildTextIndexTransform::getSegments(const String & index_name, size_t part_idx) const
{
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

    /// The index parameters already carry the version that can represent every enabled feature, so with
    /// BM25 scoring this is `V3_WithScoring` and each token's `HasTermFrequencies` flag is valid.
    return PostingsSerialization(std::move(codec_copy), text_index.getParams().serialization_version);
}

static ALWAYS_INLINE UInt32 adjustPartOffset(const MergedPartOffsets & merged_part_offsets, size_t part_index, UInt32 row_id)
{
    UInt64 new_offset = merged_part_offsets[part_index, row_id];

    if (new_offset > std::numeric_limits<UInt32>::max())
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot merge text index: remapped row id {} exceeds the maximum supported row id {}",
            new_offset, std::numeric_limits<UInt32>::max());
    }

    return static_cast<UInt32>(new_offset);
}

struct MergeTextIndexesTask::PostingsMergeCursor
{
    const TokenSource * source = nullptr;
    /// Position of the current row id in the row_ids array.
    size_t pos = 0;
    /// Next entry of info.offsets to decode.
    size_t next_segment = 0;
    /// Decoded and remapped row ids of the current segment.
    PaddedPODArray<UInt32> row_ids;
    /// Exact per-row term frequencies of the current segment, parallel to the row ids.
    /// Empty when the source stores none (every `tf == 1`) or the merge is not scoring.
    PaddedPODArray<UInt32> tfs;
    /// Row ids of the source's postings for the current token in pre-remap order, accumulated
    /// across segments. Filled only when the token has positions, which are addressed by posting rank.
    PaddedPODArray<UInt32> token_row_ids;

    UInt32 current() const { return row_ids[pos]; }
    bool isValid() const { return pos < row_ids.size(); }
    std::span<const UInt32> remaining() const { return {row_ids.data() + pos, row_ids.size() - pos}; }

    /// Term frequencies parallel to remaining(), or an empty span when the source stores none.
    std::span<const UInt32> remainingTfs() const
    {
        if (tfs.empty())
            return {};
        return {tfs.data() + pos, tfs.size() - pos};
    }
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
///
/// On the scoring path every chunk comes with the per-row `(tf - 1)` parallel to its row ids. The term frequencies
/// are converted from the exact `tf` of the cursors on the way, so on this path every chunk goes through the buffer.
class MergeTextIndexesTask::PostingsMergeQueue
{
public:
    PostingsMergeQueue(MergeTextIndexesTask & task_, size_t max_sources, bool with_term_frequencies_)
        : task(task_), with_term_frequencies(with_term_frequencies_), cursors(max_sources)
    {
        active_cursors.reserve(max_sources);
    }

    void push(const TokenSource & source);
    bool isValid() const { return !active_cursors.empty(); }

    /// True if any pushed source of the current token stores term frequencies; otherwise every merged `tf` is 1.
    bool hasTermFrequencies() const { return has_term_frequencies; }

    /// Passes the row ids of the active cursors to the sink in the globally sorted order.
    /// Aligned runs of a single source are passed through without copying, the rest is buffered.
    /// Once a source is exhausted, its positions (if any) are appended to the task's output_positions.
    /// Leaves the queue ready for the sources of the next token.
    template <typename Sink> void merge(Sink && sink);

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

    /// Sets a bit in window_bits for every row id of every cursor inside the window;
    /// on the scoring path also stores its `(tf - 1)` in window_tfs under the same bit.
    /// Returns the number of consumed row ids and the mask of non-empty words of window_bits.
    std::pair<UInt64, UInt64> consumeWindow(Window window);

    /// Extracts row ids with set bits inside the window (and their `(tf - 1)`) and appends them to the buffer.
    void processWindow(Window window);

    /// Flushes the run of the smallest source below the second head to the sink.
    template <typename Sink>
    void flushRun(Window window, Sink && sink);

    /// Flushes the row ids to the sink directly if they are aligned with append_granularity, otherwise buffers them.
    /// On the scoring path the chunk is always buffered, converting its term frequencies to `(tf - 1)`.
    template <typename Sink>
    void flushDirect(std::span<const UInt32> row_ids, std::span<const UInt32> tfs, Sink && sink);

    /// Flushes the aligned prefix of the buffered row ids to the sink once enough are accumulated, or all of them at the end.
    template <typename Sink>
    void flushBuffered(bool is_final, Sink && sink);

    /// Appends the row ids to the buffer and, on the scoring path, their `(tf - 1)` to tfs_buffer.
    /// An empty `tfs` means every `tf` of the chunk is 1.
    void appendToBuffer(std::span<const UInt32> row_ids, std::span<const UInt32> tfs);

    /// Loads the next segment of the exhausted cursor, or reads the positions of its source and drops it.
    void refill(size_t pos);

    MergeTextIndexesTask & task;
    /// Whether the chunks carry `(tf - 1)`, i.e. the merge is scoring.
    const bool with_term_frequencies;
    /// Whether any pushed source of the current token stores term frequencies.
    bool has_term_frequencies = false;
    /// Reusable cursors, one per source.
    std::vector<PostingsMergeCursor> cursors;
    /// Cursors of the current token that still have row ids to merge.
    std::vector<PostingsMergeCursor *> active_cursors;
    /// Bitset of the current window.
    std::array<UInt64, WINDOW_ROWS / 64> window_bits{};
    /// `(tf - 1)` of the row ids of the current window, indexed by the bit. Used only on the scoring path.
    std::array<UInt32, WINDOW_ROWS> window_tfs{};
    /// Row ids buffered for the sink.
    PaddedPODArray<UInt32> buffer;
    /// `(tf - 1)` buffered for the sink, parallel to buffer. Used only on the scoring path.
    PaddedPODArray<UInt32> tfs_buffer;
};

MergeTextIndexesTask::MergeTextIndexesTask(
    std::vector<TextIndexSegment> segments_,
    MergeTreeMutableDataPartPtr new_data_part_,
    MergeTreeIndexGranularityPtr index_granularity_,
    size_t num_rows_,
    MergeTreeIndexPtr index_ptr_,
    std::shared_ptr<MergedPartOffsets> merged_part_offsets_,
    const MergeTreeReaderSettings & reader_settings_,
    const MergeTreeWriterSettings & writer_settings_,
    bool need_fsync_)
    : segments(std::move(segments_))
    , new_data_part(std::move(new_data_part_))
    , index_granularity(std::move(index_granularity_))
    , num_rows(num_rows_)
    , index_ptr(std::move(index_ptr_))
    , merged_part_offsets(std::move(merged_part_offsets_))
    , writer_settings(writer_settings_)
    , need_fsync(need_fsync_)
    , step_time_ms((*new_data_part->storage.getSettings())[MergeTreeSetting::background_task_preferred_step_execution_time_ms].totalMilliseconds())
    , postings_serialization(createPostingsSerialization(*index_ptr))
{
    tokens_cursors.resize(segments.size());
    inputs.resize(segments.size());
    input_streams.resize(segments.size());
    output_tokens = ColumnString::create();

    const auto & text_index = typeid_cast<const MergeTreeIndexText &>(*index_ptr);
    params = text_index.getParams();
    postings_queue = std::make_unique<PostingsMergeQueue>(*this, segments.size(), params.hasScoring());
    sparse_index_tokens = ColumnString::create();
    sparse_index_offsets = ColumnUInt64::create();

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
            auto stream = makeTextIndexInputStream(
                segments[i].part_storage,
                segments[i].index_file_name + substream.suffix,
                substream.extension,
                MergeTreeIndexReader::patchSettings(reader_settings_, substream.type));

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

void MergeTextIndexesTask::readDictionaryBlock(size_t source_num)
{
    auto * stream = input_streams[source_num].at(MergeTreeIndexSubstream::Type::TextIndexDictionary);
    auto * data_buffer = stream->getDataBuffer();

    if (data_buffer->eof())
        return;

    inputs[source_num] = TextIndexSerialization::deserializeDictionaryBlock(*data_buffer, /*with_postings=*/true);
    const auto & tokens = inputs[source_num].tokens;
    tokens_cursors[source_num].reset({tokens}, getHeader(), tokens->size());
    tokens_queue.push(tokens_cursors[source_num]);
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

void MergeTextIndexesTask::adjustPartOffsets(std::span<UInt32> row_ids, size_t part_index) const
{
    if (!merged_part_offsets)
        return;

    checkRowIdsInPart(row_ids, part_index);

    for (UInt32 & row_id : row_ids)
        row_id = adjustPartOffset(*merged_part_offsets, part_index, row_id);
}

void MergeTextIndexesTask::initPostingsCursor(PostingsMergeCursor & cursor, const TokenSource & source)
{
    /// The cursor is reused across tokens: drop the state of the previous one before anything can fail.
    cursor.source = &source;
    cursor.pos = 0;
    cursor.next_segment = 0;
    cursor.row_ids.clear();
    cursor.tfs.clear();
    cursor.token_row_ids.clear();

    const auto & info = source.info;

    if (info.embedded_postings.empty())
    {
        if (!advancePostingsCursor(cursor))
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "Corrupted data in text index: token with {} row ids has no posting list segments", info.cardinality);
        }
        return;
    }

    /// Embedded postings are already in memory. The row ids are captured for the positions before they are remapped.
    cursor.row_ids.assign(info.embedded_postings.begin(), info.embedded_postings.end());
    captureRowIdsForPositions(cursor);
    adjustPartOffsets(cursor.row_ids, segments[source.source_num].part_index);

    if (params.hasScoring() && !info.embedded_term_frequencies.empty())
        cursor.tfs.assign(info.embedded_term_frequencies.begin(), info.embedded_term_frequencies.end());

    cursor.next_segment = info.offsets.size();
}

void MergeTextIndexesTask::readPostingsSegment(const TokenSource & source, size_t segment_idx, PaddedPODArray<UInt32> & row_ids, PaddedPODArray<UInt32> & tfs)
{
    const auto & info = source.info;
    auto * stream = input_streams[source.source_num].at(MergeTreeIndexSubstream::Type::TextIndexPostings);
    stream->seekToMark({info.offsets[segment_idx], 0});

    /// The exact term frequencies are decoded only if the source stores them and the merge is scoring.
    const bool has_term_frequencies = params.hasScoring() && (info.header & PostingsSerialization::Flags::HasTermFrequencies);

    source_postings_serializations[source.source_num].deserializeToArray(
        *stream->getDataBuffer(), info, segment_idx, row_ids, has_term_frequencies ? &tfs : nullptr);
}

bool MergeTextIndexesTask::advancePostingsCursor(PostingsMergeCursor & cursor)
{
    const auto & source = *cursor.source;
    if (cursor.next_segment == source.info.offsets.size())
        return false;

    cursor.row_ids.clear();
    cursor.tfs.clear();
    readPostingsSegment(source, cursor.next_segment, cursor.row_ids, cursor.tfs);
    captureRowIdsForPositions(cursor);
    adjustPartOffsets(cursor.row_ids, segments[source.source_num].part_index);

    ++cursor.next_segment;
    cursor.pos = 0;

    /// Deserialization rejects an empty segment and a segment outside its row range; the order inside is not verified.
    chassert(!cursor.row_ids.empty());
    chassert(std::is_sorted(cursor.row_ids.begin(), cursor.row_ids.end()));
    chassert(cursor.tfs.empty() || cursor.tfs.size() == cursor.row_ids.size());
    return true;
}

void MergeTextIndexesTask::captureRowIdsForPositions(PostingsMergeCursor & cursor) const
{
    /// Positions are addressed by posting rank, so decoding them needs the token's row ids of this
    /// source in pre-remap order. The row ids are remapped in place right after, so capture them first.
    if (params.enable_positions && (cursor.source->info.header & PostingsSerialization::Flags::HasPositions))
        cursor.token_row_ids.insert(cursor.row_ids.begin(), cursor.row_ids.end());
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

        /// The `(tf - 1)` of every consumed row id is stored under its bit, to be gathered along with the row id.
        if (with_term_frequencies)
        {
            if (cursor.tfs.empty())
            {
                for (size_t j = cursor.pos; j < pos; ++j)
                    window_tfs[row_ids[j] - window.begin] = 0;
            }
            else
            {
                for (size_t j = cursor.pos; j < pos; ++j)
                    window_tfs[row_ids[j] - window.begin] = cursor.tfs[j] - 1;
            }
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

    UInt32 * tfs_out = nullptr;
    if (with_term_frequencies)
    {
        tfs_buffer.resize(old_size + num_consumed);
        tfs_out = tfs_buffer.data() + old_size;
    }

    while (bits_summary)
    {
        size_t word_idx = std::countr_zero(bits_summary);
        bits_summary &= bits_summary - 1;

        UInt64 word = std::exchange(window_bits[word_idx], 0);

        while (word)
        {
            size_t bit = word_idx * 64 + std::countr_zero(word);
            word &= word - 1;

            *out++ = static_cast<UInt32>(window.begin + bit);
            if (tfs_out)
                *tfs_out++ = window_tfs[bit];
        }
    }

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
    auto remaining_tfs = cursor.remainingTfs();

    size_t run_length = remaining.back() < window.second_head
        ? remaining.size()
        : std::lower_bound(remaining.begin(), remaining.end(), window.second_head) - remaining.begin();

    flushDirect(remaining.first(run_length), remaining_tfs.empty() ? remaining_tfs : remaining_tfs.first(run_length), sink);
    cursor.pos += run_length;

    if (!cursor.isValid())
        refill(window.min_pos);
}

template <typename Sink>
void MergeTextIndexesTask::PostingsMergeQueue::flushDirect(std::span<const UInt32> row_ids, std::span<const UInt32> tfs, Sink && sink)
{
    constexpr size_t granularity = IPostingListEncoder::append_granularity;

    if (with_term_frequencies || row_ids.size() < granularity)
    {
        appendToBuffer(row_ids, tfs);
        flushBuffered(false, sink);
        return;
    }

    /// Only reached without scoring, so the chunks carry no term frequencies and the sink gets an empty span.
    chassert(tfs.empty());

    /// The buffered row ids precede the run, so they are completed to the granularity and passed first.
    if (!buffer.empty())
    {
        size_t prefix_to_buffer = granularity - buffer.size() % granularity;
        buffer.insert(row_ids.begin(), row_ids.begin() + prefix_to_buffer);
        sink(std::span<const UInt32>(buffer.data(), buffer.size()), std::span<const UInt32>{});
        row_ids = row_ids.subspan(prefix_to_buffer);
        buffer.clear();
    }

    size_t prefix_to_flush = row_ids.size() - row_ids.size() % granularity;

    if (prefix_to_flush != 0)
    {
        sink(row_ids.first(prefix_to_flush), std::span<const UInt32>{});
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

    auto tfs = with_term_frequencies ? std::span<const UInt32>(tfs_buffer.data(), count) : std::span<const UInt32>{};
    sink(std::span<const UInt32>(buffer.data(), count), tfs);
    buffer.erase(buffer.begin(), buffer.begin() + count);

    if (with_term_frequencies)
        tfs_buffer.erase(tfs_buffer.begin(), tfs_buffer.begin() + count);
}

void MergeTextIndexesTask::PostingsMergeQueue::appendToBuffer(std::span<const UInt32> row_ids, std::span<const UInt32> tfs)
{
    buffer.insert(row_ids.begin(), row_ids.end());

    if (!with_term_frequencies)
        return;

    if (tfs.empty())
    {
        tfs_buffer.resize_fill(tfs_buffer.size() + row_ids.size(), 0u);
    }
    else
    {
        chassert(tfs.size() == row_ids.size());
        for (UInt32 tf : tfs)
            tfs_buffer.push_back(tf - 1);
    }
}

void MergeTextIndexesTask::PostingsMergeQueue::refill(size_t pos)
{
    auto & cursor = *active_cursors[pos];
    chassert(!cursor.isValid());

    if (task.advancePostingsCursor(cursor))
        return;

    /// The source is exhausted, so its row ids are fully captured and its positions can be paired with them.
    task.readAndAppendPositions(cursor);
    active_cursors[pos] = active_cursors.back();
    active_cursors.pop_back();
}

void MergeTextIndexesTask::PostingsMergeQueue::push(const TokenSource & source)
{
    chassert(active_cursors.size() < cursors.size());
    auto & cursor = cursors[active_cursors.size()];
    task.initPostingsCursor(cursor, source);
    /// A source stores term frequencies either for all of its segments or for none, so the first decoded one tells.
    has_term_frequencies |= !cursor.tfs.empty();
    active_cursors.push_back(&cursor);
}

template <typename Sink>
void MergeTextIndexesTask::PostingsMergeQueue::merge(Sink && sink)
{
    chassert(buffer.empty());
    chassert(tfs_buffer.empty());

    if (active_cursors.size() == 1)
    {
        auto & cursor = *active_cursors.front();

        do
        {
            flushDirect(cursor.remaining(), cursor.remainingTfs(), sink);
        }
        while (task.advancePostingsCursor(cursor));

        task.readAndAppendPositions(cursor);
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
    has_term_frequencies = false;
}

template <typename Sink>
void MergeTextIndexesTask::mergePostings(Sink && sink)
{
    chassert(!postings_queue->isValid());

    for (const auto & source : current_token_sources)
    {
        if (source.info.cardinality != 0)
            postings_queue->push(source);
    }

    postings_queue->merge(sink);
}

TokenPostingsInfo MergeTextIndexesTask::flushRawPostings(MergeTreeIndexWriterStream & postings_stream, size_t total_cardinality)
{
    using enum PostingsSerialization::Flags;
    TokenPostingsInfo token_info;

    /// Raw postings are fewer than the encoder granularity, so the queue passes all of them in one chunk.
    mergePostings([&](std::span<const UInt32> row_ids, std::span<const UInt32> tf_minus_one)
    {
        if (token_info.cardinality != 0)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Raw postings must be passed in one chunk, got {} row ids after {}",
                row_ids.size(), token_info.cardinality);
        }

        token_info.cardinality = static_cast<UInt32>(row_ids.size());

        /// The term frequencies are written only if some source stores them, otherwise every `tf` is 1.
        if (postings_queue->hasTermFrequencies())
            token_info.header |= HasTermFrequencies;
        else
            tf_minus_one = {};

        /// Embedded postings (and their inline term frequencies) are serialized into the dictionary block by flushDictionaryBlock.
        if (row_ids.size() <= MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS)
        {
            token_info.header |= RawPostings | EmbeddedPostings;
            token_info.embedded_postings.assign(row_ids.begin(), row_ids.end());
            token_info.embedded_term_frequencies.assign(tf_minus_one.begin(), tf_minus_one.end());
        }
        else
        {
            token_info.header |= RawPostings | SingleBlock;
            token_info.offsets.emplace_back(postings_stream.plain_hashing.count());
            token_info.ranges.emplace_back(row_ids.front(), row_ids.back());

            /// Per-row `(tf - 1)` parallel to the row ids, written as VarUInts after them.
            TextIndexSerialization::serializeRawPostings(row_ids, tf_minus_one, postings_stream.plain_hashing);
        }
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

    const PostingListBuildContext context
    {
        .codec = *codec,
        .segment_size = params.posting_list_block_size,
        .enable_positions = params.enable_positions,
        .enable_scoring = params.hasScoring(),
        .doc_lengths = params.scoring == TextIndexScoringKind::BM25 ? &merged_doc_lengths : nullptr,
    };

    mergePostings([&](std::span<const UInt32> row_ids, std::span<const UInt32> tf_minus_one)
    {
        encoder->append(row_ids, tf_minus_one, context);
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

void MergeTextIndexesTask::readAndAppendPositions(const PostingsMergeCursor & cursor)
{
    const auto & source = *cursor.source;
    const auto & token_info = source.info;

    if (!params.enable_positions || !(token_info.header & PostingsSerialization::Flags::HasPositions))
        return;

    auto * stream = input_streams[source.source_num].at(MergeTreeIndexSubstream::Type::TextIndexPositions);
    auto * data_buffer = stream->getDataBuffer();

    /// Checked before seeking: an offset outside the stream would leave the buffer out of range.
    const size_t file_size = stream->getFileSize();
    if ((token_info.position_bytes == 0)
        || (token_info.position_offset > file_size)
        || (token_info.position_bytes > file_size - token_info.position_offset))
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Corrupt text index positions: blob of {} bytes at offset {} is outside the {}-byte stream",
            token_info.position_bytes, token_info.position_offset, file_size);
    }

    stream->seekToMark({token_info.position_offset, 0});

    /// The stream stores position lists per posting rank with no document ids, so it is paired with
    /// this token's row ids in pre-remap order, captured by the cursor while its postings were merged.
    position_entries_buffer.clear();
    decodeBlockedPositions(
        *data_buffer, cursor.token_row_ids, token_info.cardinality, token_info.position_bytes,
        blocked_decode_scratch, position_entries_buffer);

    /// Adjust doc_ids if merging parts with offset remapping.
    if (merged_part_offsets)
    {
        size_t part_index = segments[source.source_num].part_index;
        for (auto & entry : position_entries_buffer)
            entry = entry.withDocId(adjustPartOffset(*merged_part_offsets, part_index, entry.doc_id));
    }

    output_positions.insert(output_positions.end(), position_entries_buffer.begin(), position_entries_buffer.end());
}

void MergeTextIndexesTask::buildDocLengthsAndStats()
{
    merged_sum_doc_length = 0;
    merged_doc_lengths.clear();
    merged_doc_lengths.resize(num_rows);

    for (size_t source_num = 0; source_num < segments.size(); ++source_num)
    {
        auto * header_stream = input_streams[source_num].at(MergeTreeIndexSubstream::Type::Regular);
        header_stream->seekToStart();
        /// Only the scoring stats are needed here, so skip deserializing the sparse index.
        auto header = TextIndexSerialization::deserializeHeaderPrefix(*header_stream->getDataBuffer());
        merged_sum_doc_length += header.scoring_stats.sum_doc_length;

        auto doc_lengths_stream_it = input_streams[source_num].find(MergeTreeIndexSubstream::Type::TextIndexDocLengths);
        if (doc_lengths_stream_it == input_streams[source_num].end() || doc_lengths_stream_it->second == nullptr)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Text index merge: source segment '{}' of a scoring merge has no doc-lengths stream",
                segments[source_num].index_file_name);
        }

        auto * doc_lengths_stream = doc_lengths_stream_it->second;
        doc_lengths_stream->seekToStart();
        auto * doc_lengths_buffer = doc_lengths_stream->getDataBuffer();

        PaddedPODArray<UInt8> source_doc_lengths;
        while (!doc_lengths_buffer->eof())
        {
            size_t available = doc_lengths_buffer->available();
            size_t old_size = source_doc_lengths.size();
            source_doc_lengths.resize(old_size + available);
            doc_lengths_buffer->readStrict(reinterpret_cast<char *>(source_doc_lengths.data() + old_size), available);
        }

        const size_t doc_lengths_size = source_doc_lengths.size();
        if (doc_lengths_size == 0)
            continue;

        if (header.scoring_stats.num_docs < doc_lengths_size)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Text index merge: segment header num_docs ({}) is smaller than its doc-lengths size ({})",
                header.scoring_stats.num_docs, doc_lengths_size);
        }

        const UInt64 segment_base = header.scoring_stats.num_docs - doc_lengths_size;

        if (merged_part_offsets)
        {
            size_t part_index = segments[source_num].part_index;

            for (size_t i = 0; i < doc_lengths_size; ++i)
            {
                UInt32 new_offset = adjustPartOffset(*merged_part_offsets, part_index, static_cast<UInt32>(segment_base + i));
                merged_doc_lengths[new_offset] = source_doc_lengths[i];
            }
        }
        else
        {
            for (size_t i = 0; i < doc_lengths_size; ++i)
                merged_doc_lengths[segment_base + i] = source_doc_lengths[i];
        }
    }
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

    if (total_cardinality <= MAX_CARDINALITY_FOR_RAW_POSTINGS)
        token_info = flushRawPostings(*postings_stream, total_cardinality);
    else
        token_info = flushEncodedPostings(*postings_stream, total_cardinality);

    /// Serialize position data if positions are enabled.
    if (params.enable_positions && !output_positions.empty())
        flushPositions(token_info);

    output_infos.push_back(token_info);
    current_token_sources.clear();
    output_positions.clear();
}

void MergeTextIndexesTask::flushPositions(TokenPostingsInfo & token_info)
{
    auto * positions_stream = output_streams.at(MergeTreeIndexSubstream::Type::TextIndexPositions);

    /// Entries from multiple source parts may interleave after doc_id remapping.
    std::sort(output_positions.begin(), output_positions.end());

    size_t out = 0;
    for (size_t i = 1; i < output_positions.size(); ++i)
    {
        if (output_positions[out].sameBucket(output_positions[i]))
            output_positions[out].mergeBitmap(output_positions[i]);
        else
            output_positions[++out] = output_positions[i];
    }

    output_positions.resize(out + 1);

    token_info.header |= PostingsSerialization::Flags::HasPositions;
    token_info.position_offset = positions_stream->plain_hashing.count();
    TextIndexBlockedPositionsCodec::encode(output_positions, positions_stream->plain_hashing);
    token_info.position_bytes = positions_stream->plain_hashing.count() - token_info.position_offset;
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
        {
            /// The per-row `(tf - 1)`, non-empty only when the token has HasTermFrequencies.
            const auto & embedded = output_infos[i].embedded_postings;
            const auto & term_frequencies = output_infos[i].embedded_term_frequencies;
            chassert(term_frequencies.empty() || term_frequencies.size() == embedded.size());
            TextIndexSerialization::serializeRawPostings(embedded, term_frequencies, ostr);
        }
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

        /// On the scoring path, build the merged per-row document lengths and per-part collection
        /// statistics once, before token iteration. This reads the per-source `.dl` and `Regular`
        /// (header) streams, which are independent of the dictionary / postings cursors used below.
        if (params.scoring == TextIndexScoringKind::BM25)
            buildDocLengthsAndStats();
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

    TextIndexScoringStats scoring_stats;

    if (params.scoring == TextIndexScoringKind::BM25)
    {
        auto * doc_lengths_stream = output_streams.at(MergeTreeIndexSubstream::Type::TextIndexDocLengths);
        if (!doc_lengths_stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Merged text index with BM25 scoring is missing its document-lengths (.dl) output stream");

        /// One uncompressed byte per row; the marks of the merged part make it readable like a column.
        doc_lengths_stream->plain_hashing.write(reinterpret_cast<const char *>(merged_doc_lengths.data()), merged_doc_lengths.size());

        if (!merged_doc_lengths.empty())
        {
            chassert(new_data_part && index_granularity);
            writePerRowSubstreamMarks(*doc_lengths_stream, *index_granularity, new_data_part->index_granularity_info.mark_type.adaptive);
        }

        scoring_stats = TextIndexScoringStats
        {
            .num_docs = num_rows,
            .sum_doc_length = merged_sum_doc_length,
        };
    }

    TextIndexHeader header
    {
        .version = params.serialization_version,
        .codec_type = postings_serialization.getPostingListCodec()->getType(),
        .has_positions = params.enable_positions,
        .positions_codec = params.positions_codec,
        .scoring = params.scoring,
        .sparse_index = DictionarySparseIndex(std::move(sparse_index_tokens), std::move(sparse_index_offsets)),
        .scoring_stats = std::move(scoring_stats),
    };

    auto * index_stream = output_streams.at(MergeTreeIndexSubstream::Type::Regular);
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
    const String & stream_name,
    const String & extension,
    const MergeTreeReaderSettings & reader_settings)
{
    /// Mirrors IMergeTreeDataPart::getFileSizeOrZeroResolved: the on-disk name (original or hashed)
    /// comes from checksums, and a stream with no checksums entry is resolved and sized via the storage.
    auto data_part_storage = data_part_info.getDataPartStorage();
    std::optional<String> actual_stream_name
        = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, data_part_info.getChecksums());
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
        std::move(data_part_storage), *actual_stream_name, extension, data_file_size, reader_settings);
}

std::unique_ptr<MergeTreeReaderStream> makeTextIndexInputStream(
    DataPartStoragePtr data_part_storage,
    const String & stream_name,
    const String & extension,
    const MergeTreeReaderSettings & reader_settings)
{
    /// Check for both original and hashed filenames (hashed if the index name is too long)
    auto actual_stream_name = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, *data_part_storage);
    if (!actual_stream_name)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File for text index stream {} does not exist", stream_name + extension);

    return makeTextIndexInputStreamImpl(
        data_part_storage,
        *actual_stream_name,
        extension,
        data_part_storage->getFileSize(*actual_stream_name + extension),
        reader_settings);
}

}
