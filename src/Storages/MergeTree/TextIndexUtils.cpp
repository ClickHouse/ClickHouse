#include <Processors/Port.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
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

#include <limits>

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

    return PostingsSerialization(std::move(codec_copy), text_index.getParams().serialization_version);
}

static PostingsSerialization createSourcePostingsSerialization(MergeTreeIndexReaderStream & header_stream)
{
    header_stream.seekToStart();
    /// Only the version and codec are needed here, so skip deserializing the sparse index.
    auto header = TextIndexSerialization::deserializeHeaderPrefix(*header_stream.getDataBuffer());
    return PostingsSerialization(PostingListCodecFactory::createPostingListCodec(header.codec_type), header.version);
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
    , postings_serialization(createPostingsSerialization(*index_ptr))
{
    cursors.resize(segments.size());
    inputs.resize(segments.size());
    input_streams.resize(segments.size());

    SortDescription postings_sort_description;
    postings_sort_description.emplace_back("row_id");

    /// The sort cursor of every postings cursor is built once and points at the cursor's
    /// own column; a segment refill only rewinds it (see resetToColumnStart).
    postings_merge_cursors.resize(segments.size());
    for (size_t i = 0; i < postings_merge_cursors.size(); ++i)
    {
        auto & cursor = postings_merge_cursors[i];
        cursor.column = ColumnUInt32::create();
        Block postings_header{ColumnWithTypeAndName{cursor.column->getPtr(), std::make_shared<DataTypeUInt32>(), "row_id"}};
        cursor.impl = SortCursorImpl(postings_header, postings_sort_description, i);
    }

    output_tokens = ColumnString::create();

    const auto & text_index = typeid_cast<const MergeTreeIndexText &>(*index_ptr);
    params = text_index.getParams();
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

    /// Resolve each source part's codec from its own header.
    source_postings_serializations.reserve(segments.size());

    for (size_t i = 0; i < segments.size(); ++i)
    {
        auto * stream = input_streams[i].at(MergeTreeIndexSubstream::Type::Regular);
        source_postings_serializations.emplace_back(createSourcePostingsSerialization(*stream));
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

void MergeTextIndexesTask::initializeQueue()
{
    SortDescription description;
    description.emplace_back("token");

    for (size_t source_num = 0; source_num < inputs.size(); ++source_num)
    {
        cursors[source_num] = SortCursorImpl(getHeader(), description, source_num);
        readDictionaryBlock(source_num);
    }
}

void MergeTextIndexesTask::readDictionaryBlock(size_t source_num)
{
    auto * stream = input_streams[source_num].at(MergeTreeIndexSubstream::Type::TextIndexDictionary);
    auto * data_buffer = stream->getDataBuffer();

    if (data_buffer->eof())
        return;

    inputs[source_num] = TextIndexSerialization::deserializeDictionaryBlock(*data_buffer);
    const auto & tokens = inputs[source_num].tokens;
    cursors[source_num].reset({tokens}, getHeader(), tokens->size());
    queue.push(cursors[source_num]);
}

UInt32 MergeTextIndexesTask::adjustPartOffset(size_t part_index, UInt32 row_id) const
{
    chassert(merged_part_offsets);
    UInt64 new_offset = (*merged_part_offsets)[part_index, row_id];

    if (new_offset > std::numeric_limits<UInt32>::max())
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Cannot merge text index: remapped row id {} exceeds the maximum supported row id {}",
            new_offset, std::numeric_limits<UInt32>::max());
    }

    return static_cast<UInt32>(new_offset);
}

void MergeTextIndexesTask::adjustPartOffsets(std::span<UInt32> row_ids, size_t part_index) const
{
    if (!merged_part_offsets)
        return;

    for (UInt32 & row_id : row_ids)
        row_id = adjustPartOffset(part_index, row_id);
}

void MergeTextIndexesTask::initCursor(PostingsMergeCursor & cursor, const TokenSource & source)
{
    cursor.source = &source;
    cursor.next_segment = 0;

    const auto & info = source.info;

    if (!info.embedded_postings.empty())
    {
        auto & row_ids = cursor.rowIds();
        row_ids.assign(info.embedded_postings.begin(), info.embedded_postings.end());
        adjustPartOffsets({row_ids.data(), row_ids.size()}, segments[source.source_num].part_index);
        cursor.next_segment = info.offsets.size();
        cursor.resetToColumnStart();
    }
    else
    {
        bool refilled = advanceCursorSegment(cursor);
        chassert(refilled);
    }
}

bool MergeTextIndexesTask::advanceCursorSegment(PostingsMergeCursor & cursor)
{
    const auto & info = cursor.source->info;
    if (cursor.next_segment == info.offsets.size())
        return false;

    size_t source_num = cursor.source->source_num;
    auto * stream = input_streams[source_num].at(MergeTreeIndexSubstream::Type::TextIndexPostings);
    stream->seekToMark({info.offsets[cursor.next_segment], 0});

    auto & row_ids = cursor.rowIds();
    row_ids.clear();
    source_postings_serializations[source_num].deserializeToArray(*stream->getDataBuffer(), info.header, info.cardinality, row_ids);
    adjustPartOffsets({row_ids.data(), row_ids.size()}, segments[source_num].part_index);
    ++cursor.next_segment;
    cursor.resetToColumnStart();

    chassert(!row_ids.empty());
    chassert(std::is_sorted(row_ids.begin(), row_ids.end()));
    return true;
}

template <typename Sink>
void MergeTextIndexesTask::mergePostings(Sink && sink)
{
    chassert(!postings_queue.isValid());
    size_t num_cursors = 0;

    for (const auto & source : output_sources)
    {
        if (source.info.cardinality == 0)
            continue;

        auto & cursor = postings_merge_cursors[num_cursors++];
        initCursor(cursor, source);
    }

    if (num_cursors == 1)
    {
        auto & cursor = postings_merge_cursors.front();

        do
        {
            const auto & row_ids = cursor.rowIds();
            sink(std::span<const UInt32>(row_ids.data(), row_ids.size()));
        }
        while (advanceCursorSegment(cursor));

        return;
    }

    for (size_t i = 0; i < num_cursors; ++i)
        postings_queue.push(postings_merge_cursors[i].impl);

    UInt64 last_row_id_watermark = 0;

    while (postings_queue.isValid())
    {
        auto [current_ptr, batch_size] = postings_queue.current();
        PostingsSortCursor & current = *current_ptr;
        auto & cursor = postings_merge_cursors[current->order];

        const UInt32 * begin = cursor.rowIds().data() + current->getPos();

        /// Sources must own disjoint row sets.
        if (begin[0] < last_row_id_watermark)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Source posting lists have overlapping row ids: got row id {} after {}",
                begin[0], last_row_id_watermark - 1);
        }

        last_row_id_watermark = static_cast<UInt64>(begin[batch_size - 1]) + 1;
        sink(std::span<const UInt32>(begin, batch_size));

        if (!current->isLast(batch_size))
        {
            postings_queue.next(batch_size);
        }
        else
        {
            /// The segment is exhausted: load the source's next one or drop the cursor.
            postings_queue.removeTop();

            if (advanceCursorSegment(cursor))
                postings_queue.push(cursor.impl);
        }
    }
}

TokenPostingsInfo MergeTextIndexesTask::flushRawPostings(MergeTreeIndexWriterStream & postings_stream, size_t total_cardinality)
{
    using enum PostingsSerialization::Flags;
    output_postings_buffer.clear();
    output_postings_buffer.reserve(total_cardinality);

    mergePostings([&](std::span<const UInt32> row_ids)
    {
        output_postings_buffer.insert(row_ids.begin(), row_ids.end());
    });

    TokenPostingsInfo token_info;
    token_info.cardinality = static_cast<UInt32>(output_postings_buffer.size());

    if (token_info.cardinality <= MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS)
    {
        /// Embedded postings are serialized into the dictionary block by flushDictionaryBlock.
        token_info.header = RawPostings | EmbeddedPostings;
        token_info.embedded_postings.assign(output_postings_buffer.begin(), output_postings_buffer.end());
    }
    else
    {
        token_info.header = RawPostings | SingleBlock;
        token_info.offsets.emplace_back(postings_stream.plain_hashing.count());
        token_info.ranges.emplace_back(output_postings_buffer.front(), output_postings_buffer.back());
        TextIndexSerialization::serializeRawPostings({output_postings_buffer.data(), output_postings_buffer.size()}, postings_stream.plain_hashing);
    }

    return token_info;
}

TokenPostingsInfo MergeTextIndexesTask::flushEncodedPostings(MergeTreeIndexWriterStream & postings_stream, size_t total_cardinality)
{
    const auto * codec = postings_serialization.getPostingListCodec();
    size_t segment_size = codec->getSegmentSize(params.posting_list_block_size);
    auto encoder = codec->createEncoder();
    constexpr size_t max_buffered_size = IPostingListEncoder::append_granularity * 16;

    output_postings_buffer.clear();
    output_postings_buffer.reserve(max_buffered_size);

    mergePostings([&](std::span<const UInt32> row_ids)
    {
        /// A granularity-aligned chunk arriving on an empty buffer (typically a whole
        /// segment of the only or a disjoint source) goes to the encoder directly, without staging.
        if (output_postings_buffer.empty() && row_ids.size() % IPostingListEncoder::append_granularity == 0)
        {
            encoder->append(row_ids, segment_size);
            return;
        }

        output_postings_buffer.insert(row_ids.begin(), row_ids.end());

        if (output_postings_buffer.size() >= max_buffered_size)
        {
            size_t count = output_postings_buffer.size() - output_postings_buffer.size() % IPostingListEncoder::append_granularity;
            encoder->append({output_postings_buffer.data(), count}, segment_size);
            output_postings_buffer.erase(output_postings_buffer.begin(), output_postings_buffer.begin() + count);
        }
    });

    if (!output_postings_buffer.empty())
    {
        encoder->append({output_postings_buffer.data(), output_postings_buffer.size()}, segment_size);
    }

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

void MergeTextIndexesTask::readAndAppendPositions(size_t source_num, TokenPostingsInfo & token_info)
{
    auto * stream = input_streams[source_num].at(MergeTreeIndexSubstream::Type::TextIndexPositions);
    auto * data_buffer = stream->getDataBuffer();
    stream->seekToMark({token_info.position_offset, 0});

    position_entries_buffer.clear();
    TextIndexPositionCodec::decode(*data_buffer, position_entries_buffer);

    /// Adjust doc_ids if merging parts with offset remapping.
    if (merged_part_offsets)
    {
        size_t part_index = segments[source_num].part_index;
        for (auto & entry : position_entries_buffer)
            entry = entry.withDocId(adjustPartOffset(part_index, entry.doc_id));
    }

    output_positions.insert(output_positions.end(), position_entries_buffer.begin(), position_entries_buffer.end());
}

void MergeTextIndexesTask::flushPostingList()
{
    chassert(!output_sources.empty());

    auto * postings_stream = output_streams.at(MergeTreeIndexSubstream::Type::TextIndexPostings);
    TokenPostingsInfo token_info;

    /// Sources own disjoint row sets, so the cardinality of the merged posting is sum of source cardinalities.
    size_t total_cardinality = 0;
    for (const auto & source : output_sources)
        total_cardinality += source.info.cardinality;

    if (total_cardinality <= MAX_CARDINALITY_FOR_RAW_POSTINGS)
        token_info = flushRawPostings(*postings_stream, total_cardinality);
    else
        token_info = flushEncodedPostings(*postings_stream, total_cardinality);

    /// Serialize position data if positions are enabled.
    if (params.positions && !output_positions.empty())
        flushPositions(token_info);

    output_infos.push_back(token_info);
    output_sources.clear();
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
    token_info.position_cardinality = static_cast<UInt32>(output_positions.size());

    TextIndexPositionCodec::encode(output_positions, positions_stream->plain_hashing);
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
        initializeQueue();

        /// Write marks for compatibility with other skip indexes.
        /// An empty part carries no marks at all, exactly like every other skip index on an empty part.
        if (num_rows != 0)
        {
            chassert(new_data_part);
            bool can_use_adaptive_granularity = new_data_part->index_granularity_info.mark_type.adaptive;
            writeMarks(output_streams, can_use_adaptive_granularity);
        }
    }

    if (!queue.isValid())
    {
        finalize();
        return false;
    }

    Stopwatch watch(CLOCK_MONOTONIC_COARSE);

    do
    {
        auto [current_ptr, batch_size] = queue.current();
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
                if (!output_sources.empty())
                    flushPostingList();

                if (output_tokens->size() >= params.dictionary_block_size)
                    flushDictionaryBlock();

                auto & output_tokens_str = assert_cast<ColumnString &>(*output_tokens);
                output_tokens_str.insertFrom(*source_block.tokens, row);
            }

            /// Postings are decoded lazily on flush.
            /// Copy the info because the dictionary block it points into may be replaced before that.
            auto & token_info = source_block.token_infos[row];
            output_sources.push_back({source_num, token_info});

            if (params.positions && (token_info.header & PostingsSerialization::Flags::HasPositions))
                readAndAppendPositions(source_num, token_info);
        }

        if (!current->isLast(batch_size))
        {
            queue.next(batch_size);
        }
        else
        {
            queue.removeTop();
            readDictionaryBlock(source_num);
        }
    } while (queue.isValid() && watch.elapsedMilliseconds() < step_time_ms);

    return true;
}

void MergeTextIndexesTask::finalize()
{
    if (!output_sources.empty())
        flushPostingList();

    if (!output_tokens->empty())
        flushDictionaryBlock();

    auto * index_stream = output_streams.at(MergeTreeIndexSubstream::Type::Regular);

    TextIndexHeader header
    {
        .version = params.serialization_version,
        .codec_type = postings_serialization.getPostingListCodec()->getType(),
        .has_positions = params.positions != 0,
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

std::unique_ptr<MergeTreeReaderStream> makeTextIndexInputStream(
    DataPartStoragePtr data_part_storage,
    const String & stream_name,
    const String & extension,
    const MergeTreeReaderSettings & reader_settings)
{
    static constexpr size_t marks_count = 1;

    /// Check for both original and hashed filenames (hashed if the index name is too long)
    auto actual_stream_name = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, *data_part_storage);
    if (!actual_stream_name)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File for text index stream {} does not exist", stream_name + extension);

    /// Use reader stream that doesn't read marks,
    /// because text index always has one mark.
    return std::make_unique<MergeTreeReaderStreamSingleColumnWholePart>(
        data_part_storage,
        *actual_stream_name,
        extension,
        marks_count,
        MarkRanges{{0, marks_count}},
        reader_settings,
        /*uncompressed_cache=*/ nullptr,
        data_part_storage->getFileSize(*actual_stream_name + extension),
        /*marks_loader=*/ nullptr,
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);
}

}
