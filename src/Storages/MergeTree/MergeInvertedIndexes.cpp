#include <Processors/Port.h>
#include <Storages/MergeTree/MergeInvertedIndexes.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Compression/CompressionFactory.h>
#include <Parsers/parseQuery.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndicesSerialization.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsMilliseconds background_task_preferred_step_execution_time_ms;
}

namespace
{

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

void writeMarks(MergeTreeIndexOutputStreams & streams)
{
    for (const auto & [_, stream] : streams)
    {
        auto & marks_out = stream->compress_marks ? stream->marks_compressed_hashing : stream->marks_hashing;

        writeBinaryLittleEndian(stream->plain_hashing.count(), marks_out);
        writeBinaryLittleEndian(stream->compressed_hashing.offset(), marks_out);
        writeBinaryLittleEndian(1UL, marks_out);
    }
}

}

BuildInvertedIndexTransform::BuildInvertedIndexTransform(
    SharedHeader header,
    String index_file_prefix_,
    std::vector<MergeTreeIndexPtr> indexes_,
    MutableDataPartStoragePtr temporary_storage_,
    MergeTreeWriterSettings writer_settings_,
    CompressionCodecPtr default_codec_,
    String marks_file_extension_)
    : ISimpleTransform(header, header, false)
    , index_file_prefix(std::move(index_file_prefix_))
    , indexes(std::move(indexes_))
    , temporary_storage(std::move(temporary_storage_))
    , writer_settings(std::move(writer_settings_))
    , default_codec(std::move(default_codec_))
    , marks_file_extension(std::move(marks_file_extension_))
    , segment_numbers(indexes.size(), 0)
{
    for (const auto & index : indexes)
    {
        auto aggregator = index->createIndexAggregator();
        aggregators.push_back(std::move(aggregator));
    }
}

void BuildInvertedIndexTransform::transform(Chunk & chunk)
{
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
    aggregate(block);
}

IProcessor::Status BuildInvertedIndexTransform::prepare()
{
    auto status = ISimpleTransform::prepare();
    if (status == Status::Finished)
        finalize();
    return status;
}

void BuildInvertedIndexTransform::aggregate(const Block & block)
{
    static constexpr size_t max_processed_tokens = 100'000'000;
    num_processed_rows += block.rows();

    for (size_t i = 0; i < indexes.size(); ++i)
    {
        size_t pos = 0;
        auto & aggregator_text = typeid_cast<MergeTreeIndexAggregatorText &>(*aggregators[i]);
        aggregator_text.update(block, &pos, block.rows());

        if (aggregator_text.getNumProcessedTokens() > max_processed_tokens)
            writeTemporarySegment(i);
    }
}

void BuildInvertedIndexTransform::finalize()
{
    for (size_t i = 0; i < indexes.size(); ++i)
    {
        if (!aggregators[i]->empty())
            writeTemporarySegment(i);
    }
}

std::vector<InvertedIndexSegment> BuildInvertedIndexTransform::getSegments(size_t index_idx, size_t part_idx) const
{
    std::vector<InvertedIndexSegment> segments;

    for (size_t i = 0; i < segment_numbers[index_idx]; ++i)
    {
        auto index_file_name = fmt::format("{}_{}_{}", index_file_prefix, i, indexes[index_idx]->getFileName());
        segments.emplace_back(temporary_storage, std::move(index_file_name), part_idx);
    }

    return segments;
}

void BuildInvertedIndexTransform::writeTemporarySegment(size_t i)
{
    auto index_file_name = fmt::format("{}_{}_{}", index_file_prefix, segment_numbers[i]++, indexes[i]->getFileName());
    auto index_substreams = indexes[i]->getSubstreams();

    auto & aggregator_text = typeid_cast<MergeTreeIndexAggregatorText &>(*aggregators[i]);
    auto granule = aggregator_text.getGranuleAndReset();
    aggregator_text.setCurrentRow(num_processed_rows);

    auto [streams, streams_holders] = makeOutputStreams(
        index_substreams,
        index_file_name,
        temporary_storage,
        default_codec,
        marks_file_extension,
        writer_settings);

    writeMarks(streams);
    granule->serializeBinaryWithMultipleStreams(streams);

    for (auto & stream : streams_holders)
        stream->finalize();
}

MergeInvertedIndexesTask::MergeInvertedIndexesTask(
    std::vector<InvertedIndexSegment> segments_,
    MergeTreeMutableDataPartPtr new_data_part_,
    MergeTreeIndexPtr index_ptr_,
    std::shared_ptr<MergedPartOffsets> merged_part_offsets_,
    const MergeTreeReaderSettings & reader_settings_,
    const MergeTreeWriterSettings & writer_settings_)
    : segments(std::move(segments_))
    , new_data_part(std::move(new_data_part_))
    , index_ptr(std::move(index_ptr_))
    , merged_part_offsets(std::move(merged_part_offsets_))
    , writer_settings(writer_settings_)
    , step_time_ms((*new_data_part->storage.getSettings())[MergeTreeSetting::background_task_preferred_step_execution_time_ms].totalMilliseconds())
{
    init();
    auto substreams = index_ptr->getSubstreams();

    for (size_t i = 0; i < segments.size(); ++i)
    {
        for (const auto & substream : substreams)
        {
            auto stream = makeInvertedIndexInputStream(
                segments[i].part_storage,
                segments[i].index_file_name + substream.suffix,
                substream.extension,
                reader_settings_);

            input_streams[i][substream.type] = stream.get();
            input_streams_holders.emplace_back(std::move(stream));
        }
    }
}

MergeInvertedIndexesTask::~MergeInvertedIndexesTask() noexcept
{
    cancelImpl();
}

void MergeInvertedIndexesTask::init()
{
    cursors.resize(segments.size());
    inputs.resize(segments.size());
    input_streams.resize(segments.size());

    output_tokens = ColumnString::create();
    params = typeid_cast<const MergeTreeIndexText &>(*index_ptr).getParams();
    sparse_index_tokens = ColumnString::create();
    sparse_index_offsets = ColumnUInt64::create();

    std::tie(output_streams, output_streams_holders) = makeOutputStreams(
        index_ptr->getSubstreams(),
        index_ptr->getFileName(),
        new_data_part->getDataPartStoragePtr(),
        new_data_part->default_codec,
        new_data_part->getMarksFileExtension(),
        writer_settings);
}

Block MergeInvertedIndexesTask::getHeader() const
{
    return Block{ColumnWithTypeAndName{ColumnString::create(), std::make_shared<DataTypeString>(), "token"}};
}

void MergeInvertedIndexesTask::initializeQueue()
{
    SortDescription description;
    description.emplace_back("token");

    for (size_t source_num = 0; source_num < inputs.size(); ++source_num)
    {
        cursors[source_num] = SortCursorImpl(getHeader(), description, source_num);
        readDictionaryBlock(source_num);
    }
}

void MergeInvertedIndexesTask::readDictionaryBlock(size_t source_num)
{
    auto * stream = input_streams[source_num].at(MergeTreeIndexSubstream::Type::TextIndexDictionary);
    auto * data_buffer = stream->getDataBuffer();

    if (data_buffer->eof())
        return;

    inputs[source_num] = MergeTreeIndexGranuleText::deserializeDictionaryBlock(*data_buffer);
    const auto & tokens = inputs[source_num].tokens;
    cursors[source_num].reset({tokens}, getHeader(), tokens->size());
    queue.push(cursors[source_num]);
}

std::vector<PostingListPtr> MergeInvertedIndexesTask::readPostingLists(size_t source_num)
{
    auto * stream = input_streams[source_num].at(MergeTreeIndexSubstream::Type::TextIndexPostings);
    auto * data_buffer = stream->getDataBuffer();
    auto * compressed_buffer = stream->getCompressedDataBuffer();

    const auto & token_info = inputs[source_num].token_infos[queue.current()->getRow()];
    std::vector<PostingListPtr> postings;

    for (const auto offset_in_file : token_info.offsets)
    {
        compressed_buffer->seek(offset_in_file, 0);
        postings.emplace_back(PostingsSerialization::deserialize(*data_buffer));
    }

    return postings;
}

PostingListPtr MergeInvertedIndexesTask::adjustPartOffsets(size_t source_num, PostingListPtr posting_list)
{
    if (!merged_part_offsets)
        return posting_list;

    std::vector<UInt32> offsets(posting_list->cardinality());
    posting_list->toUint32Array(offsets.data());
    size_t part_index = segments[source_num].part_index;

    for (auto & offset : offsets)
        offset = (*merged_part_offsets)[part_index, offset];

    return std::make_shared<PostingList>(offsets.size(), offsets.data());
}

void MergeInvertedIndexesTask::flushPostingList()
{
    auto * postings_stream = output_streams.at(MergeTreeIndexSubstream::Type::TextIndexPostings);

    PostingListBuilder builder(&output_postings);
    auto token_info = TextIndexSerialization::serializePostings(std::move(builder), *postings_stream, params.posting_list_block_size);
    output_infos.push_back(token_info);
    output_postings.clear();
}

void MergeInvertedIndexesTask::flushDictionaryBlock()
{
    if (output_tokens->size() != output_infos.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tokens size ({}) doesn't match infos size ({})", output_tokens->size(), output_infos.size());

    if (output_infos.empty())
        return;

    auto tokens_format = params.dictionary_block_frontcoding_compression
        ? TextIndexSerialization::TokensFormat::FrontCodedStrings
        : TextIndexSerialization::TokensFormat::RawStrings;

    size_t num_tokens = output_tokens->size();
    auto & output_str = assert_cast<ColumnString &>(*output_tokens);
    auto * dictionary_stream = output_streams.at(MergeTreeIndexSubstream::Type::TextIndexDictionary);
    auto & ostr = dictionary_stream->compressed_hashing;

    ostr.next();
    auto current_mark = dictionary_stream->getCurrentMark();
    chassert(current_mark.offset_in_decompressed_block == 0);

    auto first_token = output_str.getDataAt(0);
    assert_cast<ColumnString &>(*sparse_index_tokens).insertData(first_token.data(), first_token.size());
    assert_cast<ColumnUInt64 &>(*sparse_index_offsets).insertValue(current_mark.offset_in_compressed_file);

    TextIndexSerialization::serializeTokens(output_str, ostr, tokens_format);

    for (size_t i = 0; i < num_tokens; ++i)
        TextIndexSerialization::serializeTokenInfo(ostr, output_infos[i]);

    output_tokens = ColumnString::create();
    output_postings.clear();
    output_infos.clear();
}

bool MergeInvertedIndexesTask::isNewToken(const SortCursor & cursor) const
{
    const auto & input_str = assert_cast<const ColumnString &>(*inputs[cursor->order].tokens);
    const auto & output_str = assert_cast<const ColumnString &>(*output_tokens);

    return output_str.empty() || input_str.compareAt(cursor->getRow(), output_str.size() - 1, output_str, 1) != 0;
}

bool MergeInvertedIndexesTask::executeStep()
{
    if (!is_initialized)
    {
        is_initialized = true;
        initializeQueue();
        writeMarks(output_streams);
    }

    if (!queue.isValid())
    {
        finalize();
        return false;
    }

    Stopwatch watch(CLOCK_MONOTONIC_COARSE);

    do
    {
        SortCursor current = queue.current();

        if (isNewToken(current))
        {
            if (!output_postings.isEmpty())
                flushPostingList();

            if (output_tokens->size() >= params.dictionary_block_size)
                flushDictionaryBlock();

            output_tokens->insertFrom(*inputs[current->order].tokens, current->getRow());
        }

        auto read_postings = readPostingLists(current->order);

        for (auto & posting : read_postings)
        {
            posting = adjustPartOffsets(current->order, posting);
            output_postings |= *posting;
        }

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            queue.removeTop();
            readDictionaryBlock(current->order);
        }
    } while (queue.isValid() && watch.elapsedMilliseconds() < step_time_ms);

    return true;
}

void MergeInvertedIndexesTask::finalize()
{
    if (!output_postings.isEmpty())
        flushPostingList();

    if (!output_tokens->empty())
        flushDictionaryBlock();

    auto * index_stream = output_streams.at(MergeTreeIndexSubstream::Type::Regular);
    DictionarySparseIndex sparse_index(std::move(sparse_index_tokens), std::move(sparse_index_offsets));
    TextIndexSerialization::serializeSparseIndex(sparse_index, index_stream->compressed_hashing);

    for (auto & stream : output_streams_holders)
        stream->finalize();
}

void MergeInvertedIndexesTask::cancel() noexcept
{
    cancelImpl();
}

void MergeInvertedIndexesTask::cancelImpl() noexcept
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

void MergeInvertedIndexesTask::addToChecksums(MergeTreeDataPartChecksums & checksums)
{
    for (auto & stream : output_streams_holders)
        stream->addToChecksums(checksums);
}

MutableDataPartStoragePtr createTemporaryInvertedIndexStorage(const DiskPtr & disk, const String & part_relative_path)
{
    static constexpr const char * temp_part_dir = "inverted_index_tmp";
    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_relative_path + "_" + temp_part_dir, disk, 0);
    auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, part_relative_path, temp_part_dir);
    storage->beginTransaction();
    storage->createDirectories();
    return storage;
}

std::vector<MergeTreeIndexPtr> getInvertedIndexesToBuild(
    const IndicesDescription & indices_description,
    const NameSet & read_column_names,
    const IMergeTreeDataPart & data_part,
    bool merge_may_reduce_rows)
{
    std::vector<MergeTreeIndexPtr> indexes;

    for (const auto & index : indices_description)
    {
        if (index.column_names.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Inverted index must have one input column, got {}", index.column_names.size());

        if (!read_column_names.contains(index.column_names[0]))
            continue;

        auto index_ptr = MergeTreeIndexFactory::instance().get(index);
        if (merge_may_reduce_rows || !index_ptr->getDeserializedFormat(data_part.checksums, index_ptr->getFileName()))
            indexes.push_back(std::move(index_ptr));
    }

    return indexes;
}

std::unique_ptr<MergeTreeReaderStream> makeInvertedIndexInputStream(
    DataPartStoragePtr data_part_storage,
    const String & stream_name,
    const String & extension,
    const MergeTreeReaderSettings & reader_settings)
{
    static constexpr size_t marks_count = 1;

    return std::make_unique<MergeTreeReaderStreamSingleColumnWholePart>(
        data_part_storage,
        stream_name,
        extension,
        marks_count,
        MarkRanges{{0, marks_count}},
        reader_settings,
        /*uncompressed_cache=*/ nullptr,
        data_part_storage->getFileSize(stream_name + extension),
        /*marks_loader=*/ nullptr,
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);
}

}
