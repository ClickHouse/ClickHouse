#include <sstream>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event TextIndexReaderTotalMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_READ_ALL_DATA;
}

MergeTreeReaderTextIndex::MergeTreeReaderTextIndex(const IMergeTreeReader * main_reader_, MergeTreeIndexWithCondition index_)
    : IMergeTreeReader(
          main_reader_->data_part_info_for_read,
          /*columns=*/ {},
          /*virtual_fields=*/ {},
          main_reader_->storage_snapshot,
          /*uncompressed_cache=*/ Context::getGlobalContextInstance()->getIndexUncompressedCache().get(),
          /*mark_cache=*/ Context::getGlobalContextInstance()->getIndexMarkCache().get(),
          main_reader_->all_mark_ranges,
          main_reader_->settings)
    , main_reader(main_reader_)
    , index(std::move(index_))
{
    size_t index_granularity = index.index->index.granularity;

    for (const auto & range : all_mark_ranges)
    {
        MarkRange index_range(
            range.begin / index_granularity,
            (range.end + index_granularity - 1) / index_granularity);

        index_ranges.push_back(index_range);
    }

    const auto * loaded_data_part = typeid_cast<const LoadedMergeTreeDataPartInfoForReader *>(main_reader->data_part_info_for_read.get());
    if (!loaded_data_part)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading text index is supported only for loaded data parts");

    const auto & data_part = loaded_data_part->getDataPart();
    size_t marks_count = data_part->index_granularity->getMarksCountWithoutFinal();
    size_t index_marks_count = (marks_count + index_granularity - 1) / index_granularity;

    index_reader.emplace(
        index.index,
        data_part,
        index_marks_count,
        index_ranges,
        mark_cache,
        uncompressed_cache,
        /*vector_similarity_index_cache=*/ nullptr,
        settings);
}

size_t MergeTreeReaderTextIndex::readRows(
    size_t from_mark,
    size_t /* current_task_last_mark */,
    bool continue_reading,
    size_t max_rows_to_read,
    size_t rows_offset,
    Columns & res_columns)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReaderTotalMicroseconds);
    size_t index_mark = from_mark / index.index->index.granularity;

    auto it = cached_granules.find(index_mark);
    if (it == cached_granules.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule not found for index mark: {}", index_mark);

    const auto & granule_text = assert_cast<const MergeTreeIndexGranuleText &>(*it->second.granule);
    const auto & remaining_tokens = granule_text.getRemainingTokens();

    PaddedPODArray<UInt32> postings_buffer;

    for (const auto & [token, mark] : remaining_tokens)
    {
        LOG_DEBUG(getLogger("KEK"), "token: {}, mark: ({}, {})", token.toString(), mark.offset_in_compressed_file, mark.offset_in_decompressed_block);

        auto * postings_stream = index_reader->getStreams().at(IndexSubstream::Type::TextIndexPostings);
        auto * data_buffer = postings_stream->getDataBuffer();
        auto * compressed_buffer = postings_stream->getCompressedDataBuffer();

        compressed_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);

        UInt32 total_tokens;
        readPODBinary(total_tokens, *data_buffer);
        postings_buffer.resize(total_tokens);

        LOG_DEBUG(getLogger("KEK"), "total_tokens: {}", total_tokens);

        size_t size = data_buffer->readBig(reinterpret_cast<char*>(postings_buffer.data()), sizeof(UInt32) * total_tokens);

        if (size != total_tokens * sizeof(UInt32))
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read postings lists for token: {}", token.toString());

        LOG_DEBUG(getLogger("KEK"), "postings_buffer.size: {}", postings_buffer.size());

        {
            std::stringstream ss;
            for (size_t i = 0; i < postings_buffer.size(); ++i)
                ss << postings_buffer[i] << " ";
            LOG_DEBUG(getLogger("KEK"), "postings_buffer: {}", ss.str());
        }
    }

    if (!res_columns.empty())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Invalid number of columns passed to MergeTreeReaderTextIndex::readRows. "
            "Expected 0, got {}",
            res_columns.size());
    }

    /// Determine the starting row.
    size_t starting_row;
    if (continue_reading)
        starting_row = current_row + rows_offset;
    else
        starting_row = data_part_info_for_read->getIndexGranularity().getMarkStartingRow(from_mark) + rows_offset;

    /// Clamp max_rows_to_read.
    size_t total_rows = data_part_info_for_read->getIndexGranularity().getTotalRows();
    if (starting_row < total_rows)
        max_rows_to_read = std::min(max_rows_to_read, total_rows - starting_row);

    current_row += max_rows_to_read;
    return max_rows_to_read;
}

bool MergeTreeReaderTextIndex::canSkipMark(size_t mark)
{
    chassert(index_reader);
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReaderTotalMicroseconds);

    size_t index_mark = mark / index.index->index.granularity;
    auto & granule = cached_granules[index_mark];

    if (!granule.granule)
    {
        index_reader->read(index_mark, index.condition.get(), granule.granule);
        granule.may_be_true = index.condition->mayBeTrueOnGranule(granule.granule);

        auto & granule_text = assert_cast<MergeTreeIndexGranuleText &>(*granule.granule);
        granule_text.resetAfterAnalysis(granule.may_be_true);
    }

    return !granule.may_be_true;
}

}
