#include <sstream>
#include <Columns/ColumnsCommon.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnsNumber.h>

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

MergeTreeReaderTextIndex::MergeTreeReaderTextIndex(const IMergeTreeReader * main_reader_, NamesAndTypesList columns_, MergeTreeIndexWithCondition index_)
    : IMergeTreeReader(
        main_reader_->data_part_info_for_read,
        /*columns=*/ columns_,
        /*virtual_fields=*/ {},
        main_reader_->storage_snapshot,
        /*uncompressed_cache=*/ Context::getGlobalContextInstance()->getIndexUncompressedCache().get(),
        /*mark_cache=*/ Context::getGlobalContextInstance()->getIndexMarkCache().get(),
        main_reader_->all_mark_ranges,
        main_reader_->settings)
    , main_reader(main_reader_)
    , index(std::move(index_))
{
    for (const auto & column : columns_)
    {
        if (!column.name.starts_with(TEXT_INDEX_VIRTUAL_COLUMN_PREFIX) || !WhichDataType(column.type).isUInt8())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column {} with type {} should not be filled by text index reader",
                column.name, column.type->getName());
        }
    }

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
    if (continue_reading)
        from_mark = current_mark;

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReaderTotalMicroseconds);
    size_t index_start_mark = from_mark / index.index->index.granularity;

    /// Determine the starting row.
    size_t starting_row;
    if (continue_reading)
        starting_row = current_row + rows_offset;
    else
        starting_row = data_part_info_for_read->getIndexGranularity().getMarkStartingRow(from_mark) + rows_offset;

    /// Clamp max_rows_to_read.
    size_t total_rows = data_part_info_for_read->getIndexGranularity().getTotalRows();
    if (starting_row < total_rows)
    {
        max_rows_to_read = std::min(max_rows_to_read, total_rows - starting_row);
    }

    if (res_columns.empty())
    {
        current_row += max_rows_to_read;
        granule_offset += max_rows_to_read;
        return max_rows_to_read;
    }

    if (!granule)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule must be read before reading rows (from_mark: {}, index_start_mark: {})", from_mark, index_start_mark);
    }

    size_t read_rows = 0;

    while (read_rows < max_rows_to_read)
    {
        size_t rows_to_read = data_part_info_for_read->getIndexGranularity().getMarkRows(from_mark);

        if (canSkipMark(from_mark))
        {
            chassert(postings.empty());
            fillColumns(res_columns, rows_to_read);
        }
        else
        {
            if (need_read_postings)
            {
                readPostings();
                need_read_postings = false;
            }

            fillColumns(res_columns, rows_to_read);
        }

        ++from_mark;
        read_rows += rows_to_read;
        granule_offset += rows_to_read;
        current_row += rows_to_read;
    }

    current_mark = from_mark;
    return read_rows;
}

void MergeTreeReaderTextIndex::readPostings()
{
    chassert(granule);
    const auto & granule_text = assert_cast<const MergeTreeIndexGranuleText &>(*granule);
    const auto & remaining_tokens = granule_text.getRemainingTokens();

    for (const auto & [token, mark] : remaining_tokens)
    {
        auto * postings_stream = index_reader->getStreams().at(IndexSubstream::Type::TextIndexPostings);
        auto * data_buffer = postings_stream->getDataBuffer();
        auto * compressed_buffer = postings_stream->getCompressedDataBuffer();

        compressed_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);

        UInt32 total_tokens;
        readPODBinary(total_tokens, *data_buffer);

        auto postings_column = ColumnUInt32::create();
        auto & postings_data = postings_column->getData();
        postings_data.resize(total_tokens);

        size_t size = data_buffer->readBig(reinterpret_cast<char*>(postings_data.data()), sizeof(UInt32) * total_tokens);

        if (size != total_tokens * sizeof(UInt32))
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read postings lists for token: {}", token.toString());

        postings[token] = std::move(postings_column);
    }
}

template <TextSearchMode search_mode>
void applyPostings(
    IColumn & column,
    const IColumn & postings_column,
    size_t column_offset,
    size_t granule_offset,
    size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    const auto & postings_data = assert_cast<const ColumnUInt32 &>(postings_column).getData();
    const auto * begin = std::lower_bound(postings_data.begin(), postings_data.end(), granule_offset);

    for (const auto * it = begin; it != postings_data.end(); ++it)
    {
        size_t relative_row_number = *it - granule_offset;

        if (relative_row_number > num_rows)
            break;

        if constexpr (search_mode == TextSearchMode::Any)
            column_data[column_offset + relative_row_number] = 1;
        else
            column_data[column_offset + relative_row_number] &= 1;
    }
}

void MergeTreeReaderTextIndex::fillColumns(Columns & res_columns, size_t num_rows)
{
    for (size_t pos = 0; pos < res_columns.size(); ++pos)
    {
        const auto & column_to_read = columns_to_read[pos];

        if (!res_columns[pos])
            res_columns[pos] = column_to_read.type->createColumn(*serializations[pos]);

        auto & column_mutable = res_columns[pos]->assumeMutableRef();
        fillColumn(column_mutable, TextSearchMode::All, num_rows);
    }
}

void MergeTreeReaderTextIndex::fillColumn(IColumn & column, TextSearchMode search_mode, size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    size_t old_size = column_data.size();
    column_data.resize_fill(old_size + num_rows, 0);

    if (postings.empty())
        return;

    auto it = postings.begin();
    applyPostings<TextSearchMode::Any>(column, *it->second, old_size, granule_offset, num_rows);
    ++it;

    if (search_mode == TextSearchMode::All)
    {
        for (; it != postings.end(); ++it)
            applyPostings<TextSearchMode::All>(column, *it->second, old_size, granule_offset, num_rows);
    }
    else
    {
        for (; it != postings.end(); ++it)
            applyPostings<TextSearchMode::Any>(column, *it->second, old_size, granule_offset, num_rows);
    }
}

bool MergeTreeReaderTextIndex::canSkipMark(size_t mark)
{
    chassert(index_reader);
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReaderTotalMicroseconds);
    size_t index_start_mark = mark / index.index->index.granularity;

    if (!granule || index_start_mark != current_index_mark)
    {
        index_reader->read(index_start_mark, index.condition.get(), granule);
        may_be_true = index.condition->mayBeTrueOnGranule(granule);

        auto & granule_text = assert_cast<MergeTreeIndexGranuleText &>(*granule);
        granule_text.resetAfterAnalysis(may_be_true);

        postings.clear();
        granule_offset = 0;
        need_read_postings = true;
        current_index_mark = index_start_mark;
    }

    return !may_be_true;
}

}
