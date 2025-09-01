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

MergeTreeReaderTextIndex::MergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader_,
    MergeTreeIndexWithCondition index_)
    : MergeTreeReaderTextIndex(main_reader_, {}, {}, std::move(index_))
{
}

MergeTreeReaderTextIndex::MergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader_,
    NamesAndTypesList columns_,
    std::vector<TextSearchMode> search_modes_,
    MergeTreeIndexWithCondition index_)
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
    , search_modes(std::move(search_modes_))
    , index(std::move(index_))
{
    chassert(search_modes.size() == columns_.size());

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

bool MergeTreeReaderTextIndex::canSkipMark(size_t mark)
{
    chassert(index_reader);
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReaderTotalMicroseconds);

    size_t index_mark = mark / index.index->index.granularity;

    if (granules.empty() || index_mark != granules.rbegin()->first)
    {
        auto & granule = granules[index_mark];
        chassert(granule.granule == nullptr);

        index_reader->read(index_mark, index.condition.get(), granule.granule);
        granule.may_be_true = index.condition->mayBeTrueOnGranule(granule.granule);
        granule.need_read_postings = granule.may_be_true;

        auto & granule_text = assert_cast<MergeTreeIndexGranuleText &>(*granule.granule);
        granule_text.resetAfterAnalysis();
    }

    chassert(granules.rbegin()->first == index_mark);
    return !granules.rbegin()->second.may_be_true;
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
        return max_rows_to_read;
    }

    size_t read_rows = 0;
    createEmptyColumns(res_columns);

    while (read_rows < max_rows_to_read)
    {
        size_t rows_to_read = data_part_info_for_read->getIndexGranularity().getMarkRows(from_mark);
        size_t index_mark = from_mark / index.index->index.granularity;

        auto it = granules.find(index_mark);
        if (it == granules.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule not found (mark: {}, index_mark: {})", from_mark, index_mark);

        auto & granule = it->second;
        const auto & index_granularity = data_part_info_for_read->getIndexGranularity();

        if (!granule.may_be_true)
        {
            for (const auto & column : res_columns)
            {
                auto & column_data = assert_cast<ColumnUInt8 &>(column->assumeMutableRef()).getData();
                column_data.resize_fill(column->size() + rows_to_read, 0);
            }
        }
        else
        {
            readPostingsIfNeeded(granule);

            size_t mark_at_index_granule = index_mark * index.index->index.granularity;
            size_t granule_offset = index_granularity.getRowsCountInRange(mark_at_index_granule, from_mark);

            for (size_t i = 0; i < res_columns.size(); ++i)
            {
                auto & column_mutable = res_columns[i]->assumeMutableRef();
                fillColumn(column_mutable, granule, search_modes[i], granule_offset, rows_to_read);
            }
        }

        ++from_mark;
        read_rows += rows_to_read;
        current_row += rows_to_read;
    }

    size_t index_mark = from_mark / index.index->index.granularity;
    auto it = granules.lower_bound(index_mark);
    granules.erase(granules.begin(), it);

    current_mark = from_mark;
    return read_rows;
}

void MergeTreeReaderTextIndex::readPostingsIfNeeded(Granule & granule)
{
    if (!granule.need_read_postings)
        return;

    const auto & granule_text = assert_cast<const MergeTreeIndexGranuleText &>(*granule.granule);
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

        granule.postings[token] = std::move(postings_column);
    }

    granule.need_read_postings = false;
}

void applyPostingsAny(
    IColumn & column,
    const PostingsMap & postings_map,
    size_t column_offset,
    size_t granule_offset,
    size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();

    for (const auto & [_, postings_column] : postings_map)
    {
        const auto & postings_data = assert_cast<const ColumnUInt32 &>(*postings_column).getData();
        const auto * begin = std::lower_bound(postings_data.begin(), postings_data.end(), granule_offset);

        for (const auto * it = begin; it != postings_data.end(); ++it)
        {
            size_t relative_row_number = *it - granule_offset;

            if (relative_row_number >= num_rows)
                break;

            column_data[column_offset + relative_row_number] = 1;
        }
    }
}

void applyPostingsAll(
    IColumn & column,
    const PostingsMap & postings_map,
    size_t column_offset,
    size_t granule_offset,
    size_t num_rows)
{
    if (postings_map.size() > std::numeric_limits<UInt16>::max())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many tokens ({}) for All search mode", postings_map.size());

    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    PaddedPODArray<UInt16> counters(num_rows, 0);

    for (const auto & [_, postings_column] : postings_map)
    {
        const auto & postings_data = assert_cast<const ColumnUInt32 &>(*postings_column).getData();
        const auto * begin = std::lower_bound(postings_data.begin(), postings_data.end(), granule_offset);

        for (const auto * it = begin; it != postings_data.end(); ++it)
        {
            size_t relative_row_number = *it - granule_offset;

            if (relative_row_number >= num_rows)
                break;

            ++counters[relative_row_number];
        }
    }

    size_t total_tokens = postings_map.size();
    for (size_t i = 0; i < num_rows; ++i)
        column_data[column_offset + i] = static_cast<UInt8>(counters[i] == total_tokens);
}

void MergeTreeReaderTextIndex::fillColumn(IColumn & column, const Granule & granule, TextSearchMode search_mode, size_t granule_offset, size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    size_t old_size = column_data.size();
    column_data.resize_fill(old_size + num_rows, 0);

    if (granule.postings.empty())
        return;

    if (search_mode == TextSearchMode::Any || granule.postings.size() == 1)
        applyPostingsAny(column, granule.postings, old_size, granule_offset, num_rows);
    else if (search_mode == TextSearchMode::All)
        applyPostingsAll(column, granule.postings, old_size, granule_offset, num_rows);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid search mode: {}", search_mode);
}

}
