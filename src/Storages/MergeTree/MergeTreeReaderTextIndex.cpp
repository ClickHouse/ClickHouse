#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

MergeTreeReaderTextIndex::MergeTreeReaderTextIndex(const IMergeTreeReader * main_reader_, MergeTreeIndexWithCondition index_)
    : IMergeTreeReader(
          main_reader_->data_part_info_for_read,
          {},
          {},
          main_reader_->storage_snapshot,
          nullptr,
          nullptr,
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
