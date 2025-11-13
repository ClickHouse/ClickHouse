#include <Columns/ColumnsCommon.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/MergeTree/TextIndexCache.h>

namespace ProfileEvents
{
    extern const Event TextIndexReaderTotalMicroseconds;
    extern const Event TextIndexReadPostings;
    extern const Event TextIndexUsedEmbeddedPostings;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

MergeTreeReaderTextIndex::MergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader_,
    MergeTreeIndexWithCondition index_,
    NamesAndTypesList columns_)
    : IMergeTreeReader(
        main_reader_->data_part_info_for_read,
        columns_,
        /*virtual_fields=*/ {},
        main_reader_->storage_snapshot,
        main_reader_->storage_settings,
        Context::getGlobalContextInstance()->getIndexUncompressedCache().get(),
        Context::getGlobalContextInstance()->getIndexMarkCache().get(),
        main_reader_->all_mark_ranges,
        main_reader_->settings)
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

    updateAllIndexRanges();

    const auto * loaded_data_part = typeid_cast<const LoadedMergeTreeDataPartInfoForReader *>(data_part_info_for_read.get());
    if (!loaded_data_part)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading text index is supported only for loaded data parts");

    const auto & data_part = loaded_data_part->getDataPart();
    size_t marks_count = data_part->index_granularity->getMarksCountWithoutFinal();
    size_t index_marks_count = (marks_count + index.index->index.granularity - 1) / index.index->index.granularity;

    index_reader.emplace(
        index.index,
        data_part,
        index_marks_count,
        all_index_ranges,
        mark_cache,
        uncompressed_cache,
        /*vector_similarity_index_cache=*/ nullptr,
        settings);
}

void MergeTreeReaderTextIndex::updateAllMarkRanges(const MarkRanges & ranges)
{
    IMergeTreeReader::updateAllMarkRanges(ranges);
    updateAllIndexRanges();
}

void MergeTreeReaderTextIndex::updateAllIndexRanges()
{
    all_index_ranges.clear();
    size_t granularity = index.index->index.granularity;

    for (const auto & range : all_mark_ranges)
    {
        for (size_t i = range.begin; i < range.end; ++i)
        {
            size_t index_mark = i / granularity;
            remaining_marks[index_mark].increment();
        }

        MarkRange index_range(
            range.begin / granularity,
            (range.end + granularity - 1) / granularity);

        all_index_ranges.push_back(index_range);
    }
}

void MergeTreeReaderTextIndex::prefetchBeginOfRange(Priority priority)
{
    if (all_index_ranges.empty())
        return;

    size_t from_mark = all_index_ranges.front().begin;
    size_t to_mark = all_index_ranges.back().end;

    index_reader->adjustRightMark(to_mark);
    index_reader->prefetchBeginOfRange(from_mark, priority);
}

bool MergeTreeReaderTextIndex::canSkipMark(size_t mark, size_t current_task_last_mark)
{
    chassert(index_reader);
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReaderTotalMicroseconds);

    size_t granularity = index.index->index.granularity;
    size_t index_mark = mark / granularity;
    size_t index_last_mark = (current_task_last_mark + granularity - 1) / granularity;

    auto [it, inserted] = granules.try_emplace(index_mark);

    if (inserted)
    {
        if (remaining_marks.at(index_mark).finished(granularity))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule already finished and removed (mark: {}, index_mark: {})", mark, index_mark);

        auto & granule = it->second;
        chassert(granule.granule == nullptr);

        index_reader->adjustRightMark(index_last_mark);
        index_reader->read(index_mark, index.condition.get(), granule.granule);
        granule.may_be_true = index.condition->mayBeTrueOnGranule(granule.granule);
        granule.need_read_postings = granule.may_be_true;

        auto & granule_text = assert_cast<MergeTreeIndexGranuleText &>(*granule.granule);
        granule_text.resetAfterAnalysis();
        analyzed_granules.add(index_mark);
    }

    return !it->second.may_be_true;
}

size_t MergeTreeReaderTextIndex::readRows(
    size_t from_mark,
    size_t current_task_last_mark,
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
    max_rows_to_read = std::min(max_rows_to_read, data_part_info_for_read->getRowCount());

    if (res_columns.empty())
    {
        current_row += max_rows_to_read;
        return max_rows_to_read;
    }

    size_t read_rows = 0;
    createEmptyColumns(res_columns);
    size_t granularity = index.index->index.granularity;

    while (read_rows < max_rows_to_read)
    {
        size_t index_mark = from_mark / granularity;
        /// When the number of rows in a part is smaller than `index_granularity`,
        /// `MergeTreeReaderTextIndex` must ensure that the virtual column it reads
        /// contains no more data rows than actually exist in the part
        size_t rows_to_read = std::min(data_part_info_for_read->getIndexGranularity().getMarkRows(from_mark), data_part_info_for_read->getRowCount());

        /// If our reader is not first in the chain, canSkipMark is not called in RangeReader.
        /// TODO: adjust the code in RangeReader to call canSkipMark for all readers.
        if (!analyzed_granules.contains(index_mark))
            canSkipMark(from_mark, current_task_last_mark);

        auto it = granules.find(index_mark);
        if (it == granules.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Granule not found (mark: {}, index_mark: {})", from_mark, index_mark);

        auto & granule = it->second;

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
            readPostingsIfNeeded(granule, index_mark);

            const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
            size_t mark_at_index_granule = index_mark * granularity;
            size_t granule_offset = index_granularity.getRowsCountInRange(mark_at_index_granule, from_mark);

            for (size_t i = 0; i < res_columns.size(); ++i)
            {
                auto & column_mutable = res_columns[i]->assumeMutableRef();
                fillColumn(column_mutable, granule, columns_to_read[i].name, granule_offset, rows_to_read);
            }
        }

        ++from_mark;
        read_rows += rows_to_read;
        current_row += rows_to_read;

        /// If we read no all ranges for the index granule,
        /// it may remain in the cache till the end of the query.
        /// It can happen for granules at the borders of the ranges.
        /// We consider this ok, because it is not a big overhead.
        if (remaining_marks.at(index_mark).decrement(granularity))
            granules.erase(index_mark);
    }

    current_mark = from_mark;
    return read_rows;
}

void MergeTreeReaderTextIndex::createEmptyColumns(Columns & columns) const
{
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[i] == nullptr)
            columns[i] = columns_to_read[i].type->createColumn(*serializations[i]);
    }
}

void MergeTreeReaderTextIndex::readPostingsIfNeeded(Granule & granule, size_t index_mark)
{
    if (!granule.need_read_postings)
        return;

    const auto & granule_text = assert_cast<const MergeTreeIndexGranuleText &>(*granule.granule);
    const auto & remaining_tokens = granule_text.getRemainingTokens();

    auto * postings_stream = index_reader->getStreams().at(MergeTreeIndexSubstream::Type::TextIndexPostings);
    auto * data_buffer = postings_stream->getDataBuffer();
    auto * compressed_buffer = postings_stream->getCompressedDataBuffer();

    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*index.condition);
    const String & data_path = data_part_info_for_read->getDataPartStorage()->getFullPath();
    const String & index_name = index.index->getFileName();
    const auto get_postings = [&](const TokenPostingsInfo::FuturePostings future_postings, std::string_view token)
    {
        const auto load_postings = [&]() -> PostingListPtr
        {
            ProfileEvents::increment(ProfileEvents::TextIndexReadPostings);
            compressed_buffer->seek(future_postings.offset_in_file, 0);
            return PostingsSerialization::deserialize(future_postings.header, future_postings.cardinality, *data_buffer);
        };

        if (condition_text.usePostingsCache())
            condition_text.postingsCache()->getOrSet(
                TextIndexPostingsCache::hash(data_path, index_name, index_mark, future_postings.cardinality, future_postings.offset_in_file, token),
                load_postings);

        return load_postings();
    };

    PostingListPtr posting_list = nullptr;
    for (const auto & [token, postings] : remaining_tokens)
    {
        if (postings.hasEmbeddedPostings())
        {
            ProfileEvents::increment(ProfileEvents::TextIndexUsedEmbeddedPostings);
            posting_list = postings.getEmbeddedPostings();
        }
        else
        {
            const auto & future_postings = postings.getFuturePostings();
            posting_list = get_postings(future_postings, token.toView());
        }

        granule.postings.emplace(token, std::move(posting_list));
    }

    granule.need_read_postings = false;
}

/// Finds the union of the posting lists for range [granule_offset, granule_offset + num_rows)
void applyPostingsAny(
    IColumn & column,
    PostingsMap & postings_map,
    PaddedPODArray<UInt32> & indices,
    const std::vector<String> & search_tokens,
    size_t column_offset,
    size_t granule_offset,
    size_t num_rows)
{
    PostingList union_posting;
    PostingList range_posting;
    range_posting.addRange(granule_offset, granule_offset + num_rows);

    for (const auto & token : search_tokens)
    {
        auto it = postings_map.find(token);
        if (it == postings_map.end())
            continue;

        union_posting |= (*it->second & range_posting);
    }

    const size_t cardinality = union_posting.cardinality();
    if (cardinality == 0)
        return;

    indices.resize(cardinality);
    union_posting.toUint32Array(indices.data());

    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    for (size_t i = 0; i < cardinality; ++i)
    {
        size_t relative_row_number = indices[i] - granule_offset;
        chassert(relative_row_number < num_rows);
        column_data[column_offset + relative_row_number] = 1;
    }
}

/// Finds the intersection of the posting lists for range [granule_offset, granule_offset + num_rows)
void applyPostingsAll(
    IColumn & column,
    PostingsMap & postings_map,
    PaddedPODArray<UInt32> & indices,
    const std::vector<String> & search_tokens,
    size_t column_offset,
    size_t granule_offset,
    size_t num_rows)
{
    if (postings_map.size() > std::numeric_limits<UInt16>::max())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many tokens ({}) for All search mode", postings_map.size());

    std::vector<PostingListPtr> token_postings;

    for (const auto & token : search_tokens)
    {
        auto it = postings_map.find(token);
        if (it == postings_map.end())
            return;

        token_postings.push_back(it->second);
    }

    PostingList intersection_posting;
    intersection_posting.addRange(granule_offset, granule_offset + num_rows);

    for (const PostingListPtr & posting : token_postings)
    {
        intersection_posting &= (*posting);

        if (intersection_posting.cardinality() == 0)
            return;
    }

    const size_t cardinality = intersection_posting.cardinality();
    if (cardinality == 0)
        return;

    indices.resize(cardinality);
    intersection_posting.toUint32Array(indices.data());

    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    for (size_t i = 0; i < cardinality; ++i)
    {
        size_t relative_row_number = indices[i] - granule_offset;
        chassert(relative_row_number < num_rows);
        column_data[column_offset + relative_row_number] = 1;
    }
}

void MergeTreeReaderTextIndex::fillColumn(IColumn & column, Granule & granule, const String & column_name, size_t granule_offset, size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    auto search_query = condition_text.getSearchQueryForVirtualColumn(column_name);

    size_t old_size = column_data.size();
    /// Always return true for empty needles.
    UInt8 default_value = search_query->tokens.empty() ? 1 : 0;
    column_data.resize_fill(old_size + num_rows, default_value);

    if (granule.postings.empty() || search_query->tokens.empty())
        return;

    if (search_query->mode == TextSearchMode::Any || granule.postings.size() == 1)
        applyPostingsAny(column, granule.postings, indices_buffer, search_query->tokens, old_size, granule_offset, num_rows);
    else if (search_query->mode == TextSearchMode::All)
        applyPostingsAll(column, granule.postings, indices_buffer, search_query->tokens, old_size, granule_offset, num_rows);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid search mode: {}", search_query->mode);
}

void MergeTreeReaderTextIndex::RemainingMarks::increment()
{
    ++total;
    ++remaining;
}

bool MergeTreeReaderTextIndex::RemainingMarks::decrement(size_t granularity)
{
    if (remaining == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There are no remaining marks");

    --remaining;
    return finished(granularity);
}

bool MergeTreeReaderTextIndex::RemainingMarks::finished(size_t granularity) const
{
    return remaining == 0 && total == granularity;
}

MergeTreeReaderPtr createMergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader,
    const MergeTreeIndexWithCondition & index,
    const NamesAndTypesList & columns_to_read)
{
    return std::make_unique<MergeTreeReaderTextIndex>(main_reader, index, columns_to_read);
}

}
