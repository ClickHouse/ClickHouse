#include <Storages/MergeTree/ProjectionIndex/MergeTreeReaderProjectionIndex.h>

#include <Storages/MergeTree/ProjectionIndex/MergeTreeIndexProjection.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace ProfileEvents
{
    extern const Event TextIndexReaderTotalMicroseconds;
}

namespace DB
{

namespace Setting
{
    extern const SettingsString text_index_posting_list_apply_mode;
}

MergeTreeReaderProjectionIndex::MergeTreeReaderProjectionIndex(
    const IMergeTreeReader * main_reader_, MergeTreeIndexWithCondition index_, NamesAndTypesList columns_, bool can_skip_mark_)
    : MergeTreeReaderTextIndex(main_reader_, index_, columns_, can_skip_mark_)
{
    auto data_part = getDataPart();
    auto index_format = index.index->getDeserializedFormat(data_part->checksums, index.index->getFileName());
    chassert(index_format);

    MergeTreeIndexDeserializationState state{
        .version = index_format.version,
        .condition = index.condition.get(),
        .part = *data_part,
        .index = *index.index,
    };

    deserialization_state = std::make_unique<MergeTreeIndexDeserializationState>(std::move(state));

    /// Determine apply mode from query settings.
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    const auto & settings = condition_text.getContext()->getSettingsRef();
    String mode = settings[Setting::text_index_posting_list_apply_mode];
    use_lazy_mode = (mode == "lazy");
}

PostingListPtr MergeTreeReaderProjectionIndex::readPostingsBlockForToken(
    std::string_view /* token */, const TokenPostingsInfo & token_info, size_t block_idx, PostingListCodecPtr)
{
    chassert(granule);
    auto & granule_projection = assert_cast<MergeTreeIndexGranuleProjection &>(*granule);
    chassert(granule_projection.large_posting_stream);
    return MergeTreeIndexGranuleProjection::materializeFromTokenInfo(*granule_projection.large_posting_stream, token_info, block_idx);
}

PostingListCursorMap MergeTreeReaderProjectionIndex::buildCursorMap()
{
    chassert(granule);
    auto & granule_text = dynamic_cast<MergeTreeIndexGranuleText &>(*granule);
    const auto & remaining_tokens = granule_text.getRemainingTokens();
    auto & granule_projection = assert_cast<MergeTreeIndexGranuleProjection &>(*granule);

    PostingListCursorMap result;

    for (const auto & [token, token_info] : remaining_tokens)
    {
        if (!useful_tokens.contains(token))
            continue;

        if (token_info.embedded_postings)
        {
            /// For embedded postings, create cursor without stream (it reads from embedded data).
            auto cursor = std::make_shared<PostingListCursor>(token_info, 0);
            result.emplace(token, std::move(cursor));
        }
        else if (!token_info.offsets.empty())
        {
            /// For large postings, create cursor with the large posting stream.
            /// Each LargePostingBlockMeta in offsets is treated as a segment.
            chassert(granule_projection.large_posting_stream);
            auto cursor = std::make_shared<PostingListCursor>(
                granule_projection.large_posting_stream.get(), token_info, 0);
            for (size_t s = 1; s < token_info.offsets.size(); ++s)
                cursor->addSegment(s);
            result.emplace(token, std::move(cursor));
        }
    }

    return result;
}

void MergeTreeReaderProjectionIndex::ensureCursorMap()
{
    if (cursor_map_built)
        return;
    cursor_map = buildCursorMap();
    cursor_map_built = true;
}

void MergeTreeReaderProjectionIndex::fillColumnLazy(
    IColumn & column,
    const String & column_name,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    auto search_query = condition_text.getSearchQueryForVirtualColumn(column_name);

    column_data.resize_fill(column_offset + num_rows, 0);

    if (cursor_map.empty() || search_query->tokens.empty())
        return;

    /// Use default thresholds for adaptive algorithm selection.
    constexpr bool brute_force_apply = false;
    constexpr float density_threshold = 0.5f;

    if (search_query->search_mode == TextSearchMode::Any || cursor_map.size() == 1)
        lazyUnionPostingLists(column, cursor_map, search_query->tokens, column_offset, row_offset, num_rows, brute_force_apply, density_threshold);
    else if (search_query->search_mode == TextSearchMode::All)
        lazyIntersectPostingLists(column, cursor_map, search_query->tokens, column_offset, row_offset, num_rows, brute_force_apply, density_threshold);
}

size_t MergeTreeReaderProjectionIndex::readRows(
    size_t from_mark,
    size_t current_task_last_mark,
    bool continue_reading,
    size_t max_rows_to_read,
    size_t rows_offset,
    Columns & res_columns)
{
    /// In materialize mode, delegate to the base class implementation.
    if (!use_lazy_mode)
        return MergeTreeReaderTextIndex::readRows(from_mark, current_task_last_mark, continue_reading, max_rows_to_read, rows_offset, res_columns);

    /// Lazy mode: use PostingListCursor-based intersection/union.
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReaderTotalMicroseconds);
    const auto & index_granularity = data_part_info_for_read->getIndexGranularity();

    size_t from_row;
    if (continue_reading)
    {
        from_mark = current_mark;
        from_row = current_row + rows_offset;
    }
    else
    {
        from_row = index_granularity.getMarkStartingRow(from_mark) + rows_offset;
    }

    size_t total_rows = data_part_info_for_read->getRowCount();
    if (from_row < total_rows)
        max_rows_to_read = std::min(max_rows_to_read, total_rows - from_row);
    else
        max_rows_to_read = 0;

    if (res_columns.empty())
    {
        ++current_mark;
        current_row += max_rows_to_read;
        return max_rows_to_read;
    }

    size_t read_rows = 0;
    createEmptyColumns(res_columns);

    while (read_rows < max_rows_to_read)
    {
        size_t rows_to_read = std::min(index_granularity.getMarkRows(from_mark), max_rows_to_read - read_rows);

        if (!analyzed_granules.contains(static_cast<UInt32>(from_mark)))
        {
            canSkipMark(from_mark, current_task_last_mark);
        }

        if (!may_be_true_granules.contains(static_cast<UInt32>(from_mark)))
        {
            for (const auto & column : res_columns)
            {
                auto & column_data = assert_cast<ColumnUInt8 &>(column->assumeMutableRef()).getData();
                column_data.resize_fill(column->size() + rows_to_read, 0);
            }
        }
        else
        {
            /// Build cursor map once (lazy), then reuse for all subsequent marks.
            ensureCursorMap();

            for (size_t i = 0; i < res_columns.size(); ++i)
            {
                auto & column_mutable = res_columns[i]->assumeMutableRef();

                if (is_always_true[i])
                {
                    auto & column_data = assert_cast<ColumnUInt8 &>(column_mutable).getData();
                    column_data.resize_fill(column_mutable.size() + rows_to_read, 1);
                }
                else
                {
                    fillColumnLazy(column_mutable, columns_to_read[i].name, column_mutable.size(), from_row, rows_to_read);
                }
            }
        }

        ++from_mark;
        from_row += rows_to_read;
        read_rows += rows_to_read;
    }

    current_mark = from_mark;
    current_row = from_row;
    return read_rows;
}

}
