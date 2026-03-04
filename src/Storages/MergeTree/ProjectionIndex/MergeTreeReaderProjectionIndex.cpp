#include <Storages/MergeTree/ProjectionIndex/MergeTreeReaderProjectionIndex.h>

#include <Storages/MergeTree/ProjectionIndex/MergeTreeIndexProjection.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListState.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexSerializationContext.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace ProfileEvents
{
    extern const Event TextIndexReaderTotalMicroseconds;
}

namespace DB
{

namespace Setting
{
    extern const SettingsString text_index_posting_list_apply_mode;
    extern const SettingsFloat text_index_density_threshold;
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
}

PostingListPtr MergeTreeReaderProjectionIndex::readPostingsBlockForToken(
    std::string_view /* token */, const TokenPostingsInfo & token_info, size_t block_idx, PostingListCodecPtr)
{
    chassert(granule);
    auto & granule_projection = assert_cast<MergeTreeIndexGranuleProjection &>(*granule);
    chassert(granule_projection.large_posting_stream);
    return MergeTreeIndexGranuleProjection::materializeFromTokenInfo(
        *granule_projection.large_posting_stream, token_info, block_idx, granule_projection.posting_list_format_version);
}

LargePostingListReaderStreamPtr MergeTreeReaderProjectionIndex::createIndependentPostingStream() const
{
    chassert(granule);
    auto & granule_projection = assert_cast<MergeTreeIndexGranuleProjection &>(*granule);
    chassert(granule_projection.projection_part);

    /// Use the projection part's checksums and storage, not the main part's.
    /// `data_part_info_for_read` refers to the main part, but the `.lpst` file
    /// is written into the projection part's directory.
    const auto & proj_part = granule_projection.projection_part;
    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
        "posting", {}, PROJECTION_INDEX_LARGE_POSTING_SUFFIX, proj_part->checksums, storage_settings);
    chassert(stream_name);

    auto proj_storage = proj_part->getDataPartStoragePtr();
    String file_name = *stream_name + PROJECTION_INDEX_LARGE_POSTING_SUFFIX;
    size_t file_size = proj_storage->getFileSize(file_name);

    static constexpr size_t marks_count = 1;
    auto large_posting_stream_settings = settings;
    large_posting_stream_settings.is_compressed = false;
    return std::make_shared<LargePostingListReaderStream>(
        data_part_info_for_read->getMergedPartOffsets(),
        data_part_info_for_read->getPartIndex(),
        data_part_info_for_read->getPartStartingOffset(),
        proj_storage,
        *stream_name,
        PROJECTION_INDEX_LARGE_POSTING_SUFFIX,
        marks_count,
        MarkRanges{{0, marks_count}},
        large_posting_stream_settings,
        /*uncompressed_cache=*/nullptr,
        file_size,
        /*marks_loader=*/nullptr,
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);
}

PostingListCursorMap MergeTreeReaderProjectionIndex::buildCursorMap()
{
    chassert(granule);
    auto & granule_text = dynamic_cast<MergeTreeIndexGranuleText &>(*granule);
    const auto & remaining_tokens = granule_text.getRemainingTokens();

    PostingListCursorMap result;

    for (const auto & [token, token_info] : remaining_tokens)
    {
        if (!useful_tokens.contains(token))
            continue;

        if (token_info.embedded_postings)
        {
            /// For embedded postings, create cursor without stream (it reads from embedded data).
            auto cursor = std::make_shared<PostingListCursor>(token_info);
            result.emplace(token, std::move(cursor));
        }
        else if (!token_info.offsets.empty())
        {
            /// For large postings, create cursor with an independent stream.
            /// Each cursor owns its own stream to avoid seek contention
            /// when multiple cursors share a ReadBuffer in leapfrog intersection.
            auto independent_stream = createIndependentPostingStream();
            auto cursor = std::make_shared<PostingListCursor>(
                std::move(independent_stream), token_info);
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

    constexpr bool brute_force_apply = false;
    const auto & query_settings = condition_text.getContext()->getSettingsRef();
    float density_threshold = static_cast<float>(double(query_settings[Setting::text_index_density_threshold]));

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
    /// Determine apply mode:
    /// - V1 (no block index): always use materialize mode (base class)
    /// - V2 (with block index): use lazy mode if setting allows
    bool use_lazy = false;
    if (granule)
    {
        auto & granule_projection = assert_cast<MergeTreeIndexGranuleProjection &>(*granule);
        const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
        const auto & query_settings = condition_text.getContext()->getSettingsRef();
        use_lazy = postingListFormatHasBlockIndex(granule_projection.posting_list_format_version)
            && String(query_settings[Setting::text_index_posting_list_apply_mode]) == "lazy";
    }

    if (!use_lazy)
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
