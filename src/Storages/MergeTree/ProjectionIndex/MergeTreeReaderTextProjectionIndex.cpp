#include <Storages/MergeTree/ProjectionIndex/MergeTreeReaderTextProjectionIndex.h>

#include <Columns/ColumnsNumber.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/ProjectionIndex/MergeTreeProjectionIndexText.h>
#include <Storages/MergeTree/ProjectionIndex/PhraseCursor.h>
#include <Storages/MergeTree/ProjectionIndex/PositionCursor.h>
#include <Storages/MergeTree/ProjectionIndex/PostingCursor.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexSerializationContext.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace ProfileEvents
{
    extern const Event TextIndexReaderTotalMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

MergeTreeReaderTextProjectionIndex::MergeTreeReaderTextProjectionIndex(
    const IMergeTreeReader * main_reader_,
    MergeTreeIndexWithCondition index_,
    NamesAndTypesList columns_,
    MergeTreeIndexGranulePtr index_granule_)
    : MergeTreeReaderTextIndex(main_reader_, index_, columns_, nullptr)
    , projection_granule(std::move(index_granule_))
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
// ─── Posting block reading ──────────────────────────────────────────────

PostingListPtr MergeTreeReaderTextProjectionIndex::readPostingsBlockForToken(
    std::string_view /* token */, const ProjectionTokenInfo & token_info, size_t block_idx, const String & /* index_id */)
{
    chassert(projection_granule);
    auto & gp = assert_cast<MergeTreeProjectionIndexGranuleText &>(*projection_granule);

    if (!gp.large_posting_stream)
        gp.large_posting_stream = createIndependentPostingStream();

    return MergeTreeProjectionIndexGranuleText::materializeFromTokenInfo(*gp.large_posting_stream, token_info, block_idx);
}
// ─── Stream creation ────────────────────────────────────────────────────

LargePostingListReaderStreamPtr MergeTreeReaderTextProjectionIndex::createIndependentStream(const char * suffix, bool required) const
{
    chassert(projection_granule);
    auto & gp = assert_cast<MergeTreeProjectionIndexGranuleText &>(*projection_granule);
    chassert(gp.projection_part);

    const auto & proj_part = gp.projection_part;
    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn("posting", {}, suffix, proj_part->checksums, storage_settings);

    if (!stream_name)
    {
        if (required)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Projection text index part {} is missing required stream {}{} (checksums report no such file)",
                proj_part->name,
                "posting",
                suffix);
        return nullptr;
    }

    auto proj_storage = proj_part->getDataPartStoragePtr();
    String file_name = *stream_name + suffix;
    size_t file_size = proj_storage->getFileSize(file_name);

    if (file_size == 0 && !required)
        return nullptr;

    static constexpr size_t marks_count = 1;
    auto stream_settings = settings;
    stream_settings.is_compressed = false;
    return std::make_shared<LargePostingListReaderStream>(
        data_part_info_for_read->getMergedPartOffsets(),
        data_part_info_for_read->getPartIndex(),
        data_part_info_for_read->getPartStartingOffset(),
        proj_storage,
        *stream_name,
        suffix,
        marks_count,
        MarkRanges{{0, marks_count}},
        stream_settings,
        /*uncompressed_cache=*/nullptr,
        file_size,
        /*marks_loader=*/nullptr,
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);
}

LargePostingListReaderStreamPtr MergeTreeReaderTextProjectionIndex::createIndependentPostingStream() const
{
    return createIndependentStream(PROJECTION_INDEX_LARGE_POSTING_SUFFIX, /*required=*/true);
}

LargePostingListReaderStreamPtr MergeTreeReaderTextProjectionIndex::createIndependentPositionStream() const
{
    return createIndependentStream(PROJECTION_INDEX_POSITION_SUFFIX, /*required=*/false);
}

LargePostingListReaderStreamPtr MergeTreeReaderTextProjectionIndex::createIndependentIndexStream() const
{
    return createIndependentStream(PROJECTION_INDEX_INDEX_SUFFIX, /*required=*/false);
}
// ─── Cursor management ─────────────────────────────────────────────────

MergeTreeReaderTextProjectionIndex::CursorMap MergeTreeReaderTextProjectionIndex::buildCursorMap()
{
    chassert(projection_granule);
    auto & granule_text = dynamic_cast<MergeTreeProjectionIndexGranuleText &>(*projection_granule);
    const auto & remaining_tokens = granule_text.getRemainingTokens();

    /// Get decoded block cache from projection granule (may be null if no mark filtering ran).
    DecodedBlockCache * block_cache = nullptr;
    if (auto * proj_granule = dynamic_cast<MergeTreeProjectionIndexGranuleText *>(projection_granule.get()))
        block_cache = proj_granule->decoded_block_cache.get();

    CursorMap result;

    for (const auto & [token, token_info] : remaining_tokens)
    {
        /// Check if the token was materialized into a Roaring bitmap (has_block_index=0 low-cardinality path).
        auto rare_posting = granule_text.getPostingsForRareToken(token);
        if (rare_posting)
        {
            result.emplace(token, std::make_shared<BitmapCursor>(std::move(rare_posting)));
            continue;
        }

        if (!token_info->large_block_metas.empty())
        {
            auto independent_idx_stream = createIndependentIndexStream();
            if (!independent_idx_stream)
            {
                /// No .pidx (has_block_index=0, high-cardinality token not pre-materialized).
                /// Materialize all large blocks into a Roaring bitmap on demand.
                auto independent_stream = createIndependentPostingStream();
                auto bitmap = std::make_shared<PostingList>();
                for (size_t b = 0; b < token_info->large_block_metas.size(); ++b)
                {
                    auto block_bitmap = MergeTreeProjectionIndexGranuleText::materializeFromTokenInfo(
                        *independent_stream, *token_info, b);
                    if (block_bitmap)
                        *bitmap |= *block_bitmap;
                }
                result.emplace(token, std::make_shared<BitmapCursor>(std::move(bitmap)));
                continue;
            }

            auto independent_stream = createIndependentPostingStream();
            auto cursor = std::make_shared<ProjectionPostingListCursor>(
                std::move(independent_stream), *token_info, std::move(independent_idx_stream),
                nullptr /* pos_stream */, block_cache);
            result.emplace(token, std::move(cursor));
        }
        else if (!token_info->ranges.empty())
        {
            auto cursor = std::make_shared<ProjectionPostingListCursor>(*token_info);
            result.emplace(token, std::move(cursor));
        }
    }

    return result;
}

void MergeTreeReaderTextProjectionIndex::ensureInitialized()
{
    if (is_initialized)
        return;
    is_initialized = true;

    if (!projection_granule)
        projectionReadGranule();

    /// Projection reader doesn't use the skip-index analyzer-based classification.
    /// For Hint-mode virtual columns, mark as always-true since the projection
    /// can't guarantee precise results — the original predicate will be evaluated
    /// on actual data as a post-filter.
    is_always_true.resize(columns_to_read.size(), false);
    use_fallback.resize(columns_to_read.size(), false);

    auto & gp = assert_cast<MergeTreeProjectionIndexGranuleText &>(*projection_granule);
    if (!gp.projection_part)
    {
        std::fill(is_always_true.begin(), is_always_true.end(), true);
        return;
    }

    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        auto search_query = condition_text.getSearchQueryForVirtualColumn(columns_to_read[i].name);
        if (search_query && search_query->direct_read_mode == TextIndexDirectReadMode::Hint)
            is_always_true[i] = true;
    }
}

void MergeTreeReaderTextProjectionIndex::ensureCursorMap()
{
    if (cursor_map_built)
        return;
    cursor_map = buildCursorMap();
    cursor_map_built = true;
}

PostingCursorPtr & MergeTreeReaderTextProjectionIndex::getOrBuildCursor(const String & column_name)
{
    auto [it, inserted] = column_cursors.try_emplace(column_name);
    if (!inserted)
        return it->second;

    ensureCursorMap();

    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    auto search_query = condition_text.getSearchQueryForVirtualColumn(column_name);

    std::vector<PostingCursorPtr> token_cursors;
    token_cursors.reserve(search_query->tokens.size());
    for (const auto & token : search_query->tokens)
    {
        auto cursor_it = cursor_map.find(token);
        if (cursor_it != cursor_map.end())
            token_cursors.emplace_back(cursor_it->second);
    }

    if (token_cursors.empty())
    {
        it->second = nullptr;
    }
    else if (search_query->search_mode == TextSearchMode::All
             && token_cursors.size() != search_query->tokens.size())
    {
        /// For All mode, all tokens must be present; if any is missing, result is empty.
        it->second = nullptr;
    }
    else if (token_cursors.size() == 1 && search_query->search_mode != TextSearchMode::Phrase)
    {
        it->second = std::move(token_cursors[0]);
    }
    else if (search_query->search_mode == TextSearchMode::Phrase)
    {
        if (token_cursors.size() != search_query->tokens.size())
        {
            it->second = nullptr;
            return it->second;
        }

        auto & granule_text = dynamic_cast<MergeTreeProjectionIndexGranuleText &>(*projection_granule);
        const auto & remaining_tokens = granule_text.getRemainingTokens();

        std::vector<ProjectionPostingListCursorPtr> phrase_token_cursors;
        std::vector<PositionCursorPtr> pos_cursors;
        phrase_token_cursors.reserve(search_query->tokens.size());
        pos_cursors.reserve(search_query->tokens.size());

        bool ok = true;
        for (size_t i = 0; i < search_query->tokens.size() && ok; ++i)
        {
            auto token_it = remaining_tokens.find(search_query->tokens[i]);
            if (token_it == remaining_tokens.end())
            {
                ok = false;
                break;
            }

            auto pst_stream = createIndependentPostingStream();
            auto idx_stream = createIndependentIndexStream();
            if (!pst_stream || !idx_stream)
            {
                ok = false;
                break;
            }

            phrase_token_cursors.push_back(
                std::make_shared<ProjectionPostingListCursor>(std::move(pst_stream), *token_it->second, std::move(idx_stream)));

            auto pos_stream = createIndependentPositionStream();
            if (!pos_stream)
            {
                ok = false;
                break;
            }
            pos_cursors.push_back(std::make_shared<PositionCursor>(
                createIndependentPostingStream(), std::move(pos_stream), createIndependentIndexStream(), *token_it->second));
        }

        if (ok)
        {
            it->second = std::make_shared<PhraseCursor>(std::move(phrase_token_cursors), std::move(pos_cursors));
        }
        else
        {
            /// Position data not available — fall back to AND-only cursor.
            /// The original hasPhrase in the WHERE clause handles exact phrase matching.
            std::sort(
                token_cursors.begin(),
                token_cursors.end(),
                [](const PostingCursorPtr & a, const PostingCursorPtr & b) { return a->cardinality() < b->cardinality(); });
            it->second = std::make_shared<AndCursor>(std::move(token_cursors));
        }
    }
    else if (search_query->search_mode == TextSearchMode::Any)
    {
        std::sort(
            token_cursors.begin(),
            token_cursors.end(),
            [](const PostingCursorPtr & a, const PostingCursorPtr & b)
            { return a->density() > b->density(); });
        it->second = std::make_shared<OrCursor>(std::move(token_cursors));
    }
    else
    {
        std::sort(
            token_cursors.begin(),
            token_cursors.end(),
            [](const PostingCursorPtr & a, const PostingCursorPtr & b)
            { return a->cardinality() < b->cardinality(); });
        it->second = std::make_shared<AndCursor>(std::move(token_cursors));
    }

    return it->second;
}
// ─── Column filling ────────────────────────────────────────────────────

void MergeTreeReaderTextProjectionIndex::fillColumnLazy(
    IColumn & column, const String & column_name, size_t column_offset, size_t row_offset, size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();

    size_t required_size = column_offset + num_rows;
    if (column_data.size() < required_size)
        column_data.resize_fill(required_size, 0);

    auto & cursor = getOrBuildCursor(column_name);
    if (!cursor)
    {
        /// Null cursor: projection can't evaluate this query.
        /// For pattern-only queries (LIKE/ILIKE in Hint mode, see MergeTreeIndexConditionText
        /// `traverseFunctionNode`), the optimizer keeps the original predicate as a post-filter
        /// (an AND with the virtual column). Fill with 1 (always true) so the post-filter
        /// evaluates and produces the correct rows. For empty-token queries (e.g.
        /// hasAnyTokens(x, [''])), fill stays 0 (no match) — there is no post-filter to fall
        /// back to and an empty needle cannot match any document.
        const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
        auto search_query = condition_text.getSearchQueryForVirtualColumn(column_name);
        if (search_query && !search_query->patterns.empty())
            memset(column_data.data() + column_offset, 1, num_rows);
        return;
    }

    cursor->fill(column_data.data() + column_offset, row_offset, num_rows);
}

void MergeTreeReaderTextProjectionIndex::fillBatch(Columns & res_columns, size_t from_row, size_t batch_rows)
{
    for (size_t i = 0; i < res_columns.size(); ++i)
    {
        auto & col = res_columns[i]->assumeMutableRef();
        if (is_always_true[i])
        {
            auto & data = assert_cast<ColumnUInt8 &>(col).getData();
            data.resize_fill(col.size() + batch_rows, 1);
        }
        else
        {
            fillColumnLazy(col, columns_to_read[i].name, col.size(), from_row, batch_rows);
        }
    }
}
// ─── Granule / mark management ──────────────────────────────────────────

void MergeTreeReaderTextProjectionIndex::projectionReadGranule()
{
    MergeTreeIndexInputStreams empty_streams;
    projection_granule = index.index->createIndexGranule();
    projection_granule->deserializeBinaryWithMultipleStreams(empty_streams, *deserialization_state);
}

void MergeTreeReaderTextProjectionIndex::setIndexGranule(MergeTreeIndexGranulePtr index_granule)
{
    /// Projection granule — just store it, don't try to cast to MergeTreeIndexGranuleText.
    projection_granule = std::move(index_granule);
}

void MergeTreeReaderTextProjectionIndex::setPrecomputedGranule(const IndexGranulesMap & granules)
{
    auto it = granules.find(index.index->index.name);
    if (it != granules.end() && it->second)
    {
        projection_granule = it->second;
    }
}

// ─── readRows ───────────────────────────────────────────────────────────

size_t MergeTreeReaderTextProjectionIndex::readRows(
    size_t from_mark,
    size_t /* current_task_last_mark */,
    bool continue_reading,
    size_t max_rows_to_read,
    size_t rows_offset,
    Columns & res_columns)
{
    ensureInitialized();

    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReaderTotalMicroseconds);
    const auto & index_granularity = data_part_info_for_read->getIndexGranularity();

    size_t from_row = 0;
    if (continue_reading)
    {
        from_mark = current_mark;
        from_row = current_row + rows_offset;
    }
    else
    {
        from_row = index_granularity.getMarkStartingRow(from_mark) + rows_offset;

        /// Non-continuation read: the read position may jump backward.
        /// Forward-only cursors (AndCursor, PhraseCursor) cannot rewind,
        /// so clear all cached cursors to force rebuild from scratch.
        if (cursor_map_built)
        {
            cursor_map.clear();
            column_cursors.clear();
            cursor_map_built = false;
        }
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

    createEmptyColumns(res_columns);
    size_t total_marks = index_granularity.getMarksCountWithoutFinal();

    size_t batch_rows = 0;
    while (from_mark < total_marks && batch_rows < max_rows_to_read)
    {
        batch_rows += std::min(index_granularity.getMarkRows(from_mark), max_rows_to_read - batch_rows);
        ++from_mark;
    }

    if (batch_rows > 0)
        fillBatch(res_columns, from_row, batch_rows);

    current_mark = from_mark;
    current_row = from_row + batch_rows;
    return batch_rows;
}

}
