#include <Columns/ColumnsCommon.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexAnalyzer.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>
#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <Storages/MergeTree/TextIndexPhraseSearch.h>
#include <Storages/MergeTree/PostingListCursor.h>
#include <Storages/MergeTree/TextIndexPostingsRankCursor.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/TextIndexUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Core/Settings.h>

#include <algorithm>
#include <iterator>
#include <optional>

namespace ProfileEvents
{
    extern const Event TextIndexReaderTotalMicroseconds;
    extern const Event TextIndexFilledFromFoldedPostings;
    extern const Event TextIndexPositionsDecodeMicroseconds;
    extern const Event TextIndexPhraseMatchMicroseconds;
    extern const Event TextIndexPhraseCandidatesMicroseconds;
    extern const Event TextIndexPositionsBlocksRead;
    extern const Event TextIndexPositionsBlocksTotal;
    extern const Event TextIndexPositionsBytesRead;
    extern const Event TextIndexPhraseCandidates;
    extern const Event TextIndexPhraseSearches;
    extern const Event TextIndexPhraseCursorSearches;
    extern const Event TextIndexPhraseFallbacks;
}

namespace DB
{

namespace Setting
{
    extern const SettingsTextIndexPostingListApplyMode text_index_posting_list_apply_mode;
    extern const SettingsTextIndexPostingsIntersectionAlgorithm text_index_postings_intersection_algorithm;
    extern const SettingsFloat text_index_hint_max_selectivity;
}

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

MergeTreeReaderTextIndex::MergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader_,
    IndexReadTask index_read_task_,
    NamesAndTypesList columns_,
    MergeTreeIndexGranulePtr index_granule_)
    : IMergeTreeReader(
        main_reader_->data_part_info_for_read,
        columns_,
        /*virtual_fields=*/ {},
        main_reader_->storage_snapshot,
        main_reader_->storage_settings,
        Context::getGlobalContextInstance()->getIndexUncompressedCache().get(),
        /*columns_cache=*/ nullptr,
        Context::getGlobalContextInstance()->getIndexMarkCache().get(),
        main_reader_->all_mark_ranges,
        main_reader_->settings)
    , index_read_task(std::move(index_read_task_))
    , can_read_incomplete_granules(main_reader_->canReadIncompleteGranules())
    , condition_text(std::dynamic_pointer_cast<MergeTreeIndexConditionText>(index_read_task.index.condition_template->generateUnsubstituted()))
    , resolved_searches(columns_.size())
    , phrase_streams(columns_.size())
{
    search_queries.reserve(columns_.size());

    for (const auto & column : columns_)
    {
        if (!column.name.starts_with(TEXT_INDEX_VIRTUAL_COLUMN_PREFIX) || !WhichDataType(column.type).isUInt8())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column {} with type {} should not be filled by text index reader",
                column.name, column.type->getName());
        }

        auto task_column_it = std::ranges::find(index_read_task.columns, column.name, &IndexReadTask::Column::name);
        if (task_column_it == index_read_task.columns.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} is not in the read task of the text index '{}'", column.name, index_read_task.index.index->index.name);

        search_queries.push_back(task_column_it->search_query);
    }

    auto data_part = getDataPart();
    auto index_format = index_read_task.index.index->getDeserializedFormat(*data_part, index_read_task.index.index->getFileName());
    chassert(index_format);

    MergeTreeIndexDeserializationState state
    {
        .version = index_format.version,
        .condition = condition_text.get(),
        .part_info = *data_part_info_for_read,
        .index = *index_read_task.index.index,
        .readable_ranges = nullptr,
        .text_index_read_postings = true,
        .reader_settings = settings,
    };

    deserialization_state = std::make_unique<MergeTreeIndexDeserializationState>(std::move(state));

    /// Lazy mode is requested per query; actual support is determined from the on-disk sparse-index header.
    const auto & ctx_settings = condition_text->getContext()->getSettingsRef();
    const auto apply_mode = ctx_settings[Setting::text_index_posting_list_apply_mode].value;

    lazy_mode_requested = (apply_mode == TextIndexPostingListApplyMode::Lazy);
    intersection_algorithm = ctx_settings[Setting::text_index_postings_intersection_algorithm].value;

    if (index_granule_)
        setIndexGranule(std::move(index_granule_));

    initializeFallbackReader(main_reader_);
}

void MergeTreeReaderTextIndex::setIndexGranule(MergeTreeIndexGranulePtr index_granule)
{
    chassert(index_granule);
    granule = std::dynamic_pointer_cast<const MergeTreeIndexGranuleText>(index_granule);
    auto postings_codec = PostingListCodecFactory::createPostingListCodec(granule->getPostingsCodecType());

    /// Lazy mode requires the per-segment block-index section (from `V1_WithCodec` onward) and
    /// pure-token queries — pattern predicates take the eager materialize path.
    use_lazy_mode = lazy_mode_requested
        && postings_codec->getType() != IPostingListCodec::Type::None
        && granule->getSerializationVersion() >= MergeTreeTextIndexSerializationVersion::V1_WithCodec
        && !condition_text->hasSearchPatterns();

    postings_serialization = PostingsSerialization(std::move(postings_codec), granule->getSerializationVersion());
}

void MergeTreeReaderTextIndex::initializeFallbackReader(const IMergeTreeReader * main_reader)
{
    /// Check if any virtual column may need a fallback path:
    /// - Pattern queries (LIKE): fallback when dictionary scan is abandoned.
    /// - Phrase queries (hasPhrase with Exact mode): fallback when estimated cardinality is too high
    ///   and reading position data would be slower than evaluating directly.
    /// Only exact direct read needs it: a hint keeps the original predicate, so it can just be always true.
    auto needs_fallback_for_query = [](const auto & search_query)
    {
        if (!search_query || search_query->getDirectReadMode() != TextIndexDirectReadMode::Exact)
            return false;

        return !search_query->getPatterns().empty() || search_query->getSearchMode() == TextSearchMode::Phrase;
    };

    if (std::ranges::none_of(search_queries, needs_fallback_for_query))
        return;

    /// Build a fallback evaluation path. Compile each virtual column's default expression
    /// (the original search predicate) and determine the required physical columns from it.
    /// Used when:
    /// - The dictionary scan is cut short (LIKE pattern queries).
    /// - Phrase search cardinality is too high (cheaper to evaluate hasPhrase on physical data).
    auto context_copy = createContextForDefaultExpressions();
    auto combined_columns = buildCombinedColumnsForDefaultExpressions();

    /// Build a header block containing all physical columns (column type only, no data).
    /// evaluateMissingDefaults passes this to createExpressionsAnalyzer, which creates
    /// a StorageDummy from it — StorageDummy requires at least one column, so the header
    /// must be non-empty.
    Block physical_header;
    for (const auto & phys_col : storage_snapshot->metadata->getColumns().getAllPhysical())
        physical_header.insert({phys_col.type->createColumn(), phys_col.type, phys_col.name});

    NameSet fallback_columns_set;
    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        const auto & column = columns_to_read[i];
        const auto & search_query = search_queries[i];
        if (!needs_fallback_for_query(search_query))
            continue;

        /// Compile the virtual column's default expression (the original search predicate).
        /// We pass a header with all physical columns so that createExpressionsAnalyzer
        /// can build a non-empty StorageDummy (it requires at least one column).
        NamesAndTypesList need_col{{column.name, column.type}};
        auto dag = DB::evaluateMissingDefaults(physical_header, need_col, combined_columns, context_copy);
        if (!dag)
            continue;

        dag->addMaterializingOutputActions(/*materialize_sparse=*/ false);
        auto actions = std::make_shared<ExpressionActions>(
            std::move(*dag), ExpressionActionsSettings(context_copy->getSettingsRef()));

        /// Collect the physical columns this expression requires.
        for (const auto & req : actions->getRequiredColumnsWithTypes())
        {
            if (fallback_columns_set.insert(req.name).second)
                fallback_columns_list.push_back(req);
        }

        fallback_expressions.emplace(column.name, std::move(actions));
    }

    if (!fallback_columns_list.empty())
    {
        /// The physical columns of the fallback expression are discovered here, long after
        /// `MergeTreeReadPoolBase` sized the query's columns cache write estimate over the
        /// columns of the read task. Writing them to the cache would therefore write bytes no
        /// budget accounted for, so the fallback reader only reads from the cache (entries an
        /// ordinary reader of the same columns put there) and never writes to it.
        auto fallback_settings = main_reader->settings;
        fallback_settings.enable_columns_cache_writes = false;

        fallback_reader = createMergeTreeReader(
            main_reader->data_part_info_for_read,
            fallback_columns_list,
            main_reader->storage_snapshot,
            main_reader->storage_settings,
            main_reader->all_mark_ranges,
            /*virtual_fields=*/{},
            main_reader->uncompressed_cache,
            main_reader->columns_cache,
            main_reader->mark_cache,
            /*deserialization_prefixes_cache=*/nullptr,
            fallback_settings,
            /*avg_value_size_hints=*/{},
            /*profile_callback=*/{});
    }
}

void MergeTreeReaderTextIndex::updateReadRequestMap(MarkRangesPtr request_map)
{
    IMergeTreeReader::updateReadRequestMap(request_map);
    /// Only the fallback reader reads the part's data. The index streams count index granules or tokens, not data marks.
    if (fallback_reader)
        fallback_reader->updateReadRequestMap(std::move(request_map));
}

void MergeTreeReaderTextIndex::updateAllMarkRanges(const MarkRanges & ranges)
{
    IMergeTreeReader::updateAllMarkRanges(ranges);

    if (fallback_reader)
    {
        fallback_reader->updateAllMarkRanges(ranges);
    }

    if (!ranges.empty())
    {
        const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
        size_t row_begin = index_granularity.getMarkStartingRow(ranges.front().begin);
        size_t row_end = index_granularity.getMarkStartingRow(ranges.back().end);

        if (row_begin != row_end)
            cleanupPostingsBlocks(RowsRange(row_begin, row_end - 1));
    }
}

MergeTreeDataPartPtr MergeTreeReaderTextIndex::getDataPart() const
{
    const auto * loaded_data_part = typeid_cast<const LoadedMergeTreeDataPartInfoForReader *>(data_part_info_for_read.get());
    if (!loaded_data_part)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading text index is supported only for loaded data parts");

    return loaded_data_part->getDataPart();
}

void MergeTreeReaderTextIndex::readGranule()
{
    auto substreams = index_read_task.index.index->getSubstreams();
    auto data_part = getDataPart();

    LOG_TRACE(getLogger("MergeTreeReaderTextIndex"), "Reading text index granule for data part '{}'", data_part->getDataPartStorage().getFullPath());

    auto sparse_index_stream = makeTextIndexInputStream(*data_part_info_for_read, index_read_task.index.index->getFileName(), substreams[0], settings, /*expected_buffer_size=*/ std::nullopt);
    sparse_index_stream->seekToStart();
    resetCursors();

    /// The analysis opens the dictionary and postings streams itself.
    MergeTreeIndexInputStreams streams;
    streams[MergeTreeIndexSubstream::Type::Regular] = sparse_index_stream.get();

    auto granule_ptr = index_read_task.index.index->createIndexGranule();
    granule_ptr->deserializeBinaryWithMultipleStreams(streams, *deserialization_state);
    setIndexGranule(std::move(granule_ptr));
}

namespace
{

/// A flat list ranks documents by index, a compressed one through its block index.
bool canUseRankCursor(const TokenPostingsInfo & info)
{
    return (info.header & PostingsSerialization::Flags::RawPostings) || (info.header & PostingsSerialization::Flags::HasBlockIndex);
}

bool canUseRankCursors(const VectorWithMemoryTracking<String> & tokens, const TokenToPostingsInfosMap & token_infos)
{
    for (const auto & token : tokens)
    {
        auto it = token_infos.find(token);
        if (it != token_infos.end() && !canUseRankCursor(*it->second))
            return false;
    }
    return true;
}

}

void MergeTreeReaderTextIndex::classifyVirtualColumns()
{
    is_always_true.resize(columns_to_read.size(), false);
    use_fallback.resize(columns_to_read.size(), false);

    const auto & analyzer = granule->getAnalyzer();

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        const auto & column = columns_to_read[i];
        const auto & search_query = search_queries[i];
        const auto & query_builder = analyzer.getQueryBuilder(*search_query);

        if (search_query->getTokens().empty() && search_query->getPatterns().empty())
        {
            /// Token and phrase searches with no search tokens never match (row-level returns 0, e.g. when a
            /// postprocessor maps every needle token to empty). Encode this as an explicit no-match so direct
            /// read agrees with the row-scan path; otherwise an always-true virtual column would wrongly keep
            /// all rows once granule pruning cannot mask it (e.g. under OR).
            if (search_query->getFunctionName() == "hasAnyTokens" || search_query->getFunctionName() == "hasAllTokens"
                || search_query->getSearchMode() == TextSearchMode::Phrase)
                continue;

            /// Always return true for empty needles.
            is_always_true[i] = true;
        }
        else if (query_builder.is_failed)
        {
            /// Query is definitely false (e.g. a required token in All mode is missing).
            continue;
        }
        else if (query_builder.is_bypassed)
        {
            if (search_query->getDirectReadMode() == TextIndexDirectReadMode::Hint)
            {
                is_always_true[i] = true;
            }
            else
            {
                if (!fallback_reader || !fallback_expressions.contains(column.name))
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "The fallback reader or expression for pattern virtual column '{}' is not initialized", column.name);
                }

                use_fallback[i] = true;
            }
        }
        else if (search_query->getSearchMode() == TextSearchMode::Phrase
            && !canUseRankCursors(search_query->getTokens(), analyzer.getAllTokenInfos()))
        {
            /// Without a posting list codec there is no block index to walk, so the phrase is evaluated on the column.
            if (search_query->getDirectReadMode() == TextIndexDirectReadMode::Hint)
            {
                is_always_true[i] = true;
            }
            else
            {
                if (!fallback_reader || !fallback_expressions.contains(column.name))
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "The fallback reader or expression for phrase virtual column '{}' is not initialized", column.name);
                }

                use_fallback[i] = true;
                ProfileEvents::increment(ProfileEvents::TextIndexPhraseFallbacks);
            }
        }
        else if (
            search_query->getSearchMode() == TextSearchMode::Phrase
            && search_query->getDirectReadMode() == TextIndexDirectReadMode::Exact
            && fallback_reader && fallback_expressions.contains(column.name))
        {
            /// For phrase queries with positions, check selectivity before reading positional data.
            /// Reading large position lists for common phrases is slower than evaluating `hasPhrase`
            /// on physical data via the fallback path. Estimate the phrase cardinality as the
            /// intersection of its tokens (a safe upper bound) from the analyzer's per-token cardinalities.
            const auto & all_token_infos = analyzer.getAllTokenInfos();
            const auto & settings = condition_text->getContext()->getSettingsRef();
            const double selectivity_threshold = static_cast<double>(settings[Setting::text_index_hint_max_selectivity]);
            /// Cardinalities (granule) and num_rows_in_part (part) share scale - a text index has whole-part granularity.
            const size_t num_rows_in_part = data_part_info_for_read->getRowCount();

            const bool all_tokens_present = ((num_rows_in_part > 0) && std::ranges::all_of(search_query->getTokens(),
                    [&](const auto & token) { return all_token_infos.find(token) != all_token_infos.end(); }));

            if (all_tokens_present)
            {
                double log_cardinality = 0.0;
                for (const auto & token : search_query->getTokens())
                    log_cardinality += std::log(static_cast<double>(all_token_infos.find(token)->second->cardinality));

                log_cardinality -= static_cast<double>(search_query->getTokens().size() - 1) * std::log(static_cast<double>(num_rows_in_part));
                if (std::exp(log_cardinality) > static_cast<double>(num_rows_in_part) * selectivity_threshold)
                {
                    use_fallback[i] = true;
                    ProfileEvents::increment(ProfileEvents::TextIndexPhraseFallbacks);
                }
            }
        }
    }
}

PostingListCursorPtr MergeTreeReaderTextIndex::makeLazyCursor(std::string_view token, const TokenPostingsInfo & token_info)
{
    if (!(token_info.header & PostingsSerialization::Flags::IsCompressed))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected token for lazy mode: {}. Multi-block postings must be compressed", token);

    return std::make_shared<PostingListCursor>(
        getPostingsStream(token, token_info),
        token_info,
        condition_text->postingsCache().get(),
        granule->getIndexIdForCaches());
}

void MergeTreeReaderTextIndex::initializePositionsStream()
{
    const auto & data_part = getDataPart();

    auto index_format = index_read_task.index.index->getDeserializedFormat(*data_part, index_read_task.index.index->getFileName());
    if (index_format.version != 2)
        return;

    const auto positions_substream = std::ranges::find_if(
        index_format.substreams,
        [](const auto & substream) { return substream.type == MergeTreeIndexSubstream::Type::TextIndexPositions; });

    if (positions_substream == index_format.substreams.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index format V2 has no positions substream for index `{}`", index_read_task.index.index->index.name);

    positions_stream = makeTextIndexInputStream(
        *data_part_info_for_read,
        index_read_task.index.index->getFileName(),
        *positions_substream,
        settings,
        /*expected_buffer_size=*/ std::nullopt);

    positions_stream->seekToStart();
}

size_t MergeTreeReaderTextIndex::readRows(
    size_t from_mark,
    size_t current_range_last_mark,
    bool continue_reading,
    size_t max_rows_to_read,
    MutableColumns & res_columns, Columns & /* shared_columns */)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReaderTotalMicroseconds);
    const auto & index_granularity = data_part_info_for_read->getIndexGranularity();

    size_t from_row = 0;
    if (continue_reading)
    {
        from_mark = current_mark;
        from_row = current_row;
    }
    else
    {
        from_row = index_granularity.getMarkStartingRow(from_mark);

        /// Backward jump invalidates the per-token cursor cache: cached cursors are
        /// forward-only (their `linearOr` / `linearAnd` / `advance` walk segments from
        /// `current_segment_idx` onward), so they cannot serve an earlier row.
        if (from_row < current_row)
            resetCursors();
    }

    size_t total_rows = data_part_info_for_read->getRowCount();
    if (from_row < total_rows)
        max_rows_to_read = std::min(max_rows_to_read, total_rows - from_row);
    else
        max_rows_to_read = 0;

    size_t total_marks = index_granularity.getMarksCountWithoutFinal();

    if (res_columns.empty())
    {
        /// Keep `current_mark` the mark containing `current_row`, as the main loop does.
        current_row = from_row + max_rows_to_read;
        current_mark = from_mark;
        while (current_mark < total_marks && index_granularity.getMarkStartingRow(current_mark + 1) <= current_row)
            ++current_mark;

        return max_rows_to_read;
    }

    size_t read_rows = 0;
    createEmptyColumns(res_columns, max_rows_to_read);

    if (!is_initialized && max_rows_to_read > 0)
    {
        /// Granule may be not set in the distributed index analysis.
        /// TODO: implement distributed index analysis for text index.
        if (!granule)
            readGranule();

        is_initialized = true;
        classifyVirtualColumns();
        initializePositionsStream();
    }

    const bool any_use_fallback = !use_fallback.empty() && std::ranges::any_of(use_fallback, [](bool b) { return b; });

    /// If any column needs the fallback evaluation, read the physical columns upfront.
    /// We pass the same mark/continue_reading/offset arguments so the fallback reader stays
    /// in sync with the text-index reader across multiple readRows calls.
    Block fallback_block;
    if (any_use_fallback && fallback_reader && max_rows_to_read > 0)
    {
        MutableColumns fallback_cols(fallback_columns_list.size());
        Columns fallback_shared_cols(fallback_columns_list.size());
        fallback_reader->readRows(from_mark, current_range_last_mark, continue_reading, max_rows_to_read, fallback_cols, fallback_shared_cols);
        size_t col_idx = 0;
        for (const auto & col_name_type : fallback_columns_list)
        {
            if (fallback_cols[col_idx])
                fallback_block.insert({std::move(fallback_cols[col_idx]), col_name_type.type, col_name_type.name});
            else
                fallback_block.insert({fallback_shared_cols[col_idx], col_name_type.type, col_name_type.name});
            ++col_idx;
        }
    }

    size_t fallback_offset = 0;
    std::optional<size_t> last_processed_mark;

    while (read_rows < max_rows_to_read && from_mark < total_marks)
    {
        /// Postings are addressed per mark: rows past a mark's last row belong to the next mark
        /// and would resolve against the wrong posting lists.
        size_t mark_begin_row = index_granularity.getMarkStartingRow(from_mark);
        size_t mark_end_row = mark_begin_row + index_granularity.getMarkRows(from_mark);

        if (from_row < mark_begin_row || from_row >= mark_end_row)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Text index reader position is out of sync: row {} is outside of mark {} with rows [{}, {})",
                from_row, from_mark, mark_begin_row, mark_end_row);
        }

        size_t rows_left_in_mark = mark_end_row - from_row;
        size_t rows_to_read = std::min(rows_left_in_mark, max_rows_to_read - read_rows);

        /// In lazy mode skip per-mark Roaring Bitmap materialization — cursors decode on demand.
        std::vector<PostingList> mark_postings;

        if (!use_lazy_mode)
            mark_postings = buildPostingsForMark(from_mark, RowsRange(from_row, from_row + rows_to_read - 1));

        for (size_t i = 0; i < res_columns.size(); ++i)
        {
            /// Filled once for all rows read, after the loop.
            if (getFoldedPostings(i))
                continue;

            auto & column_mutable = *res_columns[i];

            if (is_always_true[i])
            {
                auto & column_data = assert_cast<ColumnUInt8 &>(column_mutable).getData();
                column_data.resize_fill(column_mutable.size() + rows_to_read, 1);
            }
            else if (use_fallback[i] && !fallback_block.empty())
            {
                fillColumnFallback(
                    column_mutable,
                    columns_to_read[i].name,
                    fallback_block,
                    fallback_offset,
                    rows_to_read);
            }
            else if (const auto & search_query = search_queries[i];
                     search_query && search_query->getSearchMode() == TextSearchMode::Phrase)
            {
                /// Phrase queries are resolved from positional data (.pos), not per-mark posting lists.
                applyPostingsPhrase(column_mutable, i, from_row, rows_to_read);
            }
            else if (use_lazy_mode)
            {
                fillColumnLazy(column_mutable, i, from_row, rows_to_read);
            }
            else
            {
                fillColumn(column_mutable, mark_postings[i], from_row, rows_to_read);
            }
        }

        from_row += rows_to_read;
        read_rows += rows_to_read;
        fallback_offset += rows_to_read;
        last_processed_mark = from_mark;

        if (from_row == mark_end_row)
            ++from_mark;
    }

    /// Remove blocks that are no longer needed; those covering the mark the next read continues in are kept.
    if (last_processed_mark)
    {
        if (auto rows_range = getRowsRangeForMark(*last_processed_mark))
            cleanupPostingsBlocks(*rows_range);
    }

    if (read_rows > 0)
    {
        for (size_t i = 0; i < res_columns.size(); ++i)
        {
            if (const auto * folded_postings = getFoldedPostings(i))
                fillColumnFromPostings(*res_columns[i], *folded_postings, from_row - read_rows, read_rows);
        }
    }

    current_mark = from_mark;
    current_row = from_row;
    return read_rows;
}

void MergeTreeReaderTextIndex::createEmptyColumns(MutableColumns & columns, size_t max_rows_to_read) const
{
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[i] == nullptr)
        {
            auto column = columns_to_read[i].type->createColumn(*serializations[i]);
            column->reserve(max_rows_to_read);
            columns[i] = std::move(column);
        }
    }
}

std::unique_ptr<MergeTreeReaderStream> MergeTreeReaderTextIndex::makePostingsStream(const TokenPostingsInfo & token_info) const
{
    return makeTextIndexInputStream(
        *data_part_info_for_read,
        index_read_task.index.index->getFileName(),
        index_read_task.index.index->getSubstreams()[2],
        settings,
        estimatePostingListBufferSize(token_info));
}

MergeTreeReaderStream & MergeTreeReaderTextIndex::getPostingsStream(std::string_view token, const TokenPostingsInfo & token_info)
{
    auto [it, inserted] = postings_streams.try_emplace(token);
    if (inserted)
        it->second = makePostingsStream(token_info);

    return *it->second;
}

std::optional<RowsRange> MergeTreeReaderTextIndex::getRowsRangeForMark(size_t mark) const
{
    const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
    size_t row_begin = index_granularity.getMarkStartingRow(mark);
    size_t row_end = index_granularity.getMarkStartingRow(mark + 1);

    if (row_begin == row_end)
        return {};

    return RowsRange(row_begin, row_end - 1);
}

std::vector<PostingList> MergeTreeReaderTextIndex::buildPostingsForMark(size_t mark, const RowsRange & slice_range)
{
    std::vector<PostingList> result(columns_to_read.size());
    auto mark_range = getRowsRangeForMark(mark);

    if (!mark_range.has_value())
        return result;

    /// Clip to `slice_range`, not the full mark, so postings stay in bounds on partial-mark
    /// reads (`max_rows_to_read` stops inside the mark).
    auto effective_range = mark_range->intersectWith(slice_range);
    if (!effective_range.has_value())
        return result;

    const auto & analyzer = granule->getAnalyzer();
    PostingList range_posting;

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        if (is_always_true[i] || use_fallback[i])
            continue;

        const auto & search_query = search_queries[i];
        if (search_query->getTokens().empty() && search_query->getPatterns().empty())
            continue;

        if (getFoldedPostings(i))
            continue;

        if (range_posting.isEmpty())
            range_posting.addRangeClosed(static_cast<UInt32>(effective_range->begin), static_cast<UInt32>(effective_range->end));

        /// Phrase queries are resolved from positional data (.pos) in applyPostingsPhrase,
        /// not from per-mark posting lists.
        if (search_query->getSearchMode() == TextSearchMode::Phrase)
            continue;

        result[i] = buildPostingsForQuery(*search_query, analyzer, *effective_range, range_posting);
    }

    return result;
}

PostingList MergeTreeReaderTextIndex::buildPostingsForQuery(
    const TextSearchQuery & query,
    const TextIndexAnalyzer & analyzer,
    const RowsRange & range,
    PostingList & range_posting)
{
    const auto & query_builder = analyzer.getQueryBuilder(query);
    if (query_builder.is_failed)
        return {};

    std::optional<PostingList> result;
    if (query_builder.postings)
        result = *query_builder.postings & range_posting;

    if (!query_builder.needReadPostings())
        return result.value_or(PostingList{});

    for (const auto & [token, token_info] : query_builder.tokens)
    {
        if (!analyzer.isTokenNeeded(token) || analyzer.hasReadPostings(token))
            continue;

        auto read_blocks = readPostingsBlocksForToken(token, *token_info, range);
        if (read_blocks.empty())
        {
            if (query.getSearchMode() == TextSearchMode::All)
                return {};
            else
                continue;
        }

        PostingList large_postings = (*read_blocks.front() & range_posting);
        for (size_t i = 1; i < read_blocks.size(); ++i)
            large_postings |= (*read_blocks[i] & range_posting);

        if (!result)
            result = std::move(large_postings);
        else if (query.getSearchMode() == TextSearchMode::All)
            *result &= large_postings;
        else if (query.getSearchMode() == TextSearchMode::Any)
            *result |= large_postings;

        if (query.getSearchMode() == TextSearchMode::All && result && result->isEmpty())
            return {};
    }

    return result.value_or(PostingList{});
}

std::vector<PostingListPtr> MergeTreeReaderTextIndex::readPostingsBlocksForToken(std::string_view token, const TokenPostingsInfo & token_info, const RowsRange & range)
{
    if (!postings_serialization.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Postings serialization is not set");

    auto blocks_to_read = token_info.getBlocksToRead(range);

    if (blocks_to_read.empty())
        return {};

    std::vector<PostingListPtr> result;
    auto & postings_stream = getPostingsStream(token, token_info);

    for (const auto & block_idx : blocks_to_read)
    {
        auto [it, inserted] = postings_blocks[token].try_emplace(block_idx);

        if (inserted)
        {
            it->second = MergeTreeIndexGranuleText::readPostingsBlock(
                postings_stream,
                *deserialization_state,
                token_info,
                block_idx,
                postings_serialization.value(),
                granule->getIndexIdForCaches());
        }

        result.push_back(it->second);
    }

    return result;
}

void MergeTreeReaderTextIndex::resetCursors()
{
    lazy_cursors.clear();
    resolved_searches.assign(resolved_searches.size(), {});
    for (auto & stream : phrase_streams)
        stream.reset();
}

void MergeTreeReaderTextIndex::cleanupPostingsBlocks(const RowsRange & range)
{
    if (!granule)
        return;

    const auto & analyzer = granule->getAnalyzer();
    const auto & token_infos = analyzer.getAllTokenInfos();

    for (const auto & [token, token_info] : token_infos)
    {
        auto it = postings_blocks.find(token);
        if (it == postings_blocks.end())
            continue;

        for (size_t i = 0; i < token_info->ranges.size(); ++i)
        {
            if (!token_info->ranges[i].intersects(range))
                it->second.erase(i);
        }
    }
}

void MergeTreeReaderTextIndex::fillColumn(IColumn & column, const PostingList & postings, size_t row_offset, size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    size_t old_size = column_data.size();
    column_data.resize_fill(old_size + num_rows, 0);

    size_t cardinality = postings.cardinality();
    if (cardinality == 0)
        return;

    indices_buffer.resize(cardinality);
    postings.toUint32Array(indices_buffer.data());

    for (size_t i = 0; i < cardinality; ++i)
    {
        size_t relative_row_number = indices_buffer[i] - row_offset;
        chassert(relative_row_number < num_rows);
        column_data[old_size + relative_row_number] = 1;
    }
}

const PostingList * MergeTreeReaderTextIndex::getFoldedPostings(size_t column_idx) const
{
    if (use_lazy_mode || is_always_true[column_idx] || use_fallback[column_idx])
        return nullptr;

    const auto & search_query = search_queries[column_idx];
    if (search_query->getSearchMode() == TextSearchMode::Phrase)
        return nullptr;

    if (search_query->getTokens().empty() && search_query->getPatterns().empty())
        return &empty_postings;

    const auto & query_builder = granule->getAnalyzer().getQueryBuilder(*search_query);
    if (query_builder.is_failed)
        return &empty_postings;

    if (query_builder.needReadPostings())
        return nullptr;

    return query_builder.postings ? &*query_builder.postings : &empty_postings;
}

void MergeTreeReaderTextIndex::fillColumnFromPostings(IColumn & column, const PostingList & postings, size_t row_offset, size_t num_rows)
{
    ProfileEvents::increment(ProfileEvents::TextIndexFilledFromFoldedPostings);

    /// Posting-list row ids are 32-bit, so a larger offset cannot legitimately occur.
    if (row_offset > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index posting lists do not support row offset larger than UINT32_MAX, got {}", row_offset);

    PostingList range;
    range.addRangeClosed(static_cast<UInt32>(row_offset), static_cast<UInt32>(std::min<size_t>(row_offset + num_rows - 1, std::numeric_limits<UInt32>::max())));
    fillColumn(column, postings & range, row_offset, num_rows);
}

MergeTreeReaderTextIndex::ResolvedSearch MergeTreeReaderTextIndex::resolveSearch(size_t column_idx)
{
    ResolvedSearch resolved;

    const auto & search_query = search_queries[column_idx];
    chassert(search_query);
    chassert(search_query->getPatterns().empty());

    /// Fill zeros without tokens.
    if (search_query->getTokens().empty())
        return resolved;

    const auto & analyzer = granule->getAnalyzer();
    const auto & query_builder = analyzer.getQueryBuilder(*search_query);

    if (query_builder.is_failed)
        return resolved;

    if (query_builder.needReadPostings())
    {
        for (const auto & [token, token_info] : query_builder.tokens)
        {
            if (!analyzer.isTokenNeeded(token) || analyzer.hasReadPostings(token))
                continue;

            auto cursor = makeLazyCursor(token, *token_info);
            resolved.cursors.emplace_back(cursor.get());
            lazy_cursors.emplace_back(std::move(cursor));
        }
    }

    if (query_builder.postings && !query_builder.postings->isEmpty())
    {
        /// Build a cursor over the sorted array of postings, shared by all readers of the granule.
        auto cursor = std::make_shared<PostingListCursor>(query_builder.getFlatPostings());
        resolved.cursors.emplace_back(cursor.get());
        lazy_cursors.emplace_back(std::move(cursor));
    }

    if (resolved.cursors.empty())
        return resolved;

    resolved.mode = search_query->getSearchMode();

    if (resolved.mode == TextSearchMode::Any)
    {
        sortCursorsForUnion(resolved.cursors);
    }
    else if (resolved.mode == TextSearchMode::All)
    {
        sortCursorsForIntersection(resolved.cursors);
        resolved.intersection_algorithm = chooseIntersectionAlgorithm(resolved.cursors, intersection_algorithm);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid search mode: {}", resolved.mode);
    }

    return resolved;
}

void MergeTreeReaderTextIndex::fillColumnLazy(IColumn & column, size_t column_idx, size_t row_offset, size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    size_t old_size = column_data.size();
    auto & resolved = resolved_searches[column_idx];

    if (!resolved.has_value())
        resolved = resolveSearch(column_idx);

    column_data.resize_fill(old_size + num_rows, 0);

    if (resolved->cursors.empty())
        return;

    if (resolved->mode == TextSearchMode::Any)
        lazyUnionPostingLists(column, resolved->cursors, old_size, row_offset, num_rows);
    else
        lazyIntersectPostingLists(column, resolved->cursors, old_size, row_offset, num_rows, resolved->intersection_algorithm, lazy_postings_stats);
}

struct MergeTreeReaderTextIndex::PhraseTokens
{
    std::vector<std::string_view> unique_tokens;
    std::vector<const TokenPostingsInfo *> unique_infos;
    std::vector<size_t> term_to_unique;

    /// Nothing to match if a token is missing from the granule or carries no positions.
    static std::optional<PhraseTokens> resolve(
        const TokenToPostingsInfosMap & all_token_infos, const VectorWithMemoryTracking<String> & phrase_tokens)
    {
        PhraseTokens phrase;
        phrase.term_to_unique.reserve(phrase_tokens.size());
        for (const auto & token : phrase_tokens)
        {
            auto it = all_token_infos.find(token);
            if (it == all_token_infos.end() || !(it->second->header & PostingsSerialization::Flags::HasPositions))
                return {};

            size_t unique_idx = 0;
            while (unique_idx < phrase.unique_tokens.size() && phrase.unique_tokens[unique_idx] != token)
                ++unique_idx;
            if (unique_idx == phrase.unique_tokens.size())
            {
                phrase.unique_tokens.emplace_back(it->first);
                phrase.unique_infos.push_back(it->second.get());
            }
            phrase.term_to_unique.push_back(unique_idx);
        }
        return phrase;
    }

    bool canUseRankCursors() const
    {
        return std::ranges::all_of(unique_infos, [](const TokenPostingsInfo * info) { return canUseRankCursor(*info); });
    }
};

namespace
{

TextIndexBlockedPositionsCodec::Directory readPositionsDirectory(
    MergeTreeReaderStream & stream, const TokenPostingsInfo & token_info, UInt64 expected_num_docs)
{
    /// Checked before seeking: an offset outside the stream would leave the buffer out of range.
    validateTokenPositions(token_info, stream.getFileSize());
    stream.seekToMark({token_info.position_offset, 0});
    return TextIndexBlockedPositionsCodec::readDirectory(
        *stream.getDataBuffer(), token_info.position_offset, expected_num_docs, token_info.position_bytes);
}

/// Counters of one phrase search, added to the profile events in the destructor.
struct PhraseSearchStats
{
    PhraseSearchStats() = default;
    PhraseSearchStats(const PhraseSearchStats &) = delete;
    PhraseSearchStats & operator=(const PhraseSearchStats &) = delete;

    ~PhraseSearchStats()
    {
        ProfileEvents::increment(ProfileEvents::TextIndexPhraseCandidates, candidates);
        ProfileEvents::increment(ProfileEvents::TextIndexPhraseCandidatesMicroseconds, candidates_us);
        ProfileEvents::increment(ProfileEvents::TextIndexPositionsDecodeMicroseconds, directory_us + decode_us);
        ProfileEvents::increment(ProfileEvents::TextIndexPhraseMatchMicroseconds, match_us);
        ProfileEvents::increment(ProfileEvents::TextIndexPositionsBlocksRead, blocks_read);
        ProfileEvents::increment(ProfileEvents::TextIndexPositionsBlocksTotal, blocks_total);
        ProfileEvents::increment(ProfileEvents::TextIndexPositionsBytesRead, bytes_read);
    }

    size_t candidates = 0;
    UInt64 candidates_us = 0;
    UInt64 directory_us = 0;
    UInt64 decode_us = 0;
    UInt64 match_us = 0;
    size_t blocks_read = 0;
    size_t blocks_total = 0;
    UInt64 bytes_read = 0;
};

/// Decodes a chunk of candidates' positions per unique token, then runs the phrase adjacency match.
/// Ranks ascend within a chunk, so blocks decode in ascending order and reseek only on a block gap.
/// Each token's last parsed block is kept, as the next chunk or window may need it again.
class PhraseChunkMatcher
{
public:
    PhraseChunkMatcher(
        MergeTreeReaderStream & stream_,
        const std::vector<TextIndexBlockedPositionsCodec::Directory> & dirs_,
        const std::vector<size_t> & term_to_unique_,
        PhraseSearchStats & stats_)
        : stream(stream_)
        , data_buffer(*stream_.getDataBuffer())
        , dirs(dirs_)
        , term_to_unique(term_to_unique_)
        , stats(stats_)
        , offsets(dirs_.size())
        , positions(dirs_.size())
        , scratches(dirs_.size())
        , parsed_blocks(dirs_.size(), std::numeric_limits<size_t>::max())
    {
    }

    /// ranks[u] holds the candidates' ranks in token u's postings, aligned with `candidates`.
    void match(
        std::span<const UInt32> candidates,
        const std::vector<std::span<const UInt64>> & ranks,
        PaddedPODArray<UInt32> & matching)
    {
        Stopwatch decode_watch;
        for (size_t u = 0; u < dirs.size(); ++u)
            decodeToken(u, ranks[u]);
        stats.decode_us += decode_watch.elapsedMicroseconds();

        Stopwatch match_watch;
        TextIndexPhraseSearch::matchCandidatePositions(candidates, offsets, positions, term_to_unique, matching);
        stats.match_us += match_watch.elapsedMicroseconds();
    }

    static constexpr size_t CHUNK = 1 << 16;
    /// Rows per cached phrase result.
    static constexpr size_t PHRASE_CACHE_CHUNK_ROWS = 1 << 14;

private:
    void decodeToken(size_t u, std::span<const UInt64> ranks)
    {
        const auto & dir = dirs[u];
        auto & token_offsets = offsets[u];
        auto & token_positions = positions[u];
        token_offsets.clear();
        token_positions.clear();
        token_offsets.push_back(0);

        size_t previous_read = std::numeric_limits<size_t>::max();
        for (size_t idx = 0; idx < ranks.size();)
        {
            const size_t block_idx = ranks[idx] / TextIndexBlockedPositionsCodec::BLOCK_DOCS;
            if (block_idx >= dir.numBlocks())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupt text index positions: rank {} is outside the token's {} blocks", ranks[idx], dir.numBlocks());
            local_ranks.clear();
            do
            {
                local_ranks.push_back(static_cast<UInt32>(ranks[idx] % TextIndexBlockedPositionsCodec::BLOCK_DOCS));
                ++idx;
            } while (idx < ranks.size() && ranks[idx] / TextIndexBlockedPositionsCodec::BLOCK_DOCS == block_idx);

            if (block_idx != parsed_blocks[u])
            {
                if (previous_read == std::numeric_limits<size_t>::max() || block_idx != previous_read + 1)
                    stream.seekToMark({dir.block_offsets[block_idx], 0});
                TextIndexBlockedPositionsCodec::readBlock(data_buffer, dir, block_idx, scratches[u]);
                parsed_blocks[u] = block_idx;
                previous_read = block_idx;
                ++stats.blocks_read;
                stats.bytes_read += dir.block_offsets[block_idx + 1] - dir.block_offsets[block_idx];
            }
            TextIndexBlockedPositionsCodec::emitRanks(scratches[u], local_ranks, token_offsets, token_positions);
        }
    }

    MergeTreeReaderStream & stream;
    ReadBuffer & data_buffer;
    const std::vector<TextIndexBlockedPositionsCodec::Directory> & dirs;
    const std::vector<size_t> & term_to_unique;
    PhraseSearchStats & stats;

    std::vector<PaddedPODArray<UInt32>> offsets;
    std::vector<PaddedPODArray<UInt32>> positions;
    std::vector<TextIndexBlockedPositionsCodec::DecodeScratch> scratches;
    std::vector<size_t> parsed_blocks;
    std::vector<UInt32> local_ranks;
};

}

std::vector<PostingListCursorPtr> MergeTreeReaderTextIndex::makePhraseCursors(const PhraseTokens & phrase)
{
    std::vector<PostingListCursorPtr> cursors;
    cursors.reserve(phrase.unique_tokens.size());
    for (size_t u = 0; u < phrase.unique_tokens.size(); ++u)
    {
        const auto & token = phrase.unique_tokens[u];
        const auto & token_info = *phrase.unique_infos[u];
        if (token_info.header & PostingsSerialization::Flags::RawPostings)
        {
            auto docs = std::make_shared<PaddedPODArray<UInt32>>();
            if (token_info.header & PostingsSerialization::Flags::EmbeddedPostings)
            {
                docs->assign(token_info.embedded_postings.begin(), token_info.embedded_postings.end());
            }
            else
            {
                std::unique_ptr<MergeTreeReaderStream> local_stream;
                MergeTreeReaderStream * stream = nullptr;
                if (auto it = postings_streams.find(token); it != postings_streams.end())
                    stream = it->second.get();
                else
                    stream = (local_stream = makePostingsStream(token_info)).get();

                stream->seekToMark({token_info.offsets[0], 0});
                postings_serialization->deserializeToArray(*stream->getDataBuffer(), token_info, 0, *docs);
            }
            cursors.push_back(std::make_shared<PostingListCursor>(std::move(docs)));
        }
        else
        {
            cursors.push_back(std::make_shared<PostingListCursor>(
                getPostingsStream(token, token_info), token_info, condition_text->postingsCache().get(), granule->getIndexIdForCaches()));
        }
    }
    return cursors;
}

/// The phrase search of one column, kept across `readRows` calls like the lazy cursors: each window intersects
/// the posting lists over its rows only and decodes the position blocks of its candidates.
struct MergeTreeReaderTextIndex::PhraseStream
{
    std::vector<size_t> term_to_unique;
    std::vector<TextIndexPostingsRankCursor> cursors;
    /// The same cursors, sorted for the leapfrog.
    std::vector<PostingListCursor *> leapfrog_cursors;
    std::vector<TextIndexBlockedPositionsCodec::Directory> directories;
    PhraseSearchStats stats;
    std::optional<PhraseChunkMatcher> matcher;

    PaddedPODArray<UInt32> candidates;
    std::vector<PaddedPODArray<UInt64>> ranks;
    std::vector<std::span<const UInt64>> rank_spans;
    /// The first row not passed by the cursors.
    size_t next_row = 0;

    /// A single-token phrase needs no positions: every row holding the token matches.
    void matchToken(UInt8 * out, size_t row_offset, size_t num_rows)
    {
        next_row = row_offset + num_rows;
        if (!cursors.empty())
            leapfrog_cursors.front()->linearOr(out, row_offset, num_rows);
    }

    /// Appends the rows of [begin, end) holding the phrase to `matching`, in ascending order.
    void match(size_t begin, size_t end, PaddedPODArray<UInt32> & matching)
    {
        next_row = end;
        if (cursors.empty())
            return;

        Stopwatch watch;
        const UInt64 nested_before_us = stats.decode_us + stats.match_us;

        leapfrog_cursors.front()->advance(static_cast<UInt32>(begin));
        leapfrogIntersect(leapfrog_cursors, end, [&](UInt32 doc)
        {
            candidates.push_back(doc);
            for (size_t u = 0; u < cursors.size(); ++u)
                ranks[u].push_back(cursors[u].rank());
            ++stats.candidates;

            if (candidates.size() == PhraseChunkMatcher::CHUNK)
                matchCandidates(matching);
        });
        matchCandidates(matching);

        const UInt64 nested_us = stats.decode_us + stats.match_us - nested_before_us;
        const UInt64 elapsed_us = watch.elapsedMicroseconds();
        stats.candidates_us += elapsed_us > nested_us ? elapsed_us - nested_us : 0;
    }

private:
    void matchCandidates(PaddedPODArray<UInt32> & matching)
    {
        if (candidates.empty())
            return;

        for (size_t u = 0; u < cursors.size(); ++u)
            rank_spans[u] = std::span<const UInt64>(ranks[u].data(), ranks[u].size());

        matcher->match(std::span<const UInt32>(candidates.data(), candidates.size()), rank_spans, matching);
        candidates.clear();

        for (auto & token_ranks : ranks)
            token_ranks.clear();
    }
};

std::shared_ptr<MergeTreeReaderTextIndex::PhraseStream> MergeTreeReaderTextIndex::makePhraseStream(const TextSearchQuery & search_query)
{
    auto stream = std::make_shared<PhraseStream>();

    auto phrase = PhraseTokens::resolve(granule->getAnalyzer().getAllTokenInfos(), search_query.getPhraseTokens());
    if (!phrase)
        return stream;

    if (!phrase->canUseRankCursors())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Phrase search over a posting list without a block index");

    /// The header deserialization rejects any codec but Blocked, so the part's positions
    /// are always the blocked candidate-driven layout here.
    chassert(static_cast<TextIndexPositionCodec::Encoding>(granule->getPositionsCodec()) == TextIndexPositionCodec::Encoding::BlockedPfor);
    ProfileEvents::increment(ProfileEvents::TextIndexPhraseSearches);
    ProfileEvents::increment(ProfileEvents::TextIndexPhraseCursorSearches);

    auto postings_cursors = makePhraseCursors(*phrase);
    const size_t num_tokens = postings_cursors.size();

    std::vector<PostingListCursor *> leapfrog_cursors;
    leapfrog_cursors.reserve(num_tokens);
    std::ranges::transform(postings_cursors, std::back_inserter(leapfrog_cursors), [](const auto & cursor) { return cursor.get(); });

    std::vector<TextIndexPostingsRankCursor> cursors;
    cursors.reserve(num_tokens);
    for (size_t u = 0; u < num_tokens; ++u)
    {
        if (phrase->unique_infos[u]->header & PostingsSerialization::Flags::RawPostings)
            cursors.emplace_back(std::move(postings_cursors[u]));
        else
            cursors.emplace_back(std::move(postings_cursors[u]), *phrase->unique_infos[u]);

        if (!cursors.back().valid())
            return stream;
    }

    if (phrase->term_to_unique.size() > 1)
    {
        /// The directory must hold as many documents as the cursor's checked posting list, which bounds every rank.
        Stopwatch directory_watch;
        stream->directories.resize(num_tokens);
        for (size_t u = 0; u < num_tokens; ++u)
        {
            stream->directories[u] = readPositionsDirectory(*positions_stream, *phrase->unique_infos[u], cursors[u].numDocuments());
            stream->stats.blocks_total += stream->directories[u].numBlocks();
        }
        stream->stats.directory_us = directory_watch.elapsedMicroseconds();
        sortCursorsForIntersection(leapfrog_cursors);
    }

    stream->term_to_unique = std::move(phrase->term_to_unique);
    stream->cursors = std::move(cursors);
    stream->leapfrog_cursors = std::move(leapfrog_cursors);
    stream->ranks.resize(num_tokens);
    stream->rank_spans.resize(num_tokens);
    stream->matcher.emplace(*positions_stream, stream->directories, stream->term_to_unique, stream->stats);
    return stream;
}

MergeTreeReaderTextIndex::PhraseStream & MergeTreeReaderTextIndex::getPhraseStream(size_t column_idx, size_t begin)
{
    /// The cursors only move forward, so a stream already past `begin` is rebuilt, e.g. after an evicted chunk.
    auto & stream = phrase_streams[column_idx];
    if (!stream || stream->next_row > begin)
        stream = makePhraseStream(*search_queries[column_idx]);
    return *stream;
}

void MergeTreeReaderTextIndex::applyPostingsPhrase(IColumn & column, size_t column_idx, size_t row_offset, size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    size_t column_offset = column_data.size();
    column_data.resize_fill(column_offset + num_rows, 0);

    const auto & search_query = *search_queries[column_idx];
    if (!positions_stream || search_query.getPhraseTokens().empty())
        return;

    if (granule->getAnalyzer().getQueryBuilder(search_query).is_failed)
        return;

    UInt8 * out = column_data.data() + column_offset;
    if (search_query.getPhraseTokens().size() == 1)
    {
        getPhraseStream(column_idx, row_offset).matchToken(out, row_offset, num_rows);
        return;
    }

    /// Results are cached per aligned chunk of rows, so windows of any shape share them.
    constexpr size_t chunk_rows = PhraseChunkMatcher::PHRASE_CACHE_CHUNK_ROWS;
    const size_t end = row_offset + num_rows;
    for (size_t chunk_begin = row_offset - row_offset % chunk_rows; chunk_begin < end; chunk_begin += chunk_rows)
    {
        auto key = TextIndexPostingsCache::hash(
            granule->getIndexIdForCaches(), search_query.getHash(), UInt64{chunk_begin}, static_cast<UInt8>(TextIndexPostingsCacheKind::Phrase));

        auto cell = condition_text->postingsCache()->getOrSet(key, [&]
        {
            auto matching = std::make_shared<PaddedPODArray<UInt32>>();
            getPhraseStream(column_idx, chunk_begin).match(chunk_begin, chunk_begin + chunk_rows, *matching);
            matching->shrink_to_fit();
            return std::make_shared<TextIndexPostingsCacheCell>(PaddedPODArrayPtr(std::move(matching)));
        });

        const auto & matching = *std::get<PaddedPODArrayPtr>(cell->value);
        for (const auto * it = std::lower_bound(matching.begin(), matching.end(), row_offset); it != matching.end() && *it < end; ++it)
            out[*it - row_offset] = 1;
    }
}

void MergeTreeReaderTextIndex::fillColumnFallback(
    IColumn & column,
    const String & column_name,
    const Block & physical_block,
    size_t offset,
    size_t num_rows) const
{
    auto it = fallback_expressions.find(column_name);
    chassert(it != fallback_expressions.end());

    /// Build a block slice for this granule: cut [offset, offset + num_rows) from each physical column.
    Block slice;
    for (const auto & col : physical_block)
        slice.insert({col.column->cut(offset, num_rows), col.type, col.name});

    /// Execute the virtual column's default expression (the original search predicate) on the slice.
    /// After execution the block contains both the physical columns and the computed virtual column.
    it->second->execute(slice);

    /// The predicate result can be sparse/const (inputs may be sparse), so make it full before the dense cast.
    const auto & result_col = slice.getByName(column_name);
    auto result_full = result_col.column->convertToFullIfWrapped();
    const auto & result_data = assert_cast<const ColumnUInt8 &>(*result_full).getData();
    chassert(result_data.size() == num_rows);

    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    const size_t old_size = column_data.size();
    column_data.resize(old_size + num_rows);
    memcpy(&column_data[old_size], result_data.data(), num_rows);
}

void MergeTreeReaderTextIndex::setPrecomputedGranule(const IndexGranulesMap & granules)
{
    auto it = granules.find(index_read_task.index.index->index.name);

    if (it != granules.end() && it->second)
    {
        resetCursors();
        postings_blocks.clear();
        setIndexGranule(it->second);
    }
}

MergeTreeReaderPtr createMergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader,
    const IndexReadTask & index_read_task,
    const NamesAndTypesList & columns_to_read,
    MergeTreeIndexGranulePtr index_granule)
{
    return std::make_unique<MergeTreeReaderTextIndex>(main_reader, index_read_task, columns_to_read, std::move(index_granule));
}

}
