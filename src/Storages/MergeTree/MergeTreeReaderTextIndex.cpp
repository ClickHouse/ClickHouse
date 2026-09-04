#include <Columns/ColumnsCommon.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexAnalyzer.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <Storages/MergeTree/TextIndexPhraseSearch.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
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

namespace ProfileEvents
{
    extern const Event TextIndexReaderTotalMicroseconds;
    extern const Event TextIndexPositionsDecodeMicroseconds;
    extern const Event TextIndexPhraseMatchMicroseconds;
    extern const Event TextIndexPositionsBlocksRead;
    extern const Event TextIndexPositionsBlocksTotal;
    extern const Event TextIndexPositionsBytesRead;
    extern const Event TextIndexPhraseCandidates;
    extern const Event TextIndexPhraseSearches;
    extern const Event TextIndexPhraseFallbacks;
}

namespace DB
{

namespace Setting
{
    extern const SettingsTextIndexPostingListApplyMode text_index_posting_list_apply_mode;
    extern const SettingsFloat text_index_lazy_intersection_density_threshold;
    extern const SettingsFloat text_index_hint_max_selectivity;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

MergeTreeReaderTextIndex::MergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader_,
    MergeTreeIndexWithCondition index_,
    NamesAndTypesList columns_,
    MergeTreeIndexGranulePtr index_granule_)
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
    , condition_text(std::dynamic_pointer_cast<MergeTreeIndexConditionText>(index.condition_template->generateUnsubstituted()))
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

        search_queries.push_back(condition_text->getSearchQueryForVirtualColumn(column.name));
    }

    lazy_cursors.resize(columns_.size());
    prebuilt_cursors.resize(columns_.size());

    auto data_part = getDataPart();
    auto index_format = index.index->getDeserializedFormat(*data_part, index.index->getFileName());
    chassert(index_format);

    MergeTreeIndexDeserializationState state
    {
        .version = index_format.version,
        .condition = condition_text.get(),
        .part_info = *data_part_info_for_read,
        .index = *index.index,
        .readable_ranges = nullptr,
        .skip_postings_deserialization = false,
    };

    deserialization_state = std::make_unique<MergeTreeIndexDeserializationState>(std::move(state));

    /// Lazy mode is requested per query; actual support is determined from the on-disk sparse-index header.
    const auto & ctx_settings = condition_text->getContext()->getSettingsRef();
    const auto apply_mode = ctx_settings[Setting::text_index_posting_list_apply_mode].value;

    lazy_mode_requested = (apply_mode == TextIndexPostingListApplyMode::LAZY);
    lazy_intersection_density_threshold = ctx_settings[Setting::text_index_lazy_intersection_density_threshold].value;

    if (!std::isfinite(lazy_intersection_density_threshold) || lazy_intersection_density_threshold < 0.0f || lazy_intersection_density_threshold > 1.0f)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting text_index_lazy_intersection_density_threshold must be a value in [0.0, 1.0], got {}", lazy_intersection_density_threshold);

    if (index_granule_)
        setIndexGranule(std::move(index_granule_));

    initializeFallbackReader(main_reader_);
}

void MergeTreeReaderTextIndex::setIndexGranule(MergeTreeIndexGranulePtr index_granule)
{
    chassert(index_granule);
    granule = std::dynamic_pointer_cast<const MergeTreeIndexGranuleText>(index_granule);
    /// Phrase search results are cached per granule; drop them when the granule changes.
    phrase_search_doc_ids.clear();
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
    bool has_fallback_candidates = condition_text->hasSearchPatterns()
        || std::ranges::any_of(
            search_queries,
            [](const auto & search_query)
            {
                return search_query && search_query->getSearchMode() == TextSearchMode::Phrase
                    && search_query->getDirectReadMode() == TextIndexDirectReadMode::Exact;
            });

    if (!has_fallback_candidates)
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
        if (!search_query)
            continue;

        bool needs_fallback = !search_query->getPatterns().empty()
            || (search_query->getSearchMode() == TextSearchMode::Phrase && search_query->getDirectReadMode() == TextIndexDirectReadMode::Exact);
        if (!needs_fallback)
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
        fallback_reader = createMergeTreeReader(
            main_reader->data_part_info_for_read,
            fallback_columns_list,
            main_reader->storage_snapshot,
            main_reader->storage_settings,
            main_reader->all_mark_ranges,
            /*virtual_fields=*/{},
            main_reader->uncompressed_cache,
            main_reader->mark_cache,
            /*deserialization_prefixes_cache=*/nullptr,
            main_reader->settings,
            /*avg_value_size_hints=*/{},
            /*profile_callback=*/{});
    }
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
    auto substreams = index.index->getSubstreams();
    auto data_part = getDataPart();

    LOG_TRACE(getLogger("MergeTreeReaderTextIndex"), "Reading text index granule for data part '{}'", data_part->getDataPartStorage().getFullPath());

    auto sparse_index_stream = makeTextIndexStream(substreams[0]);
    auto dictionary_stream = makeTextIndexStream(substreams[1]);
    small_postings_stream = makeTextIndexStream(substreams[2]);

    sparse_index_stream->seekToStart();
    resetCursors();

    MergeTreeIndexInputStreams streams;
    streams[MergeTreeIndexSubstream::Type::Regular] = sparse_index_stream.get();
    streams[MergeTreeIndexSubstream::Type::TextIndexDictionary] = dictionary_stream.get();
    streams[MergeTreeIndexSubstream::Type::TextIndexPostings] = small_postings_stream.get();

    auto granule_ptr = index.index->createIndexGranule();
    granule_ptr->deserializeBinaryWithMultipleStreams(streams, *deserialization_state);
    setIndexGranule(std::move(granule_ptr));
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

void MergeTreeReaderTextIndex::initializePostingStreams()
{
    const auto & analyzer = granule->getAnalyzer();
    const auto & token_infos = analyzer.getAllTokenInfos();

    auto data_part = getDataPart();
    auto substream = index.index->getSubstreams()[2];

    for (const auto & [token, token_info] : token_infos)
    {
        if (analyzer.isTokenNeeded(token) && !analyzer.hasReadPostings(token))
            large_postings_streams.emplace(token, makeTextIndexStream(substream));
    }
}

PostingListCursorPtr MergeTreeReaderTextIndex::makeLazyCursor(std::string_view token, const TokenPostingsInfo & token_info)
{
    if (!(token_info.header & PostingsSerialization::Flags::IsCompressed))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected token for lazy mode: {}. Multi-block postings must be compressed", token);

    auto * postings_cache = condition_text->postingsCache().get();
    const auto & index_id_for_cache = granule->getIndexIdForCaches();

    auto stream_it = large_postings_streams.find(token);
    if (stream_it != large_postings_streams.end())
        return std::make_shared<PostingListCursor>(*stream_it->second, token_info, postings_cache, index_id_for_cache);

    if (!small_postings_stream)
        small_postings_stream = makeTextIndexStream(index.index->getSubstreams()[2]);

    return std::make_shared<PostingListCursor>(*small_postings_stream, token_info, postings_cache, index_id_for_cache);
}

void MergeTreeReaderTextIndex::initializePositionsStream()
{
    const auto & data_part = getDataPart();

    auto index_format = index.index->getDeserializedFormat(*data_part, index.index->getFileName());
    if (index_format.version != 2)
        return;

    const auto positions_substream = std::ranges::find_if(
        index_format.substreams,
        [](const auto & substream) { return substream.type == MergeTreeIndexSubstream::Type::TextIndexPositions; });

    if (positions_substream == index_format.substreams.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index format V2 has no positions substream for index `{}`", index.index->index.name);

    positions_stream = makeTextIndexInputStream(
        data_part->getDataPartStoragePtr(),
        index.index->getFileName() + positions_substream->suffix,
        positions_substream->extension,
        MergeTreeIndexReader::patchSettings(settings, positions_substream->type));

    positions_stream->seekToStart();
}

size_t MergeTreeReaderTextIndex::readRows(
    size_t from_mark,
    bool continue_reading,
    size_t max_rows_to_read,
    MutableColumns & res_columns)
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
        /// Backward jump invalidates the per-token cursor cache: cached cursors are
        /// forward-only (their `linearOr` / `linearAnd` / `advance` walk segments from
        /// `current_segment_idx` onward), so they cannot serve an earlier row.
        if (from_mark < current_mark)
            resetCursors();

        from_row = index_granularity.getMarkStartingRow(from_mark);
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
    createEmptyColumns(res_columns, max_rows_to_read);
    size_t total_marks = data_part_info_for_read->getIndexGranularity().getMarksCountWithoutFinal();

    if (!is_initialized && max_rows_to_read > 0)
    {
        /// Granule may be not set in the distributed index analysis.
        /// TODO: implement distributed index analysis for text index.
        if (!granule)
            readGranule();

        is_initialized = true;
        classifyVirtualColumns();
        initializePostingStreams();
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
        fallback_reader->readRows(from_mark, continue_reading, max_rows_to_read, fallback_cols);
        size_t col_idx = 0;
        for (const auto & col_name_type : fallback_columns_list)
            fallback_block.insert({std::move(fallback_cols[col_idx++]), col_name_type.type, col_name_type.name});
    }

    size_t fallback_offset = 0;

    while (read_rows < max_rows_to_read && from_mark < total_marks)
    {
        /// When the number of rows in a part is smaller than `index_granularity`,
        /// `MergeTreeReaderTextIndex` must ensure that the virtual column it reads
        /// contains no more data rows than actually exist in the part
        size_t rows_to_read = std::min(index_granularity.getMarkRows(from_mark), max_rows_to_read - read_rows);

        /// In lazy mode skip per-mark Roaring Bitmap materialization — cursors decode on demand.
        PostingList range_posting;
        std::vector<PostingList> mark_postings;

        if (!use_lazy_mode)
            mark_postings = buildPostingsForMark(from_mark, RowsRange(from_row, from_row + rows_to_read - 1), range_posting);

        for (size_t i = 0; i < res_columns.size(); ++i)
        {
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
                applyPostingsPhrase(column_mutable, search_query, from_row, rows_to_read);
            }
            else if (use_lazy_mode)
            {
                fillColumnLazy(column_mutable, i, from_row, rows_to_read, range_posting);
            }
            else
            {
                fillColumn(column_mutable, mark_postings[i], from_row, rows_to_read);
            }
        }

        ++from_mark;
        from_row += rows_to_read;
        read_rows += rows_to_read;
        fallback_offset += rows_to_read;
    }

    /// Remove blocks that are no longer needed.
    if (auto rows_range = getRowsRangeForMark(from_mark - 1))
        cleanupPostingsBlocks(*rows_range);

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

std::unique_ptr<MergeTreeReaderStream> MergeTreeReaderTextIndex::makeTextIndexStream(const MergeTreeIndexSubstream & substream) const
{
    auto data_part = getDataPart();

    return makeTextIndexInputStream(
        data_part->getDataPartStoragePtr(),
        index.index->getFileName() + substream.suffix,
        substream.extension,
        MergeTreeIndexReader::patchSettings(settings, substream.type));
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

std::vector<PostingList> MergeTreeReaderTextIndex::buildPostingsForMark(size_t mark, const RowsRange & slice_range, PostingList & range_posting)
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
    range_posting.addRangeClosed(static_cast<UInt32>(effective_range->begin), static_cast<UInt32>(effective_range->end));

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        if (is_always_true[i] || use_fallback[i])
            continue;

        const auto & search_query = search_queries[i];
        if (search_query->getTokens().empty() && search_query->getPatterns().empty())
            continue;

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
        if (!large_postings_streams.contains(token))
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
    for (const auto & block_idx : blocks_to_read)
    {
        auto * postings_stream = large_postings_streams.at(token).get();
        auto [it, inserted] = postings_blocks[token].try_emplace(block_idx);

        if (inserted)
        {
            it->second = MergeTreeIndexGranuleText::readPostingsBlock(
                *postings_stream,
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
    lazy_cursors.assign(lazy_cursors.size(), {});
    prebuilt_cursors.assign(prebuilt_cursors.size(), {});
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

void MergeTreeReaderTextIndex::fillColumnLazy(IColumn & column, size_t column_idx, size_t row_offset, size_t num_rows, PostingList & range_posting)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    size_t old_size = column_data.size();

    const auto & search_query = search_queries[column_idx];
    chassert(search_query->getPatterns().empty());

    if (search_query->getTokens().empty())
    {
        /// hasAnyTokens / hasAllTokens whose needle tokens were all dropped (e.g. by a postprocessor): no
        /// match, so fill zeros for every row read, matching fillColumn and the row-scan path.
        column_data.resize_fill(old_size + num_rows, 0);
        return;
    }

    const auto & analyzer = granule->getAnalyzer();
    const auto & query_builder = analyzer.getQueryBuilder(*search_query);

    if (query_builder.is_failed)
    {
        column_data.resize_fill(old_size + num_rows, 0);
        return;
    }

    std::vector<PostingListCursorPtr> cursors;
    cursors.reserve(query_builder.tokens.size());

    if (query_builder.needReadPostings())
    {
        auto & column_cursors = lazy_cursors[column_idx];

        for (const auto & [token, token_info] : query_builder.tokens)
        {
            if (analyzer.hasReadPostings(token))
                continue;

            auto [it, inserted] = column_cursors.try_emplace(token);

            if (inserted)
                it->second = makeLazyCursor(token, *token_info);

            cursors.push_back(it->second);
        }
    }

    if (query_builder.postings)
    {
        /// Check the per-column cache first: the prebuilt cursor is built once and reused across marks.
        auto & prebuilt_cursor = prebuilt_cursors[column_idx];

        if (prebuilt_cursor)
        {
            cursors.push_back(prebuilt_cursor);
        }
        else if (!query_builder.postings->isEmpty())
        {
            /// If there are no cursors for large postings, fill the column directly from the postings.
            if (cursors.empty())
            {
                if (range_posting.isEmpty())
                {
                    requireRowOffsetRepresentable(row_offset);
                    auto range_end = static_cast<UInt32>(std::min<size_t>(row_offset + num_rows - 1, std::numeric_limits<UInt32>::max()));
                    range_posting.addRangeClosed(static_cast<UInt32>(row_offset), range_end);
                }

                PostingList clipped = *query_builder.postings & range_posting;
                fillColumn(column, clipped, row_offset, num_rows);
                return;
            }

            /// Convert postings to a sorted array and build a cursor from it.
            auto key = TextIndexPostingsCache::hash(granule->getIndexIdForCaches(), columns_to_read[column_idx].name, static_cast<UInt8>(TextIndexPostingsCacheKind::Flat));

            auto cell = condition_text->postingsCache()->getOrSet(key, [&]
            {
                auto flat = std::make_shared<PaddedPODArray<UInt32>>(query_builder.postings->cardinality());
                query_builder.postings->toUint32Array(flat->data());
                return std::make_shared<TextIndexPostingsCacheCell>(std::move(flat));
            });

            prebuilt_cursor = std::make_shared<PostingListCursor>(std::get<FlatPostingsPtr>(cell->value));
            cursors.push_back(prebuilt_cursor);
        }
    }

    column_data.resize_fill(old_size + num_rows, 0);

    if (cursors.empty())
        return;

    if (search_query->getSearchMode() == TextSearchMode::Any)
        lazyUnionPostingLists(column, cursors, old_size, row_offset, num_rows);
    else if (search_query->getSearchMode() == TextSearchMode::All)
        lazyIntersectPostingLists(column, cursors, old_size, row_offset, num_rows, lazy_intersection_density_threshold);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid search mode: {}", search_query->getSearchMode());
}

PostingList MergeTreeReaderTextIndex::readAllPostingsForToken(std::string_view token, const TokenPostingsInfo & token_info)
{
    if (token_info.header & PostingsSerialization::Flags::EmbeddedPostings)
    {
        /// Embedded postings are stored as a flat sorted array in the dictionary.
        PostingList result;
        result.addMany(token_info.embedded_postings.size(), token_info.embedded_postings.data());
        return result;
    }

    if (!postings_serialization.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Postings serialization is not set");

    const size_t num_rows_in_part = data_part_info_for_read->getRowCount();
    const RowsRange full_range(0, num_rows_in_part ? num_rows_in_part - 1 : 0);
    const auto blocks_to_read = token_info.getBlocksToRead(full_range);

    PostingList result;
    for (const auto & block_idx : blocks_to_read)
    {
        MergeTreeReaderStream * postings_stream = nullptr;
        if (auto stream_it = large_postings_streams.find(token); stream_it != large_postings_streams.end())
        {
            postings_stream = stream_it->second.get();
        }
        else
        {
            if (!small_postings_stream)
                small_postings_stream = makeTextIndexStream(index.index->getSubstreams()[2]);
            postings_stream = small_postings_stream.get();
        }

        auto [it, inserted] = postings_blocks[token].try_emplace(block_idx);
        if (inserted)
        {
            it->second = MergeTreeIndexGranuleText::readPostingsBlock(
                *postings_stream,
                *deserialization_state,
                token_info,
                block_idx,
                postings_serialization.value(),
                granule->getIndexIdForCaches());
        }

        result |= *it->second;
    }

    return result;
}

PaddedPODArray<UInt32> MergeTreeReaderTextIndex::phraseSearchBlocked(const TextSearchQuery & search_query)
{
    const auto & all_token_infos = granule->getAnalyzer().getAllTokenInfos();
    const auto & phrase_tokens = search_query.getPhraseTokens();

    /// Repeated phrase terms reuse one posting list and one decoded position stream.
    std::vector<std::string_view> unique_tokens;
    std::vector<const TokenPostingsInfo *> unique_infos;
    std::vector<size_t> term_to_unique;
    term_to_unique.reserve(phrase_tokens.size());
    for (const auto & token : phrase_tokens)
    {
        auto it = all_token_infos.find(token);
        if (it == all_token_infos.end() || !(it->second->header & PostingsSerialization::Flags::HasPositions))
            return {};

        size_t unique_idx = 0;
        while (unique_idx < unique_tokens.size() && unique_tokens[unique_idx] != token)
            ++unique_idx;
        if (unique_idx == unique_tokens.size())
        {
            unique_tokens.emplace_back(token);
            unique_infos.push_back(it->second.get());
        }
        term_to_unique.push_back(unique_idx);
    }

    /// Candidate rows = intersection of the phrase tokens' postings. The full per-token posting
    /// list is also the rank space the blocked position stream is addressed in.
    std::vector<PostingList> token_postings;
    token_postings.reserve(unique_tokens.size());
    for (size_t u = 0; u < unique_tokens.size(); ++u)
    {
        token_postings.push_back(readAllPostingsForToken(unique_tokens[u], *unique_infos[u]));
        if (token_postings.back().cardinality() == 0)
            return {};
    }

    PostingList intersection = token_postings[0];
    for (size_t u = 1; u < token_postings.size(); ++u)
    {
        intersection &= token_postings[u];
        if (intersection.cardinality() == 0)
            return {};
    }

    PaddedPODArray<UInt32> candidates(intersection.cardinality());
    intersection.toUint32Array(candidates.data());
    ProfileEvents::increment(ProfileEvents::TextIndexPhraseCandidates, candidates.size());

    /// A single-term "phrase" needs no positional check: every row containing the token matches.
    if (term_to_unique.size() == 1)
        return candidates;

    /// Bounded-memory chunked phrase match: precompute per-token candidate ranks, then process
    /// candidates in fixed chunks. Per chunk, decode only that chunk's covering blocks per token
    /// (token-sequential; consecutive blocks skip the reseek) into small reused buffers, run the
    /// two-pointer adjacency with per-candidate early-exit, and keep only matching doc ids. The full
    /// candidate position set is never materialized (the old per-token arrays cost ~GiB per phrase).
    const size_t pos_file_size = positions_stream->getFileSize();
    auto * data_buffer = positions_stream->getDataBuffer();

    std::vector<TextIndexBlockedPositionsCodec::Directory> dirs(unique_tokens.size());
    std::vector<PaddedPODArray<UInt64>> candidate_ranks(unique_tokens.size());
    size_t blocks_total = 0;
    UInt64 decode_us = 0;
    {
        Stopwatch prep_watch;
        PaddedPODArray<UInt32> posting_docs;
        for (size_t u = 0; u < unique_tokens.size(); ++u)
        {
            const auto & token_info = *unique_infos[u];
            /// Checked before seeking: an offset outside the stream would leave the buffer out of range.
            if ((token_info.position_bytes == 0) || (token_info.position_offset > pos_file_size)
                || (token_info.position_bytes > pos_file_size - token_info.position_offset))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Corrupt text index positions: blob of {} bytes at offset {} is outside the {}-byte stream",
                    token_info.position_bytes, token_info.position_offset, pos_file_size);
            positions_stream->seekToMark({token_info.position_offset, 0});
            const size_t available = token_info.position_bytes;
            /// Candidate ranks in this token's postings. Dense candidates: one linear walk over the
            /// materialized list beats per-candidate roaring rank(); sparse: rank() wins.
            const auto & postings = token_postings[u];

            /// readDirectory rejects a blob whose document count disagrees with the postings. That
            /// equality is what bounds a rank below num_docs, so the block index needs no check.
            dirs[u] = TextIndexBlockedPositionsCodec::readDirectory(
                *data_buffer, token_info.position_offset, postings.cardinality(), available);
            blocks_total += dirs[u].numBlocks();

            auto & ranks = candidate_ranks[u];
            ranks.resize(candidates.size());
            if (const UInt64 postings_cardinality = postings.cardinality(); candidates.size() * 16 >= postings_cardinality)
            {
                posting_docs.resize(postings_cardinality);
                postings.toUint32Array(posting_docs.data());
                size_t doc_idx = 0;
                for (size_t i = 0; i < candidates.size(); ++i)
                {
                    while (posting_docs[doc_idx] < candidates[i])
                        ++doc_idx;
                    ranks[i] = doc_idx; /// candidates are members: posting_docs[doc_idx] == candidates[i]
                }
            }
            else
            {
                /// roaring rank() is 1-based for the smallest element; candidates are members.
                for (size_t i = 0; i < candidates.size(); ++i)
                    ranks[i] = postings.rank(candidates[i]) - 1;
            }
        }
        decode_us += prep_watch.elapsedMicroseconds();
    }

    size_t blocks_read = 0;
    UInt64 block_bytes_read = 0;
    UInt64 block_decode_us = 0;
    std::vector<UInt32> block_local_ranks;

    /// Decode candidates [lo, hi) of token `u` into offsets/positions (offsets seeded with a leading
    /// 0; indices chunk-relative). Blocks decode in ascending order, reseeking only on a block gap.
    auto decode_chunk = [&](size_t u, size_t lo, size_t hi, PaddedPODArray<UInt32> & offsets, PaddedPODArray<UInt32> & positions)
    {
        Stopwatch sw;
        const auto & dir = dirs[u];
        const auto & ranks = candidate_ranks[u];
        offsets.clear();
        positions.clear();
        offsets.push_back(0);
        size_t previous_block = std::numeric_limits<size_t>::max();
        for (size_t idx = lo; idx < hi;)
        {
            const size_t block_idx = ranks[idx] / TextIndexBlockedPositionsCodec::BLOCK_DOCS;
            block_local_ranks.clear();
            block_local_ranks.push_back(static_cast<UInt32>(ranks[idx] % TextIndexBlockedPositionsCodec::BLOCK_DOCS));
            ++idx;
            while (idx < hi && ranks[idx] / TextIndexBlockedPositionsCodec::BLOCK_DOCS == block_idx)
            {
                block_local_ranks.push_back(static_cast<UInt32>(ranks[idx] % TextIndexBlockedPositionsCodec::BLOCK_DOCS));
                ++idx;
            }
            if (previous_block == std::numeric_limits<size_t>::max() || block_idx != previous_block + 1)
                positions_stream->seekToMark({dir.block_offsets[block_idx], 0});
            TextIndexBlockedPositionsCodec::decodeBlock(
                *data_buffer, dir, block_idx, block_local_ranks, offsets, positions, blocked_positions_scratch);
            previous_block = block_idx;
            ++blocks_read;
            block_bytes_read += dir.block_offsets[block_idx + 1] - dir.block_offsets[block_idx];
        }
        block_decode_us += sw.elapsedMicroseconds();
    };

    static constexpr size_t CHUNK = 1 << 16;
    PaddedPODArray<UInt32> matching;
    std::vector<PaddedPODArray<UInt32>> chunk_offsets(unique_tokens.size());
    std::vector<PaddedPODArray<UInt32>> chunk_positions(unique_tokens.size());
    UInt64 match_us = 0;

    for (size_t chunk_lo = 0; chunk_lo < candidates.size(); chunk_lo += CHUNK)
    {
        const size_t chunk_hi = std::min(candidates.size(), chunk_lo + CHUNK);
        for (size_t u = 0; u < unique_tokens.size(); ++u)
            decode_chunk(u, chunk_lo, chunk_hi, chunk_offsets[u], chunk_positions[u]);

        Stopwatch match_watch;
        TextIndexPhraseSearch::matchCandidatePositions(
            std::span<const UInt32>(candidates.data() + chunk_lo, chunk_hi - chunk_lo),
            chunk_offsets, chunk_positions, term_to_unique, matching);
        match_us += match_watch.elapsedMicroseconds();
    }
    decode_us += block_decode_us;

    ProfileEvents::increment(ProfileEvents::TextIndexPositionsDecodeMicroseconds, decode_us);
    ProfileEvents::increment(ProfileEvents::TextIndexPhraseMatchMicroseconds, match_us);
    ProfileEvents::increment(ProfileEvents::TextIndexPositionsBlocksRead, blocks_read);
    ProfileEvents::increment(ProfileEvents::TextIndexPositionsBlocksTotal, blocks_total);
    ProfileEvents::increment(ProfileEvents::TextIndexPositionsBytesRead, block_bytes_read);
    return matching;
}

void MergeTreeReaderTextIndex::applyPostingsPhrase(
    IColumn & column,
    const TextSearchQueryPtr & search_query,
    size_t row_offset,
    size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    size_t column_offset = column_data.size();
    column_data.resize_fill(column_offset + num_rows, 0);

    if (!positions_stream || search_query->getPhraseTokens().empty())
        return;

    auto cache_key = search_query->getHash();
    auto doc_ids_it = phrase_search_doc_ids.find(cache_key);

    if (doc_ids_it == phrase_search_doc_ids.end())
    {
        /// Phrase result is a posting list (sorted doc-ids): computed once per (part, query) via the postings cache (Phrase key), shared across the part's readers.
        auto phrase_key = TextIndexPostingsCache::hash(
            granule->getIndexIdForCaches(), cache_key, static_cast<UInt8>(TextIndexPostingsCacheKind::Phrase));

        auto cell = condition_text->postingsCache()->getOrSet(phrase_key, [&]
        {
            /// The header deserialization rejects any codec but Blocked, so the part's positions
            /// are always the blocked candidate-driven layout here.
            chassert(static_cast<TextIndexPositionCodec::Encoding>(granule->getPositionsCodec()) == TextIndexPositionCodec::Encoding::BlockedPfor);
            ProfileEvents::increment(ProfileEvents::TextIndexPhraseSearches);
            return std::make_shared<TextIndexPostingsCacheCell>(
                std::make_shared<PaddedPODArray<UInt32>>(phraseSearchBlocked(*search_query)));
        });

        doc_ids_it = phrase_search_doc_ids.emplace(cache_key, std::get<FlatPostingsPtr>(cell->value)).first;
    }

    const auto & matching_doc_ids = *doc_ids_it->second;
    const size_t window_end = row_offset + num_rows;
    for (const auto * it = std::ranges::lower_bound(matching_doc_ids, row_offset);
         it != matching_doc_ids.end() && *it < window_end;
         ++it)
    {
        size_t relative_row_number = *it - row_offset;
        column_data[column_offset + relative_row_number] = 1;
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
    auto it = granules.find(index.index->index.name);

    if (it != granules.end() && it->second)
    {
        resetCursors();
        postings_blocks.clear();
        setIndexGranule(it->second);
    }
}

MergeTreeReaderPtr createMergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader,
    const MergeTreeIndexWithCondition & index,
    const NamesAndTypesList & columns_to_read,
    MergeTreeIndexGranulePtr index_granule)
{
    return std::make_unique<MergeTreeReaderTextIndex>(main_reader, index, columns_to_read, std::move(index_granule));
}

}
