#include <Columns/ColumnsCommon.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexAnalyzer.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <Storages/MergeTree/TextIndexUtils.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Core/Settings.h>

namespace ProfileEvents
{
    extern const Event TextIndexReaderTotalMicroseconds;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_text_index_lazy_apply;
    extern const SettingsTextIndexPostingListApplyMode text_index_posting_list_apply_mode;
    extern const SettingsFloat text_index_density_threshold;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
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

    auto data_part = getDataPart();
    auto index_format = index.index->getDeserializedFormat(data_part->checksums, index.index->getFileName());
    chassert(index_format);

    MergeTreeIndexDeserializationState state
    {
        .version = index_format.version,
        .condition = index.condition.get(),
        .part = *data_part,
        .index = *index.index,
    };

    deserialization_state = std::make_unique<MergeTreeIndexDeserializationState>(std::move(state));

    /// Validate lazy mode request once; actual support is determined from the on-disk sparse-index header.
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    const auto & ctx_settings = condition_text.getContext()->getSettingsRef();
    const auto apply_mode = ctx_settings[Setting::text_index_posting_list_apply_mode].value;

    if (apply_mode == TextIndexPostingListApplyMode::LAZY && !ctx_settings[Setting::allow_experimental_text_index_lazy_apply])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Lazy posting list apply mode requires setting allow_experimental_text_index_lazy_apply = 1");

    lazy_mode_requested = (apply_mode == TextIndexPostingListApplyMode::LAZY);
    lazy_density_threshold = ctx_settings[Setting::text_index_density_threshold].value;

    if (!std::isfinite(lazy_density_threshold) || lazy_density_threshold < 0.0f || lazy_density_threshold > 1.0f)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting text_index_density_threshold must be a value in [0.0, 1.0], got {}", lazy_density_threshold);

    if (index_granule_)
        setIndexGranule(std::move(index_granule_));

    initializeFallbackReader(main_reader_);
}

void MergeTreeReaderTextIndex::setIndexGranule(MergeTreeIndexGranulePtr index_granule)
{
    chassert(index_granule);
    granule = std::dynamic_pointer_cast<const MergeTreeIndexGranuleText>(index_granule);
    auto postings_codec = PostingListCodecFactory::createPostingListCodec(granule->getPostingsCodecType());

    /// Lazy mode requires the per-segment block-index section (from `WithCodec` onward) and
    /// pure-token queries — pattern predicates take the eager materialize path.
    auto required_version = static_cast<MergeTreeIndexVersion>(TextIndexHeader::Version::WithCodec);
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);

    use_lazy_mode = lazy_mode_requested
        && postings_codec->getType() != IPostingListCodec::Type::None
        && granule->getSerializationVersion() >= required_version
        && !condition_text.hasSearchPatterns();

    postings_serialization = PostingsSerialization(std::move(postings_codec), granule->getSerializationVersion());
}

void MergeTreeReaderTextIndex::initializeFallbackReader(const IMergeTreeReader * main_reader)
{
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    if (!condition_text.hasSearchPatterns())
        return;

    /// Build a fallback evaluation path for when the dictionary scan is cut short
    /// (too many pattern-matching tokens exceed text_index_like_max_postings_to_read).
    ///
    /// Instead of reading the indexed column by name (which fails for expression-based
    /// indices, e.g. INDEX idx lower(text) where column_names[0] = "lower(text)" is
    /// not a physical column), we compile each virtual column's default expression
    /// (the original search predicate) and determine the required physical columns
    /// from it. The fallback reader is then created for those physical columns only.
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
    for (const auto & column : columns_to_read)
    {
        auto search_query = condition_text.getSearchQueryForVirtualColumn(column.name);
        if (!search_query || search_query->patterns.empty())
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
        fallback_reader->updateAllMarkRanges(ranges);

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
    lazy_cursors.clear();
    prebuilt_cursors.clear();

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
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        const auto & column = columns_to_read[i];
        auto search_query = condition_text.getSearchQueryForVirtualColumn(column.name);
        const auto & query_builder = analyzer.getQueryBuilder(*search_query);

        if (search_query->tokens.empty() && search_query->patterns.empty())
        {
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
            if (search_query->direct_read_mode == TextIndexDirectReadMode::Hint)
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

    auto stream_it = large_postings_streams.find(token);
    if (stream_it != large_postings_streams.end())
        return std::make_shared<PostingListCursor>(*stream_it->second, token_info);

    if (!small_postings_stream)
        small_postings_stream = makeTextIndexStream(index.index->getSubstreams()[2]);

    return std::make_shared<PostingListCursor>(*small_postings_stream, token_info);
}

size_t MergeTreeReaderTextIndex::readRows(
    size_t from_mark,
    size_t current_task_last_mark,
    bool continue_reading,
    size_t max_rows_to_read,
    size_t rows_offset,
    Columns & res_columns)
{
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
        /// Backward jump invalidates the per-token cursor cache: cached cursors are
        /// forward-only (their `linearOr` / `linearAnd` / `advance` walk segments from
        /// `current_segment_idx` onward), so they cannot serve an earlier `row_offset`.
        if (from_mark < current_mark)
        {
            lazy_cursors.clear();
            prebuilt_cursors.clear();
        }

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
    }

    const bool any_use_fallback = !use_fallback.empty() && std::ranges::any_of(use_fallback, [](bool b) { return b; });

    /// If any column needs the fallback evaluation, read the physical columns upfront.
    /// We pass the same mark/continue_reading/offset arguments so the fallback reader stays
    /// in sync with the text-index reader across multiple readRows calls.
    Block fallback_block;
    if (any_use_fallback && fallback_reader && max_rows_to_read > 0)
    {
        Columns fallback_cols(fallback_columns_list.size(), nullptr);
        fallback_reader->readRows(from_mark, current_task_last_mark, continue_reading, max_rows_to_read, rows_offset, fallback_cols);
        size_t col_idx = 0;
        for (const auto & col_name_type : fallback_columns_list)
            fallback_block.insert({fallback_cols[col_idx++], col_name_type.type, col_name_type.name});
    }

    size_t fallback_offset = 0;

    while (read_rows < max_rows_to_read && from_mark < total_marks)
    {
        /// When the number of rows in a part is smaller than `index_granularity`,
        /// `MergeTreeReaderTextIndex` must ensure that the virtual column it reads
        /// contains no more data rows than actually exist in the part
        size_t rows_to_read = std::min(index_granularity.getMarkRows(from_mark), max_rows_to_read - read_rows);

        /// In lazy mode skip per-mark Roaring Bitmap materialization — cursors decode on demand.
        std::vector<PostingList> mark_postings;
        if (!use_lazy_mode)
            mark_postings = buildPostingsForMark(from_mark, RowsRange(from_row, from_row + rows_to_read - 1));

        for (size_t i = 0; i < res_columns.size(); ++i)
        {
            auto & column_mutable = res_columns[i]->assumeMutableRef();

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
            else if (use_lazy_mode)
            {
                fillColumnLazy(column_mutable, columns_to_read[i].name, from_row, rows_to_read);
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

void MergeTreeReaderTextIndex::createEmptyColumns(Columns & columns) const
{
    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (columns[i] == nullptr)
            columns[i] = columns_to_read[i].type->createColumn(*serializations[i]);
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

std::vector<PostingList> MergeTreeReaderTextIndex::buildPostingsForMark(size_t mark, const RowsRange & slice_range)
{
    std::vector<PostingList> result(columns_to_read.size());
    auto mark_range = getRowsRangeForMark(mark);

    if (!mark_range.has_value())
        return result;

    /// Clip to `slice_range`, not the full mark, so postings stay in bounds on partial-mark
    /// reads (`rows_offset > 0` or `max_rows_to_read` stops inside the mark).
    auto effective_range = mark_range->intersectWith(slice_range);
    if (!effective_range.has_value())
        return result;

    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    const auto & analyzer = granule->getAnalyzer();

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        if (is_always_true[i] || use_fallback[i])
            continue;

        auto search_query = condition_text.getSearchQueryForVirtualColumn(columns_to_read[i].name);
        if (search_query->tokens.empty() && search_query->patterns.empty())
            continue;

        result[i] = buildPostingsForQuery(*search_query, analyzer, *effective_range);
    }

    return result;
}

PostingList MergeTreeReaderTextIndex::buildPostingsForQuery(
    const TextSearchQuery & query,
    const TextIndexAnalyzer & analyzer,
    const RowsRange & range)
{
    const auto & query_builder = analyzer.getQueryBuilder(query);
    if (query_builder.is_failed)
        return {};

    std::optional<PostingList> result;
    PostingList range_posting;
    range_posting.addRangeClosed(static_cast<UInt32>(range.begin), static_cast<UInt32>(range.end));

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
            if (query.search_mode == TextSearchMode::All)
                return {};
            else
                continue;
        }

        PostingList large_postings = (*read_blocks.front() & range_posting);
        for (size_t i = 1; i < read_blocks.size(); ++i)
            large_postings |= (*read_blocks[i] & range_posting);

        if (!result)
            result = std::move(large_postings);
        else if (query.search_mode == TextSearchMode::All)
            *result &= large_postings;
        else if (query.search_mode == TextSearchMode::Any)
            *result |= large_postings;

        if (query.search_mode == TextSearchMode::All && result && result->cardinality() == 0)
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

void MergeTreeReaderTextIndex::fillColumnLazy(IColumn & column, const String & column_name, size_t row_offset, size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    size_t old_size = column_data.size();
    column_data.resize_fill(old_size + num_rows, 0);

    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    auto search_query = condition_text.getSearchQueryForVirtualColumn(column_name);
    chassert(search_query->patterns.empty());

    if (search_query->tokens.empty())
        return;

    const auto & analyzer = granule->getAnalyzer();
    const auto & query_builder = analyzer.getQueryBuilder(*search_query);

    if (query_builder.is_failed)
        return;

    std::vector<PostingListCursorPtr> cursors;
    cursors.reserve(query_builder.tokens.size());

    if (query_builder.postings && query_builder.postings->cardinality() > 0)
    {
        auto [it, inserted] = prebuilt_cursors.try_emplace(column_name);

        if (inserted)
        {
            TokenPostingsInfo prebuilt_info;
            prebuilt_info.header = PostingsSerialization::Flags::EmbeddedPostings;
            prebuilt_info.cardinality = static_cast<UInt32>(query_builder.postings->cardinality());
            prebuilt_info.offsets = {0};
            prebuilt_info.ranges = {{query_builder.postings->minimum(), query_builder.postings->maximum()}};
            prebuilt_info.embedded_postings = std::make_shared<PostingList>(*query_builder.postings);

            it->second = std::make_shared<PostingListCursor>(prebuilt_info);
        }

        cursors.push_back(it->second);
    }

    if (query_builder.needReadPostings())
    {
        auto & column_cursors = lazy_cursors[column_name];

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

    if (cursors.empty())
        return;

    if (search_query->search_mode == TextSearchMode::Any)
        lazyUnionPostingLists(column, cursors, old_size, row_offset, num_rows);
    else if (search_query->search_mode == TextSearchMode::All)
        lazyIntersectPostingLists(column, cursors, old_size, row_offset, num_rows, lazy_density_threshold);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid search mode: {}", search_query->search_mode);
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

    const auto & result_col = slice.getByName(column_name);
    const auto & result_data = assert_cast<const ColumnUInt8 &>(*result_col.column).getData();
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
        lazy_cursors.clear();
        prebuilt_cursors.clear();
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
