#include <Columns/ColumnsCommon.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
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
    extern const Event TextIndexUseHint;
    extern const Event TextIndexDiscardHint;
}

namespace DB
{

namespace Setting
{
    extern const SettingsFloat text_index_hint_max_selectivity;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

MergeTreeReaderTextIndex::MergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader_,
    MergeTreeIndexWithCondition index_,
    NamesAndTypesList columns_,
    bool can_skip_mark_)
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
    , can_skip_mark(can_skip_mark_)
    , postings_serialization(typeid_cast<const MergeTreeIndexText &>(*index.index).getPostingListCodec())
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
    auto substreams = index.index->getSubstreams();

    auto make_stream = [&](const auto & substream)
    {
        return makeTextIndexInputStream(
            data_part->getDataPartStoragePtr(),
            index.index->getFileName() + substream.suffix,
            substream.extension,
            MergeTreeIndexReader::patchSettings(settings, substream.type));
    };

    sparse_index_stream = make_stream(substreams[0]);
    dictionary_stream = make_stream(substreams[1]);
    small_postings_stream = make_stream(substreams[2]);

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

    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    if (!condition_text.getAllSearchPatterns().empty())
    {
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
        for (const auto & col : columns_)
        {
            auto search_query = condition_text.getSearchQueryForVirtualColumn(col.name);
            if (!search_query || search_query->patterns.empty())
                continue;

            /// Compile the virtual column's default expression (the original search predicate).
            /// We pass a header with all physical columns so that createExpressionsAnalyzer
            /// can build a non-empty StorageDummy (it requires at least one column).
            NamesAndTypesList need_col{{col.name, col.type}};
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

            fallback_expressions.emplace(col.name, std::move(actions));
        }

        if (!fallback_columns_list.empty())
        {
            fallback_reader = createMergeTreeReader(
                main_reader_->data_part_info_for_read,
                fallback_columns_list,
                main_reader_->storage_snapshot,
                main_reader_->storage_settings,
                main_reader_->all_mark_ranges,
                /*virtual_fields=*/{},
                main_reader_->uncompressed_cache,
                main_reader_->mark_cache,
                /*deserialization_prefixes_cache=*/nullptr,
                main_reader_->settings,
                /*avg_value_size_hints=*/{},
                /*profile_callback=*/{});
        }
    }
}

void MergeTreeReaderTextIndex::updateAllMarkRanges(const MarkRanges & ranges)
{
    IMergeTreeReader::updateAllMarkRanges(ranges);

    if (fallback_reader)
        fallback_reader->updateAllMarkRanges(ranges);

    if (granule && !ranges.empty())
    {
        const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
        size_t row_begin = index_granularity.getMarkStartingRow(ranges.front().begin);
        size_t row_end = index_granularity.getMarkStartingRow(ranges.back().end);

        if (row_begin != row_end)
            cleanupPostingsBlocks(RowsRange(row_begin, row_end - 1));
    }
}

void MergeTreeReaderTextIndex::prefetchBeginOfRange(Priority priority)
{
    sparse_index_stream->seekToStart();
    sparse_index_stream->getDataBuffer()->prefetch(priority);
    is_prefetched = true;
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
    if (!is_prefetched)
        sparse_index_stream->seekToStart();

    dictionary_stream->seekToStart();
    small_postings_stream->seekToStart();

    MergeTreeIndexInputStreams streams;
    streams[MergeTreeIndexSubstream::Type::Regular] = sparse_index_stream.get();
    streams[MergeTreeIndexSubstream::Type::TextIndexDictionary] = dictionary_stream.get();
    streams[MergeTreeIndexSubstream::Type::TextIndexPostings] = small_postings_stream.get();

    granule = index.index->createIndexGranule();
    granule->deserializeBinaryWithMultipleStreams(streams, *deserialization_state);
}

void MergeTreeReaderTextIndex::analyzeTokensCardinality()
{
    is_always_true.resize(columns_to_read.size(), false);
    use_fallback.resize(columns_to_read.size(), false);
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    auto & granule_text = assert_cast<MergeTreeIndexGranuleText &>(*granule);
    const auto & remaining_tokens = granule_text.getRemainingTokens();

    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        const auto & column = columns_to_read[i];
        auto search_query = condition_text.getSearchQueryForVirtualColumn(column.name);

        /// Always return true for empty needles.
        if (search_query->tokens.empty() && search_query->patterns.empty())
        {
            is_always_true[i] = true;
        }
        else if (!search_query->patterns.empty())
        {
            if (!granule_text.canUseLikeDictionaryScan())
            {
                if (!fallback_reader)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "The fallback reader for patterns is not initialized.");

                use_fallback[i] = true;
            }
            else
            {
                const auto & pattern_tokens = granule_text.getPatternTokensForTextQuery(*search_query);
                for (const auto & token : pattern_tokens)
                    useful_tokens.insert(token);
            }
        }
        else if (search_query->direct_read_mode == TextIndexDirectReadMode::Exact)
        {
            useful_tokens.insert(search_query->tokens.begin(), search_query->tokens.end());
        }
        else if (search_query->direct_read_mode == TextIndexDirectReadMode::Hint)
        {
            const auto & settings = condition_text.getContext()->getSettingsRef();
            double selectivity_threshold = settings[Setting::text_index_hint_max_selectivity];
            size_t num_rows_in_part = data_part_info_for_read->getRowCount();
            double cardinality = estimateCardinality(*search_query, remaining_tokens, num_rows_in_part);

            if (cardinality <= static_cast<double>(num_rows_in_part) * selectivity_threshold)
            {
                useful_tokens.insert(search_query->tokens.begin(), search_query->tokens.end());
                ProfileEvents::increment(ProfileEvents::TextIndexUseHint);
            }
            else
            {
                is_always_true[i] = true;
                ProfileEvents::increment(ProfileEvents::TextIndexDiscardHint);
            }
        }
    }
}

void MergeTreeReaderTextIndex::initializePostingStreams()
{
    const auto & granule_text = assert_cast<MergeTreeIndexGranuleText &>(*granule);
    const auto & remaining_tokens = granule_text.getRemainingTokens();
    const auto & pattern_tokens = granule_text.getPatternTokens();

    auto data_part = getDataPart();
    auto substream = index.index->getSubstreams()[2];

    auto make_stream = [&]
    {
        auto stream = makeTextIndexInputStream(
            data_part->getDataPartStoragePtr(),
            index.index->getFileName() + substream.suffix,
            substream.extension,
            MergeTreeIndexReader::patchSettings(settings, substream.type));

        stream->seekToStart();
        return stream;
    };

    for (const auto & [token, token_info] : remaining_tokens)
    {
        if (granule_text.getPostingsForRareToken(token) || !useful_tokens.contains(token))
            continue;

        large_postings_streams.emplace(token, make_stream());
    }

    for (const auto & [token, token_info] : pattern_tokens)
    {
        if (granule_text.getPostingsForRareToken(token) || !useful_tokens.contains(token))
            continue;

        large_postings_streams.emplace(token, make_stream());
    }
}

bool MergeTreeReaderTextIndex::canSkipMark(size_t mark, size_t)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReaderTotalMicroseconds);

    auto rows_range = getRowsRangeForMark(mark);
    if (!rows_range.has_value())
        return true;

    if (!granule)
    {
        readGranule();
        analyzeTokensCardinality();
        initializePostingStreams();
    }

    auto & granule_text = assert_cast<MergeTreeIndexGranuleText &>(*granule);
    granule_text.setCurrentRange(*rows_range);
    bool may_be_true = index.condition->mayBeTrueOnGranule(granule, nullptr);

    if (may_be_true)
        may_be_true_granules.add(static_cast<UInt32>(mark));

    analyzed_granules.add(static_cast<UInt32>(mark));
    return can_skip_mark && !may_be_true;
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

    /// Ensure analyzeTokensCardinality has run (it's called lazily inside canSkipMark)
    /// so use_fallback is known before we decide whether to pre-read the body column.
    if (!granule && max_rows_to_read > 0)
        canSkipMark(from_mark, current_task_last_mark);

    const bool any_use_fallback = !use_fallback.empty()
        && std::any_of(use_fallback.begin(), use_fallback.end(), [](bool b) { return b; });

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

        /// If our reader is not first in the chain, canSkipMark is not called in RangeReader.
        /// TODO: adjust the code in RangeReader to call canSkipMark for all readers.
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
            auto mark_postings = readPostingsIfNeeded(from_mark);

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
                else
                {
                    fillColumn(column_mutable, columns_to_read[i].name, mark_postings, from_row, rows_to_read);
                }
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

double MergeTreeReaderTextIndex::estimateCardinality(const TextSearchQuery & query, const TokenToPostingsInfosMap & remaining_tokens, size_t total_rows) const
{
    chassert(!query.tokens.empty());

    /// Here we assume that tokens are independent and their distribution is uniform.
    /// Below universe E stands for the set of documents in the index granule.
    /// N stands for the size of the index granule in rows.
    /// Sets Ai stand for the posting lists of the searched tokens.
    switch (query.search_mode)
    {
        case TextSearchMode::All:
        {
            /// Estimate the cardinality of the intersection of the sets.
            /// Assume each set Ai has known size |Ai|, and all sets are chosen
            /// independently and uniformly at random from the universe E of size N.
            /// Then, for any particular element, the probability that it appears in set Ai is pi = |Ai|/N.
            /// The probability that a particular element is in all n sets is pn = p1 * p2 * ... * pn.
            /// The the expected cardinality of the intersection is:
            /// N * pn = N * (|A1| * |A2| * ... * |An| / N) = |A1| * |A2| * ... * |An| / N^(n-1).

            /// Compute in log-space to avoid double overflow when many tokens
            /// have large cardinalities: log(N * p1 * p2 * ... * pn) =
            /// sum(log(|Ai|)) - (n-1) * log(N).
            double log_cardinality = 0.0;

            for (const auto & token : query.tokens)
            {
                auto it = remaining_tokens.find(token);
                if (it == remaining_tokens.end())
                    return 0;

                log_cardinality += std::log(static_cast<double>(it->second->cardinality));
            }

            log_cardinality -= static_cast<double>(query.tokens.size() - 1) * std::log(static_cast<double>(total_rows));
            return std::exp(log_cardinality);
        }
        case TextSearchMode::Any:
        {
            /// Estimate the cardinality of the union of the sets.
            /// The same as above the probability that a particular element appears in set Ai is pi = |Ai|/N
            /// The probability that element is not in set Ai is 1 - pi
            /// The probability that element is in none of the n sets is (1 - p1) * (1 - p2) * ... * (1 - pn).
            /// The probability that element is at least in one of the n sets is 1 - (1 - p1) * (1 - p2) * ... * (1 - pn).
            /// Then, the expected cardinality of the union is:
            /// N * (1 - (1 - |A1|/N) * (1 - |A2|/N) * ... * (1 - |An|/N))

            double cardinality = 1.0;

            for (const auto & token : query.tokens)
            {
                auto it = remaining_tokens.find(token);
                /// Same reasoning as above: absent from sparse index ⟹ too common ⟹ treat as all rows.
                double token_cardinality = it == remaining_tokens.end() ? static_cast<double>(total_rows) : it->second->cardinality;
                cardinality *= (1.0 - (token_cardinality / static_cast<double>(total_rows)));
            }

            cardinality = static_cast<double>(total_rows) * (1.0 - cardinality);
            return cardinality;
        }
    }
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

PostingsMap MergeTreeReaderTextIndex::readPostingsIfNeeded(size_t mark)
{
    auto rows_range = getRowsRangeForMark(mark);
    if (!rows_range.has_value())
        return {};

    auto & granule_text = assert_cast<MergeTreeIndexGranuleText &>(*granule);
    const auto & remaining_tokens = granule_text.getRemainingTokens();
    const auto & pattern_tokens = granule_text.getPatternTokens();
    PostingsMap result;

    const auto read_postings_if_needed = [&](const String & token, const TokenPostingsInfoPtr & token_info)
    {
        if (!useful_tokens.contains(token))
            return;

        auto token_postings = readPostingsBlocksForToken(token, *token_info, *rows_range);

        if (token_postings.size() == 1)
        {
            result[token] = std::move(token_postings.front());
        }
        else if (token_postings.size() > 1)
        {
            auto union_posting = std::make_shared<PostingList>();

            for (const auto & posting : token_postings)
                *union_posting |= *posting;

            result[token] = std::move(union_posting);
        }
    };

    /// Read postings for exact token matches
    for (const auto & [token, token_info] : remaining_tokens)
        read_postings_if_needed(token, token_info);

    /// Read postings for pattern-matched tokens
    for (const auto & [token, token_info] : pattern_tokens)
        read_postings_if_needed(token, token_info);

    return result;
}

std::vector<PostingListPtr> MergeTreeReaderTextIndex::readPostingsBlocksForToken(std::string_view token, const TokenPostingsInfo & token_info, const RowsRange & range)
{
    auto & granule_text = assert_cast<MergeTreeIndexGranuleText &>(*granule);
    auto read_postings = granule_text.getPostingsForRareToken(token);

    if (read_postings)
        return {read_postings};

    auto blocks_to_read = token_info.getBlocksToRead(range);
    std::vector<PostingListPtr> token_postings;
    token_postings.reserve(blocks_to_read.size());

    for (const auto & block_idx : blocks_to_read)
    {
        auto [it, inserted] = postings_blocks[token].try_emplace(block_idx);

        if (inserted)
        {
            auto * postings_stream = large_postings_streams.at(token).get();
            it->second = MergeTreeIndexGranuleText::readPostingsBlock(*postings_stream, *deserialization_state, token_info, block_idx, postings_serialization, granule_text.getIndexIdForCaches());
        }

        token_postings.push_back(it->second);
    }

    return token_postings;
}

void MergeTreeReaderTextIndex::cleanupPostingsBlocks(const RowsRange & range)
{
    const auto & granule_text = assert_cast<const MergeTreeIndexGranuleText &>(*granule);
    const auto & remaining_tokens = granule_text.getRemainingTokens();
    const auto & pattern_tokens = granule_text.getPatternTokens();

    const auto cleanup_postings = [&](const String & token, const TokenPostingsInfoPtr & token_info)
    {
        auto it = postings_blocks.find(token);
        if (it == postings_blocks.end())
            return;

        for (size_t i = 0; i < token_info->ranges.size(); ++i)
        {
            if (!token_info->ranges[i].intersects(range))
                it->second.erase(i);
        }
    };

    for (const auto & [token, token_info] : remaining_tokens)
        cleanup_postings(token, token_info);

    for (const auto & [token, token_info] : pattern_tokens)
        cleanup_postings(token, token_info);
}

/// Finds the union of the posting lists for range [granule_offset, granule_offset + num_rows)
void applyPostingsAny(
    IColumn & column,
    PostingsMap & postings_map,
    PaddedPODArray<UInt32> & indices,
    const std::vector<String> & search_tokens,
    size_t column_offset,
    size_t row_offset,
    size_t num_rows)
{
    PostingList union_posting;
    PostingList range_posting;
    range_posting.addRange(row_offset, row_offset + num_rows);

    for (const auto & token : search_tokens)
    {
        auto it = postings_map.find(token);
        if (it == postings_map.end())
            continue;

        union_posting |= (*it->second & range_posting);
    }

    size_t cardinality = union_posting.cardinality();
    if (cardinality == 0)
        return;

    indices.resize(cardinality);
    union_posting.toUint32Array(indices.data());

    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    for (size_t i = 0; i < cardinality; ++i)
    {
        size_t relative_row_number = indices[i] - row_offset;
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
    size_t row_offset,
    size_t num_rows)
{
    if (postings_map.size() > std::numeric_limits<UInt16>::max())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Too many tokens ({}) for All search mode", postings_map.size());

    std::vector<PostingListPtr> token_postings;
    token_postings.reserve(search_tokens.size());

    for (const auto & token : search_tokens)
    {
        auto it = postings_map.find(token);
        if (it == postings_map.end())
            return;

        token_postings.push_back(it->second);
    }

    PostingList intersection_posting;
    intersection_posting.addRange(row_offset, row_offset + num_rows);

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
        size_t relative_row_number = indices[i] - row_offset;
        chassert(relative_row_number < num_rows);
        column_data[column_offset + relative_row_number] = 1;
    }
}

void MergeTreeReaderTextIndex::fillColumn(IColumn & column, const String & column_name, PostingsMap & postings, size_t row_offset, size_t num_rows)
{
    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    const auto & condition_text = assert_cast<const MergeTreeIndexConditionText &>(*index.condition);
    auto search_query = condition_text.getSearchQueryForVirtualColumn(column_name);

    size_t old_size = column_data.size();
    column_data.resize_fill(old_size + num_rows, 0);

    if (!search_query->patterns.empty())
    {
        const auto & granule_text = assert_cast<const MergeTreeIndexGranuleText &>(*granule);
        std::vector<String> matched_tokens;
        for (const auto & token : granule_text.getPatternTokensForTextQuery(*search_query))
            if (postings.contains(token))
                matched_tokens.push_back(String(token));

        if (!matched_tokens.empty())
            applyPostingsAny(column, postings, indices_buffer, matched_tokens, old_size, row_offset, num_rows);

        return;
    }

    if (postings.empty())
        return;

    if (search_query->tokens.empty())
    {
        return;
    }
    else if (search_query->search_mode == TextSearchMode::Any)
    {
        applyPostingsAny(column, postings, indices_buffer, search_query->tokens, old_size, row_offset, num_rows);
    }
    else if (search_query->search_mode == TextSearchMode::All)
    {
        applyPostingsAll(column, postings, indices_buffer, search_query->tokens, old_size, row_offset, num_rows);
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid search mode: {}", search_query->search_mode);
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

    const auto & result_col = slice.getByName(column_name);
    const auto & result_data = assert_cast<const ColumnUInt8 &>(*result_col.column).getData();
    chassert(result_data.size() == num_rows);

    auto & column_data = assert_cast<ColumnUInt8 &>(column).getData();
    const size_t old_size = column_data.size();
    column_data.resize(old_size + num_rows);
    memcpy(&column_data[old_size], result_data.data(), num_rows);
}

MergeTreeReaderPtr createMergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader,
    const MergeTreeIndexWithCondition & index,
    const NamesAndTypesList & columns_to_read,
    bool can_skip_mark)
{
    return std::make_unique<MergeTreeReaderTextIndex>(main_reader, index, columns_to_read, can_skip_mark);
}

}
