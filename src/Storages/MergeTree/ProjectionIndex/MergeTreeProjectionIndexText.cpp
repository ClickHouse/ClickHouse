#include <Storages/MergeTree/ProjectionIndex/MergeTreeProjectionIndexText.h>

#include <Columns/ColumnAggregateFunction.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListState.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexText.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Storages/ProjectionsDescription.h>

namespace ProfileEvents
{
    extern const Event TextIndexReadDictionaryBlocks;
    extern const Event TextIndexReadGranulesMicroseconds;
    extern const Event TextIndexReadPostings;
    extern const Event TextIndexTokensCacheHits;
    extern const Event TextIndexTokensCacheMisses;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

MergeTreeProjectionIndexGranuleText::MergeTreeProjectionIndexGranuleText(const String & projection_name_)
    : projection_name(projection_name_)
{
}

MergeTreeProjectionIndexGranuleText::~MergeTreeProjectionIndexGranuleText() = default;

void MergeTreeProjectionIndexGranuleText::serializeBinary(WriteBuffer &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Projection text index granule does not support legacy serialization");
}

void MergeTreeProjectionIndexGranuleText::deserializeBinary(ReadBuffer &, MergeTreeIndexVersion)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Projection text index granule does not support legacy deserialization");
}

PostingListPtr MergeTreeProjectionIndexGranuleText::getPostingsForRareToken(std::string_view token) const
{
    auto it = rare_tokens_postings.find(token);
    if (it != rare_tokens_postings.end())
        return it->second;
    return nullptr;
}

std::vector<String> MergeTreeProjectionIndexGranuleText::fillTokensFromCache(MergeTreeIndexDeserializationState & state)
{
    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);
    const auto & all_search_tokens = condition_text.getAllSearchTokens();
    auto tokens_cache = condition_text.tokensCache();

    std::vector<TextIndexTokensCache::Key> keys;
    keys.reserve(all_search_tokens.size());
    for (const auto & token : all_search_tokens)
        keys.emplace_back(TextIndexTokensCache::hash(index_id_for_caches, token));

    auto cached_infos = tokens_cache->getMany(keys);
    std::vector<String> tokens_to_read;
    for (size_t i = 0; i < all_search_tokens.size(); ++i)
    {
        auto proj_info = cached_infos[i] ? std::dynamic_pointer_cast<ProjectionTokenInfo>(cached_infos[i]) : nullptr;
        if (proj_info)
        {
            remaining_tokens.emplace(all_search_tokens[i], std::move(proj_info));
            ProfileEvents::increment(ProfileEvents::TextIndexTokensCacheHits);
        }
        else
        {
            tokens_to_read.emplace_back(all_search_tokens[i]);
            ProfileEvents::increment(ProfileEvents::TextIndexTokensCacheMisses);
        }
    }
    return tokens_to_read;
}

void MergeTreeProjectionIndexGranuleText::deserializeBinaryWithMultipleStreams(
    MergeTreeIndexInputStreams & /* streams */, MergeTreeIndexDeserializationState & state)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReadGranulesMicroseconds);

    for (const auto & [name, proj_part] : state.part.getProjectionParts())
    {
        if (name == projection_name)
        {
            projection_part = proj_part;
            break;
        }
    }

    if (!projection_part)
        return;

    if (index_id_for_caches.empty())
    {
        const auto & part_storage = state.part.getDataPartStorage();
        index_id_for_caches = fmt::format("proj:{}:{}:{}", part_storage.getDiskName(), part_storage.getFullPath(), projection_name);
    }

    auto tokens_to_read = fillTokensFromCache(state);
    if (tokens_to_read.empty())
        return;

    DictionaryBlockBase sparse_index(projection_part->getIndex()->at(0));
    if (sparse_index.empty())
        return;

    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);
    auto global_search_mode = condition_text.getGlobalSearchMode();
    auto tokens_cache = condition_text.tokensCache();

    /// Map each token to a mark (granule) that should contain it, using the sparse index.
    /// Group tokens by mark for batch reading.
    std::sort(tokens_to_read.begin(), tokens_to_read.end());
    std::vector<std::pair<size_t, std::vector<std::string_view>>> marks_to_read;

    for (const auto & token : tokens_to_read)
    {
        size_t pos = sparse_index.upperBound(token);

        if (pos == 0)
        {
            if (global_search_mode == TextSearchMode::All)
                return;
            continue;
        }

        size_t mark = pos - 1;

        if (pos == sparse_index.size())
        {
            /// `pos == size` means the token is >= the last sparse key.
            /// `mark = pos - 1` already points to the correct (last) granule.
            /// Adjust for the final mark if present (it has zero rows).
            if (projection_part->index_granularity->hasFinalMark())
                --mark;
        }

        if (marks_to_read.empty() || marks_to_read.back().first != mark)
            marks_to_read.emplace_back(mark, std::vector<std::string_view>());

        marks_to_read.back().second.emplace_back(token);
    }

    if (marks_to_read.empty())
        return;

    StorageMetadataPtr metadata_ptr = projection_part->storage.getInMemoryMetadataPtr(Context::getGlobalContextInstance(), false);
    StorageSnapshotPtr storage_snapshot_ptr = std::make_shared<StorageSnapshot>(projection_part->storage, metadata_ptr);
    auto alter_conversions = std::make_shared<AlterConversions>();
    auto part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(projection_part, alter_conversions);
    auto cols = projection_part->getColumns();
    auto reader_settings = MergeTreeReaderSettings::createFromSettings();
    reader_settings.save_marks_in_cache = true;

    MergeTreeReaderPtr reader = createMergeTreeReader(
        part_info,
        cols,
        storage_snapshot_ptr,
        projection_part->storage.getSettings(),
        MarkRanges{MarkRange(marks_to_read.front().first, marks_to_read.back().first + 1)},
        /*virtual_fields=*/{},
        /*uncompressed_cache=*/{},
        projection_part->storage.getContext()->getMarkCache().get(),
        nullptr,
        reader_settings,
        ValueSizeMap{},
        ReadBufferFromFileBase::ProfileCallback{});

    /// Set up posting filter callback: invoked by the reader after reading the term column,
    /// before reading the posting column. Binary-searches for needed tokens and returns
    /// matched row indices so the posting deserializer skips non-matched rows (only calls
    /// PostingListStream::skip, no Arena allocation for posting data).
    std::vector<std::string_view> needed_tokens_for_mark;
    TextSearchMode search_mode_for_mark = global_search_mode;
    bool search_failed = false;

    reader->posting_filter_callback = [&](const IColumn & term_column) -> std::vector<size_t>
    {
        const auto & tokens_column = assert_cast<const ColumnString &>(term_column);
        const size_t num_tokens = tokens_column.size();
        std::vector<size_t> matched;

        size_t search_start = 0;
        for (const auto & token : needed_tokens_for_mark)
        {
            size_t lo = search_start;
            size_t hi = num_tokens;
            size_t found_idx = num_tokens;
            while (lo < hi)
            {
                size_t mid = lo + (hi - lo) / 2;
                auto cmp = tokens_column.getDataAt(mid);
                if (cmp < token)
                    lo = mid + 1;
                else if (token < cmp)
                    hi = mid;
                else
                {
                    found_idx = mid;
                    break;
                }
            }

            if (found_idx == num_tokens)
            {
                if (search_mode_for_mark == TextSearchMode::All)
                {
                    search_failed = true;
                    return {};
                }
                continue;
            }

            search_start = found_idx;
            matched.push_back(found_idx);
        }
        return matched;
    };

    std::optional<size_t> prev_mark;

    /// `.pidx` stream — lazily acquired below once we know the real `has_block_index`
    /// from the aggregate function params. The granule's default `has_block_index = true`
    /// may not match the actual index format: when the index was built with
    /// `has_block_index = false`, the `.pidx` file exists but is empty, and any read
    /// on it would throw `ATTEMPT_TO_READ_AFTER_EOF`.
    LargePostingListReaderStreamPtr pidx_stream;

    for (const auto & [mark, needed_tokens] : marks_to_read)
    {
        ProfileEvents::increment(ProfileEvents::TextIndexReadDictionaryBlocks);

        const size_t rows_to_read = projection_part->index_granularity->getMarkRows(mark);

        /// Set up per-mark state for the callback
        needed_tokens_for_mark = needed_tokens;
        search_mode_for_mark = global_search_mode;
        search_failed = false;

        /// Single readRows: reads term column → callback fires → reads posting column with filter.
        /// The reader keeps `matched_row_indices_for_posting` from the previous mark; clear it
        /// so this iteration sees a fresh empty optional. The callback either sets it or
        /// leaves it nullopt (meaning: read all rows for this mark).
        reader->matched_row_indices_for_posting.reset();
        Columns result;
        result.resize(cols.size());
        size_t rows_read = reader->readRows(
            mark,
            sparse_index.size(),
            prev_mark && *prev_mark == mark - 1,
            rows_to_read,
            /*rows_offset=*/0,
            result);

        chassert(rows_read > 0);
        prev_mark = mark;

        if (search_failed)
        {
            remaining_tokens.clear();
            return;
        }

        const ColumnAggregateFunction & posting_column = assert_cast<const ColumnAggregateFunction &>(*result[1]);
        const auto & data = posting_column.getData();
        const auto & tokens_column = assert_cast<const ColumnString &>(*result[0]);

        if (data.empty())
            continue;

        /// Extract has_block_index and phrase_enabled from the aggregate function params.
        const auto * agg_func = dynamic_cast<const AggregateFunctionPostingList *>(posting_column.getAggregateFunction().get());
        bool phrase_enabled = false;
        if (agg_func)
        {
            has_block_index = agg_func->params.has_block_index;
            phrase_enabled = agg_func->params.enable_phrase_query_support;
        }

        if (has_block_index && !pidx_stream)
            pidx_stream = reader->getProjectionIndexPostingIndexStreamPtr();

        if (has_block_index && !pst_stream)
            pst_stream = reader->getProjectionIndexPostingStreamPtr();

        /// Build TokenPostingsInfo from a matched (token, posting) pair.
        auto process_match = [&](std::string_view token, const PostingListData * posting_list_data)
        {
            chassert(posting_list_data->isStream());

            auto info = ProjectionTokenInfo::buildFromPostingStream(
                posting_list_data->stream, pidx_stream.get(), phrase_enabled);

            if (info)
            {
                String token_str(token);
                auto token_hash = TextIndexTokensCache::hash(index_id_for_caches, token_str);
                tokens_cache->set(token_hash, info);
                remaining_tokens.emplace(std::move(token_str), std::move(info));
            }
        };

        /// Term column has N rows (unfiltered). Posting column has M rows when filter
        /// callback is active (`optional` engaged, including the zero-match case), or N
        /// rows when the filter is disabled (`std::nullopt` — e.g. merge path, or all
        /// rows matched and we chose to skip the per-row dispatch).
        const auto & matched_opt = reader->matched_row_indices_for_posting;
        if (matched_opt.has_value())
        {
            /// Filtered path: posting column has M rows, term column has N rows.
            const auto & matched = *matched_opt;
            chassert(matched.size() == data.size());
            for (size_t i = 0; i < data.size(); ++i)
                process_match(tokens_column.getDataAt(matched[i]), reinterpret_cast<const PostingListData *>(data[i]));
        }
        else
        {
            /// No filter callback (e.g. merge path) — iterate all rows.
            size_t search_start = 0;
            for (const auto & token : needed_tokens)
            {
                size_t lo = search_start;
                size_t hi = data.size();
                size_t found_idx = data.size();
                while (lo < hi)
                {
                    size_t mid = lo + (hi - lo) / 2;
                    auto cmp = tokens_column.getDataAt(mid);
                    if (cmp < token)
                        lo = mid + 1;
                    else if (token < cmp)
                        hi = mid;
                    else
                    {
                        found_idx = mid;
                        break;
                    }
                }

                if (found_idx == data.size())
                {
                    if (global_search_mode == TextSearchMode::All)
                    {
                        remaining_tokens.clear();
                        return;
                    }
                    continue;
                }

                search_start = found_idx;
                process_match(token, reinterpret_cast<const PostingListData *>(data[found_idx]));
            }
        }
    }

    if (remaining_tokens.empty())
        return;

    const String & data_path = state.part.getDataPartStorage().getFullPath();

    for (const auto & [token, token_info] : remaining_tokens)
    {
        if (!has_block_index && !token_info->large_block_metas.empty())
        {
            /// Without block index, fall back to materializing low-cardinality
            /// tokens into Roaring bitmaps for mark filtering.
            /// High-cardinality tokens are materialized on-demand in buildCursorMap.
            static constexpr UInt32 MATERIALIZATION_SKIP_THRESHOLD = 8192;
            if (token_info->cardinality > MATERIALIZATION_SKIP_THRESHOLD)
                continue;

            if (!large_posting_stream)
            {
                large_posting_stream = reader->getProjectionIndexPostingStreamPtr();
                chassert(large_posting_stream);
            }

            const auto load_postings = [&]
            {
                ProfileEvents::increment(ProfileEvents::TextIndexReadPostings);
                auto bitmap = std::make_shared<PostingList>();
                for (size_t b = 0; b < token_info->large_block_metas.size(); ++b)
                {
                    auto block_bitmap = materializeFromTokenInfo(*large_posting_stream, *token_info, b);
                    if (block_bitmap)
                        *bitmap |= *block_bitmap;
                }
                return std::make_shared<TextIndexPostingsCacheCell>(std::move(bitmap));
            };

            auto hash = TextIndexPostingsCache::hash(data_path, projection_part->name, token_info->large_block_metas[0].offset);
            auto cell = condition_text.postingsCache()->getOrSet(hash, load_postings);
            rare_tokens_postings.emplace(token, std::get<PostingListPtr>(cell->value));
        }
    }
}
// ─── Mark filtering ───────────────────────────────────────────────────

bool MergeTreeProjectionIndexGranuleText::hasAnyQueryTokens(const TextSearchQuery & query) const
{
    if (!projection_part)
        return true;

    if (query.tokens.empty())
        return false;

    if (!current_range.has_value())
    {
        for (const auto & token : query.tokens)
            if (remaining_tokens.contains(token))
                return true;
        return false;
    }

    UInt32 range_begin = static_cast<UInt32>(current_range->begin);
    UInt32 range_end = static_cast<UInt32>(current_range->end);

    for (const auto & token : query.tokens)
    {
        auto it = remaining_tokens.find(token);
        if (it != remaining_tokens.end()
            && it->second->hasDocInRange(*current_range, range_begin, range_end, decoded_block_cache, pst_stream.get()))
            return true;
    }
    return false;
}

bool MergeTreeProjectionIndexGranuleText::hasAnyQueryPatterns(const TextSearchQuery & query) const
{
    if (!projection_part)
        return true;

    if (query.patterns.empty())
        return false;

    /// Patterns carry extracted tokens (e.g. ngrams from LIKE '%5555%').
    /// No tokens means the tokenizer could not extract anything — conservatively pass.
    if (query.tokens.empty())
        return true;

    if (!current_range.has_value())
    {
        for (const auto & token : query.tokens)
            if (remaining_tokens.contains(token))
                return true;
        return false;
    }

    UInt32 range_begin = static_cast<UInt32>(current_range->begin);
    UInt32 range_end = static_cast<UInt32>(current_range->end);

    for (const auto & token : query.tokens)
    {
        auto it = remaining_tokens.find(token);
        if (it != remaining_tokens.end()
            && it->second->hasDocInRange(*current_range, range_begin, range_end, decoded_block_cache, pst_stream.get()))
            return true;
    }
    return false;
}

bool MergeTreeProjectionIndexGranuleText::hasAllQueryTokens(const TextSearchQuery & query) const
{
    if (query.tokens.empty())
        return false;
    return hasAllQueryTokensOrEmpty(query);
}

bool MergeTreeProjectionIndexGranuleText::hasAllQueryTokensOrEmpty(const TextSearchQuery & query) const
{
    if (!projection_part)
        return true;

    if (!current_range.has_value())
    {
        for (const auto & token : query.tokens)
            if (!remaining_tokens.contains(token))
                return false;
        return true;
    }

    UInt32 range_begin = static_cast<UInt32>(current_range->begin);
    UInt32 range_end = static_cast<UInt32>(current_range->end);

    for (const auto & token : query.tokens)
    {
        auto it = remaining_tokens.find(token);
        if (it == remaining_tokens.end()
            || !it->second->hasDocInRange(*current_range, range_begin, range_end, decoded_block_cache, pst_stream.get()))
            return false;
    }
    return true;
}

PostingListPtr MergeTreeProjectionIndexGranuleText::materializeFromTokenInfo(
    LargePostingListReaderStream & stream, const ProjectionTokenInfo & token_info, size_t block_idx)
{
    const auto & meta = token_info.large_block_metas[block_idx];

    /// For block_idx==0: the cursor emits first_doc_id (from ranges[0].begin) as the
    /// first element, then decodes block_doc_count packed docs from .pst.
    /// The delta-1 base is first_doc_id itself (the packed deltas are relative to it).
    ///
    /// For block_idx > 0: include_first_doc=false, cursor calls loadNextBlock immediately.
    /// Delta-1 base must be the last doc_id of the previous large block, because the
    /// packed deltas were encoded relative to it.
    UInt32 delta_base = 0;
    if (block_idx == 0)
        delta_base = static_cast<UInt32>(token_info.ranges[0].begin);
    else
        delta_base = static_cast<UInt32>(token_info.ranges[block_idx - 1].end);

    return ReaderStreamEntry::materializeLargeBlockIntoBitmap(
        stream, delta_base, meta.block_doc_count, meta.offset, block_idx == 0 /* include_first_doc */,
        token_info.first_doc_freq > 0 /* has_freq */);
}

namespace
{

const IndexDescription & getIndexDescriptionOrThrow(const ProjectionDescription & projection)
{
    if (!projection.index)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Projection index is not initialized");

    const auto * index_desc = projection.index->getIndexDescription();
    if (!index_desc)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Projection index {} should have index description initialized. It is a bug",
            projection.index->getName());
    }

    return *index_desc;
}

}

MergeTreeProjectionIndexText::MergeTreeProjectionIndexText(
    const ProjectionDescription & projection, std::shared_ptr<const MergeTreeIndexText> text_index_)
    : IMergeTreeIndex(getIndexDescriptionOrThrow(projection))
    , text_index(std::move(text_index_))
{
}

MergeTreeProjectionIndexText::MergeTreeProjectionIndexText(
    const IndexDescription & index_description, std::shared_ptr<const MergeTreeIndexText> text_index_)
    : IMergeTreeIndex(index_description)
    , text_index(std::move(text_index_))
{
}

MergeTreeIndexSubstreams MergeTreeProjectionIndexText::getSubstreams() const
{
    return text_index->getSubstreams();
}

MergeTreeIndexFormat MergeTreeProjectionIndexText::getDeserializedFormat(
    const MergeTreeDataPartChecksums & checksums, const std::string & /* path_prefix */) const
{
    if (checksums.files.contains(index.name + ".proj"))
        return {1, getSubstreams()};

    return {0 /*unknown*/, {}};
}

MergeTreeIndexGranulePtr MergeTreeProjectionIndexText::createIndexGranule() const
{
    return std::make_shared<MergeTreeProjectionIndexGranuleText>(index.name);
}

MergeTreeIndexAggregatorPtr MergeTreeProjectionIndexText::createIndexAggregator() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeProjectionIndexText cannot create aggregator");
}

MergeTreeIndexConditionPtr MergeTreeProjectionIndexText::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionText>(
        predicate,
        context,
        index.sample_block,
        text_index->tokenizer.get(),
        text_index->preprocessor,
        text_index->params.enable_phrase_query_support);
}

}
