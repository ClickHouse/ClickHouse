#include <Storages/MergeTree/ProjectionIndex/MergeTreeIndexProjection.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/ProjectionIndex/PostingListData.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexText.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Storages/ProjectionsDescription.h>

namespace ProfileEvents
{
    extern const Event TextIndexReadDictionaryBlocks;
    extern const Event TextIndexReadGranulesMicroseconds;
    extern const Event TextIndexReadPostings;
    extern const Event TextIndexUsedEmbeddedPostings;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeIndexGranuleProjection::MergeTreeIndexGranuleProjection(const String & projection_name_)
    : MergeTreeIndexGranuleText({})
    , projection_name(projection_name_)
{
}

MergeTreeIndexGranuleProjection::~MergeTreeIndexGranuleProjection() = default;

void MergeTreeIndexGranuleProjection::deserializeBinaryWithMultipleStreams(
    MergeTreeIndexInputStreams & /* streams */, MergeTreeIndexDeserializationState & state)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::TextIndexReadGranulesMicroseconds);

    MergeTreeDataPartPtr part;
    for (const auto & [name, projection_part] : state.part.getProjectionParts())
    {
        if (name == projection_name)
        {
            part = projection_part;
            break;
        }
    }

    if (!part)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Projection part '{}' not found while deserializing projection index in part '{}'",
            projection_name,
            state.part.name);
    }

    DictionaryBlockBase sparse_index(part->getIndex()->at(0));
    if (sparse_index.empty())
        return;

    const auto & condition_text = typeid_cast<const MergeTreeIndexConditionText &>(*state.condition);
    auto global_search_mode = condition_text.getGlobalSearchMode();
    const auto & all_search_tokens = condition_text.getAllSearchTokens();
    std::map<size_t, std::vector<std::string_view>> mark_to_tokens;

    for (const auto & token : all_search_tokens)
    {
        /// upperBound returns the first position with value > token
        size_t pos = sparse_index.upperBound(token);

        /// All sparse index values are greater than token: no mark satisfies (mark_value <= token)
        if (pos == 0)
        {
            if (global_search_mode == TextSearchMode::All)
                return;
            continue;
        }

        /// Convert upper_bound result to the last index with value <= token
        /// Normally this is pos - 1
        size_t mark = pos - 1;

        /// If upperBound points past the end, the token is greater than or equal to
        /// the last indexed token.
        ///
        /// Two cases:
        /// 1) The token is strictly greater than all indexed tokens:
        ///    - No granule can possibly contain it.
        ///    - In ALL mode, we can early-exit; otherwise skip this token.
        /// 2) The token is equal to the last indexed token:
        ///    - upperBound() returns size(), but the token still belongs to
        ///      the last granule.
        ///    - If the part has a final mark, we must seek to (last_mark - 1)
        ///      to read the last granule.
        if (pos == sparse_index.size())
        {
            if (sparse_index.tokens->getDataAt(mark) == token)
            {
                /// Special handling for the last mark:
                /// upperBound() lands past the end, but the matching granule
                /// is the one before the final mark.
                if (part->index_granularity->hasFinalMark())
                    --mark;
            }
            else
            {
                if (global_search_mode == TextSearchMode::All)
                    return;
                continue;
            }
        }

        mark_to_tokens[mark].emplace_back(token);
    }

    if (mark_to_tokens.empty())
        return;

    StorageMetadataPtr metadata_ptr = part->storage.getInMemoryMetadataPtr();
    StorageSnapshotPtr storage_snapshot_ptr = std::make_shared<StorageSnapshot>(part->storage, metadata_ptr);
    auto alter_conversions = std::make_shared<AlterConversions>();
    auto part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, alter_conversions);
    auto cols = part->getColumns();
    MergeTreeReaderPtr reader = createMergeTreeReader(
        part_info,
        cols,
        storage_snapshot_ptr,
        part->storage.getSettings(),
        MarkRanges{MarkRange(mark_to_tokens.begin()->first, mark_to_tokens.rbegin()->first + 1)},
        /*virtual_fields=*/{},
        /*uncompressed_cache=*/{},
        part->storage.getContext()->getMarkCache().get(),
        nullptr,
        MergeTreeReaderSettings::createFromSettings(),
        ValueSizeMap{},
        ReadBufferFromFileBase::ProfileCallback{});

    std::optional<size_t> prev_mark;
    const auto get_dictionary_block = [&](size_t mark)
    {
        const auto load_dictionary_block = [&] -> TextIndexDictionaryBlockCacheEntryPtr
        {
            ProfileEvents::increment(ProfileEvents::TextIndexReadDictionaryBlocks);
            const size_t rows_to_read = part->index_granularity->getMarkRows(mark);
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

            assert_cast<const ColumnString &>(*result[0]);
            const ColumnAggregateFunction & posting_column = assert_cast<const ColumnAggregateFunction &>(*result[1]);
            const auto & data = posting_column.getData();
            const size_t rows = data.size();
            chassert(rows_read == rows);
            std::vector<TokenPostingsInfo> token_infos;
            token_infos.reserve(rows);

            for (size_t i = 0; i < rows; ++i)
            {
                const auto * posting_list_data = reinterpret_cast<const PostingListData *>(data[i]);
                chassert(posting_list_data->isStream());
                const auto & stream = posting_list_data->stream;

                TokenPostingsInfo info;
                info.cardinality = stream.doc_count;
                if (stream.doc_count == 0)
                {
                    /// Ignore empty token
                }
                else if (stream.doc_count <= MAX_SIZE_OF_EMBEDDED_POSTINGS)
                {
                    info.embedded_postings = std::make_shared<PostingList>();
                    info.embedded_postings->addMany(info.cardinality, stream.embedded_postings);
                    info.ranges.emplace_back(stream.embedded_postings[0], stream.embedded_postings[info.cardinality - 1]);
                }
                else
                {
                    chassert(stream.lazy_posting_stream);
                    chassert(stream.lazy_posting_stream->merged_embedded_postings.empty());
                    chassert(stream.lazy_posting_stream->streams.size() == 1);

                    const auto & entry = stream.lazy_posting_stream->streams.entries.front();
                    size_t num_large_blocks = entry.large_posting_blocks.size();
                    chassert(num_large_blocks > 0);

                    info.offsets.reserve(num_large_blocks);
                    info.ranges.reserve(num_large_blocks);
                    UInt32 range_begin = entry.first_doc_id;
                    for (size_t b = 0; b < num_large_blocks; ++b)
                    {
                        info.offsets.push_back(entry.large_posting_blocks[b]);
                        info.ranges.emplace_back(range_begin, entry.large_posting_blocks[b].last_doc_id);
                        range_begin = entry.large_posting_blocks[b].last_doc_id + 1;
                    }
                }

                token_infos.emplace_back(std::move(info));
            }

            return std::make_shared<TextIndexDictionaryBlockCacheEntry>(DictionaryBlock(std::move(result[0]), std::move(token_infos)));
        };

        auto hash = TextIndexDictionaryBlockCache::hash(state.part.getDataPartStorage().getFullPath(), part->name, mark);
        return condition_text.dictionaryBlockCache()->getOrSet(hash, load_dictionary_block);
    };

    for (const auto & [mark, tokens] : mark_to_tokens)
    {
        const auto dictionary_block = get_dictionary_block(mark);

        for (const auto & token : tokens)
        {
            auto * token_info = dictionary_block->getTokenInfo(token);
            if (token_info && token_info->cardinality > 0)
            {
                remaining_tokens.emplace(token, *token_info);
            }
            else if (global_search_mode == TextSearchMode::All)
            {
                remaining_tokens.clear();
                return;
            }
        }
    }

    if (remaining_tokens.empty())
        return;

    large_posting_stream = reader->getProjectionIndexPostingStreamPtr();
    chassert(large_posting_stream);

    const String & data_path = state.part.getDataPartStorage().getFullPath();
    for (const auto & [token, token_info] : remaining_tokens)
    {
        if (token_info.embedded_postings)
        {
            ProfileEvents::increment(ProfileEvents::TextIndexUsedEmbeddedPostings);
            rare_tokens_postings.emplace(token, token_info.embedded_postings);
        }
        else if (token_info.offsets.size() == 1)
        {
            const auto load_postings = [&]() -> PostingListPtr
            {
                ProfileEvents::increment(ProfileEvents::TextIndexReadPostings);
                return materializeFromTokenInfo(*large_posting_stream, token_info, 0);
            };

            auto hash = TextIndexPostingsCache::hash(data_path, part->name, token_info.offsets[0].offset);
            auto p = condition_text.postingsCache()->getOrSet(hash, load_postings);
            rare_tokens_postings.emplace(token, std::move(p));
        }
    }
}

PostingListPtr MergeTreeIndexGranuleProjection::materializeFromTokenInfo(
    LargePostingListReaderStream & stream, const TokenPostingsInfo & token_info, size_t block_idx)
{
    /// For delta-decoding:
    /// - First block: 'begin' is the first doc_id (include it).
    /// - Other blocks: 'begin - 1' is the baseline to reconstruct 'begin' (exclude it).
    UInt32 last_doc_id = token_info.ranges[block_idx].begin;
    if (block_idx > 0)
        --last_doc_id;

    return ReaderStreamEntry::materializeLargeBlockIntoBitmap(
        stream,
        last_doc_id,
        token_info.offsets[block_idx].block_doc_count,
        token_info.offsets[block_idx].offset,
        block_idx == 0 /* include_first_doc */);
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

MergeTreeIndexProjection::MergeTreeIndexProjection(
    const ProjectionDescription & projection, std::shared_ptr<const MergeTreeIndexText> text_index_)
    : IMergeTreeIndex(getIndexDescriptionOrThrow(projection))
    , text_index(std::move(text_index_))
{
}

MergeTreeIndexSubstreams MergeTreeIndexProjection::getSubstreams() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeIndexProjection cannot get substreams");
}

MergeTreeIndexFormat
MergeTreeIndexProjection::getDeserializedFormat(const MergeTreeDataPartChecksums & checksums, const std::string & /* path_prefix */) const
{
    /// Projection index intentionally does not return any index streams here. It relies on the MergeTree part reader
    /// for deserialization instead.
    if (checksums.files.contains(index.name + ".proj"))
        return {1, {{}}};

    return {0 /*unknown*/, {}};
}

MergeTreeIndexGranulePtr MergeTreeIndexProjection::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleProjection>(index.name);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexProjection::createIndexAggregator() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeIndexProjection cannot create aggregator");
}

MergeTreeIndexConditionPtr MergeTreeIndexProjection::createIndexCondition(const ActionsDAG::Node * predicate, ContextPtr context) const
{
    return std::make_shared<MergeTreeIndexConditionText>(
        predicate, context, index.sample_block, text_index->token_extractor.get(), text_index->preprocessor);
}

}
