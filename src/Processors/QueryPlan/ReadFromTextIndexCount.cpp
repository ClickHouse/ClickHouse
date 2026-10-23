#include <Processors/QueryPlan/ReadFromTextIndexCount.h>

#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/ProcessList.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexAnalyzer.h>
#include <Storages/MergeTree/TextIndexUtils.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace
{

/// Reads a token's posting blocks from the postings stream, unioning the blocks that overlap a row range.
template <typename CheckCancelledCallback>
class PostingBlockReader
{
public:
    PostingBlockReader(
        MergeTreeReaderStream & postings_stream_,
        MergeTreeIndexDeserializationState & state_,
        PostingsSerialization & serialization_,
        const String & index_id_,
        const CheckCancelledCallback & check_cancelled_)
        : postings_stream(postings_stream_)
        , state(state_)
        , serialization(serialization_)
        , index_id(index_id_)
        , check_cancelled(check_cancelled_)
    {
    }

    PostingList read(const TokenPostingsInfo & token_info, const RowsRange & range, const PostingList * candidates) const
    {
        if (!token_info.embedded_postings.empty())
        {
            const auto & embedded = token_info.embedded_postings;
            return PostingList(embedded.size(), embedded.data());
        }

        PostingList postings;
        for (size_t block_idx : token_info.getBlocksToRead(range))
        {
            check_cancelled();

            if (candidates)
            {
                const auto & block_range = token_info.ranges[block_idx];
                UInt64 up_to_end = candidates->rank(static_cast<UInt32>(block_range.end));
                UInt64 before_begin = block_range.begin == 0 ? 0 : candidates->rank(static_cast<UInt32>(block_range.begin - 1));
                if (up_to_end == before_begin)
                    continue;
            }

            auto block = MergeTreeIndexGranuleText::readPostingsBlock(
                postings_stream, state, token_info, block_idx, serialization, index_id);

            if (block)
                postings |= *block;
        }
        return postings;
    }

private:
    MergeTreeReaderStream & postings_stream;
    MergeTreeIndexDeserializationState & state;
    PostingsSerialization & serialization;
    const String & index_id;
    const CheckCancelledCallback & check_cancelled;
};

/// Counts matching rows in one part from the text-index posting metadata, without reading rows.
/// `check_cancelled` is polled between posting blocks and tokens so a large part stays interruptible.
template <typename CheckCancelledCallback>
UInt64 computeCountForPart(
    const RangesInDataPart & part_with_ranges,
    const ReadFromTextIndexCount::ResolvedQuery & resolved,
    const MergeTreeReaderSettings & reader_settings,
    const CheckCancelledCallback & check_cancelled)
{
    const auto & data_part = part_with_ranges.data_part;
    const auto & index = resolved.index;

    auto index_format = index.index->getDeserializedFormat(*data_part, index.index->getFileName());
    if (!index_format)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Text index {} is not materialized in part {}. It must have been checked at plan time",
            index.index->index.name, data_part->name);

    /// A single-token count is answered directly from the dictionary cardinality, so posting list is never read.
    const bool single_token = resolved.query->getTokens().size() == 1;

    LoadedMergeTreeDataPartInfoForReader part_info(data_part, std::make_shared<AlterConversions>());

    MergeTreeIndexDeserializationState state
    {
        .version = index_format.version,
        .condition = resolved.condition.get(),
        .part_info = part_info,
        .index = *index.index,
        .readable_ranges = nullptr,
        .skip_postings_deserialization = single_token,
    };

    const auto substreams = index.index->getSubstreams();
    auto make_stream = [&](const MergeTreeIndexSubstream & substream)
    {
        return makeTextIndexInputStream(
            data_part->getDataPartStoragePtr(),
            index.index->getFileName() + substream.suffix,
            substream.extension,
            MergeTreeIndexReader::patchSettings(reader_settings, substream.type));
    };

    auto sparse_index_stream = make_stream(substreams[0]);
    auto dictionary_stream = make_stream(substreams[1]);
    auto postings_stream = make_stream(substreams[2]);

    sparse_index_stream->seekToStart();

    MergeTreeIndexInputStreams streams;
    streams[MergeTreeIndexSubstream::Type::Regular] = sparse_index_stream.get();
    streams[MergeTreeIndexSubstream::Type::TextIndexDictionary] = dictionary_stream.get();
    streams[MergeTreeIndexSubstream::Type::TextIndexPostings] = postings_stream.get();

    auto granule_ptr = index.index->createIndexGranule();
    granule_ptr->deserializeBinaryWithMultipleStreams(streams, state);
    auto granule = std::dynamic_pointer_cast<const MergeTreeIndexGranuleText>(granule_ptr);

    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index {} produced a granule of unexpected type", index.index->index.name);

    const auto & analyzer = granule->getAnalyzer();
    const auto & tokens = resolved.query->getTokens();

    /// One granule per part (GRANULARITY ignored), so the dictionary cardinality is the exact part-wide count; no posting I/O.
    if (single_token)
    {
        const auto & token_infos = analyzer.getAllTokenInfos();
        auto it = token_infos.find(tokens.front());
        return it == token_infos.end() ? 0 : static_cast<UInt64>(it->second->cardinality);
    }

    /// `is_failed`: e.g. All mode with a token missing from the part. An empty part matches nothing.
    if (data_part->rows_count == 0)
        return 0;

    const auto & query_builder = analyzer.getQueryBuilder(*resolved.query);
    if (query_builder.is_failed)
        return 0;

    const RowsRange full_range(0, data_part->rows_count - 1);
    auto postings_serialization = PostingsSerialization(
        PostingListCodecFactory::createPostingListCodec(granule->getPostingsCodecType()),
        granule->getSerializationVersion());

    const PostingBlockReader<CheckCancelledCallback> posting_reader(
        *postings_stream, state, postings_serialization, granule->getIndexIdForCaches(), check_cancelled);

    /// `analyzePostings` already folded the small (single-block) postings into `query_builder.postings` by search mode.
    std::vector<const TokenPostingsInfo *> tokens_to_read;
    tokens_to_read.reserve(query_builder.tokens.size());
    for (const auto & [token, token_info] : query_builder.tokens)
        if (!analyzer.hasReadPostings(token))
            tokens_to_read.push_back(token_info.get());

    if (tokens_to_read.empty())
        return query_builder.postings ? query_builder.postings->cardinality() : 0;

    if (resolved.query->getSearchMode() != TextSearchMode::All)
    {
        std::optional<PostingList> merged_postings = query_builder.postings;
        for (const auto * token_info : tokens_to_read)
        {
            check_cancelled();
            auto token_postings = posting_reader.read(*token_info, full_range, nullptr);
            if (!merged_postings)
                merged_postings = std::move(token_postings);
            else
                *merged_postings |= token_postings;
        }
        return merged_postings ? merged_postings->cardinality() : 0;
    }

    /// Candidate-driven intersection: start from the folded baseline (or the rarest unread token when
    /// nothing was folded) and read the remaining tokens' blocks only where candidates survive,
    /// rarest first, mirroring the reader's lazy intersection.
    std::sort(tokens_to_read.begin(), tokens_to_read.end(),
        [](const auto * lhs, const auto * rhs) { return lhs->cardinality < rhs->cardinality; });

    std::optional<PostingList> candidates = query_builder.postings;
    size_t next = 0;
    if (!candidates)
    {
        candidates = posting_reader.read(*tokens_to_read.front(), full_range, nullptr);
        next = 1;
    }

    while (next < tokens_to_read.size() && !candidates->isEmpty())
    {
        check_cancelled();
        const RowsRange candidate_range(candidates->minimum(), candidates->maximum());
        *candidates &= posting_reader.read(*tokens_to_read[next], candidate_range, &*candidates);
        ++next;
    }

    return candidates ? candidates->cardinality() : 0;
}

/// Emits one chunk with a single `count()` aggregate state per part, claiming parts from a shared queue.
class TextIndexCountSource : public ISource
{
public:
    struct State
    {
        RangesInDataParts parts;
        std::atomic<size_t> next_part{0};
    };
    using StatePtr = std::shared_ptr<State>;

    TextIndexCountSource(
        SharedHeader header,
        StatePtr state_,
        std::shared_ptr<const ReadFromTextIndexCount::ResolvedQuery> resolved_,
        MergeTreeReaderSettings reader_settings_,
        QueryStatusPtr query_status_)
        : ISource(std::move(header))
        , state(std::move(state_))
        , resolved(std::move(resolved_))
        , reader_settings(std::move(reader_settings_))
        , query_status(std::move(query_status_))
    {
    }

    String getName() const override { return "TextIndexCount"; }

protected:
    Chunk generate() override
    {
        auto component_guard = Coordination::setCurrentComponent("TextIndexCountSource::generate");

        size_t part_idx = state->next_part.fetch_add(1);
        if (part_idx >= state->parts.size())
            return {};

        auto check_cancelled = [this]
        {
            if (isCancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
            if (query_status)
                query_status->checkTimeLimit();
        };

        UInt64 count = computeCountForPart(state->parts[part_idx], *resolved, reader_settings, check_cancelled);

        auto agg_count = std::make_shared<AggregateFunctionCount>(DataTypes{});
        return Chunk(Columns{createSingleCountStateColumn(agg_count, count)}, 1);
    }

private:
    StatePtr state;
    std::shared_ptr<const ReadFromTextIndexCount::ResolvedQuery> resolved;
    MergeTreeReaderSettings reader_settings;
    QueryStatusPtr query_status;
};

SharedHeader makeCountHeader(const String & count_column_name)
{
    auto agg_count = std::make_shared<AggregateFunctionCount>(DataTypes{});
    auto type = std::make_shared<DataTypeAggregateFunction>(agg_count, DataTypes{}, Array{});
    return std::make_shared<const Block>(Block{{type->createColumn(), type, count_column_name}});
}

}

ReadFromTextIndexCount::ReadFromTextIndexCount(
    RangesInDataParts parts_,
    ResolvedQuery resolved_,
    MergeTreeReaderSettings reader_settings_,
    const String & count_column_name,
    size_t num_streams_)
    : ISourceStep(makeCountHeader(count_column_name))
    , parts(std::move(parts_))
    , resolved(std::make_shared<const ResolvedQuery>(std::move(resolved_)))
    , reader_settings(std::move(reader_settings_))
    , num_streams(num_streams_)
{
}

void ReadFromTextIndexCount::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto state = std::make_shared<TextIndexCountSource::State>();
    state->parts = std::move(parts);

    size_t streams = std::max<size_t>(1, std::min(num_streams, state->parts.size()));

    Pipes pipes;
    for (size_t i = 0; i < streams; ++i)
        pipes.emplace_back(std::make_shared<TextIndexCountSource>(
            getOutputHeader(), state, resolved, reader_settings, settings.process_list_element));

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

}
