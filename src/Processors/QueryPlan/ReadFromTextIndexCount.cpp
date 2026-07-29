#include <Processors/QueryPlan/ReadFromTextIndexCount.h>

#include <AggregateFunctions/AggregateFunctionCount.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IPostingListCodec.h>
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
}

namespace
{

/// Counts matching rows in one part from the text-index posting metadata, without reading rows.
UInt64 computeCountForPart(
    const RangesInDataPart & part_with_ranges,
    const ReadFromTextIndexCount::ResolvedQuery & resolved,
    const MergeTreeReaderSettings & reader_settings)
{
    const auto & data_part = part_with_ranges.data_part;
    const auto & index = resolved.index;

    auto index_format = index.index->getDeserializedFormat(*data_part, index.index->getFileName());
    if (!index_format)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Text index {} is not materialized in part {}. It must have been checked at plan time",
            index.index->index.name, data_part->name);

    MergeTreeIndexDeserializationState state
    {
        .version = index_format.version,
        .condition = resolved.condition.get(),
        .part = *data_part,
        .index = *index.index,
        .readable_ranges = nullptr,
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
    if (tokens.size() == 1)
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

    /// Reads and unions the token's posting blocks that overlap `range`.
    /// With `candidates`, additionally skips blocks whose row range contains no candidate rows.
    auto read_postings = [&](const TokenPostingsInfo & token_info, const RowsRange & range, const PostingList * candidates)
    {
        if (token_info.embedded_postings)
            return *token_info.embedded_postings;

        PostingList postings;
        for (size_t block_idx : token_info.getBlocksToRead(range))
        {
            if (candidates)
            {
                const auto & block_range = token_info.ranges[block_idx];
                UInt64 up_to_end = candidates->rank(static_cast<UInt32>(block_range.end));
                UInt64 before_begin = block_range.begin == 0 ? 0 : candidates->rank(static_cast<UInt32>(block_range.begin - 1));
                if (up_to_end == before_begin)
                    continue;
            }

            auto block = MergeTreeIndexGranuleText::readPostingsBlock(
                *postings_stream, state, token_info, block_idx, postings_serialization, granule->getIndexIdForCaches());
            if (block)
                postings |= *block;
        }
        return postings;
    };

    if (resolved.query->getSearchMode() != TextSearchMode::All)
    {
        std::optional<PostingList> merged_postings;
        for (const auto & [_, token_info] : query_builder.tokens)
        {
            auto token_postings = read_postings(*token_info, full_range, nullptr);
            if (!merged_postings)
                merged_postings = std::move(token_postings);
            else
                *merged_postings |= token_postings;
        }
        return merged_postings ? merged_postings->cardinality() : 0;
    }

    /// Candidate-driven intersection: start from the rarest token (cardinalities are known from
    /// the dictionary before any posting I/O) and read the other tokens' posting blocks only
    /// where candidates survive, mirroring the reader's lazy intersection.
    std::vector<const TokenPostingsInfo *> token_infos;
    token_infos.reserve(query_builder.tokens.size());
    for (const auto & [_, token_info] : query_builder.tokens)
        token_infos.push_back(token_info.get());
    std::sort(token_infos.begin(), token_infos.end(), [](const auto * lhs, const auto * rhs) { return lhs->cardinality < rhs->cardinality; });

    PostingList candidates = read_postings(*token_infos.front(), full_range, nullptr);
    for (size_t i = 1; i < token_infos.size() && !candidates.isEmpty(); ++i)
    {
        const RowsRange candidate_range(candidates.minimum(), candidates.maximum());
        candidates &= read_postings(*token_infos[i], candidate_range, &candidates);
    }

    return candidates.cardinality();
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
        MergeTreeReaderSettings reader_settings_)
        : ISource(std::move(header))
        , state(std::move(state_))
        , resolved(std::move(resolved_))
        , reader_settings(std::move(reader_settings_))
    {
    }

    String getName() const override { return "TextIndexCount"; }

protected:
    Chunk generate() override
    {
        size_t part_idx = state->next_part.fetch_add(1);
        if (part_idx >= state->parts.size())
            return {};

        UInt64 count = computeCountForPart(state->parts[part_idx], *resolved, reader_settings);

        auto agg_count = std::make_shared<AggregateFunctionCount>(DataTypes{});
        return Chunk(Columns{createSingleCountStateColumn(agg_count, count)}, 1);
    }

private:
    StatePtr state;
    std::shared_ptr<const ReadFromTextIndexCount::ResolvedQuery> resolved;
    MergeTreeReaderSettings reader_settings;
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

void ReadFromTextIndexCount::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto state = std::make_shared<TextIndexCountSource::State>();
    state->parts = std::move(parts);

    size_t streams = std::max<size_t>(1, std::min(num_streams, state->parts.size()));

    Pipes pipes;
    for (size_t i = 0; i < streams; ++i)
        pipes.emplace_back(std::make_shared<TextIndexCountSource>(getOutputHeader(), state, resolved, reader_settings));

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

}
