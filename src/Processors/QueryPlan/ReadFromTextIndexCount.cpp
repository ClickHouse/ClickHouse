#include <Processors/QueryPlan/ReadFromTextIndexCount.h>

#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/PostingListCursor.h>
#include <Storages/MergeTree/TextIndexAnalyzer.h>
#include <Storages/MergeTree/TextIndexUtils.h>

#include <algorithm>
#include <mutex>

namespace DB
{

namespace Setting
{
    extern const SettingsTextIndexPostingListApplyMode text_index_posting_list_apply_mode;
    extern const SettingsTextIndexPostingsIntersectionAlgorithm text_index_postings_intersection_algorithm;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace
{

/// The number of postings in the rows of `range`.
UInt64 countInRange(const PostingList & postings, const RowsRange & range)
{
    UInt64 up_to_end = postings.rank(static_cast<UInt32>(range.end));
    UInt64 before_begin = range.begin == 0 ? 0 : postings.rank(static_cast<UInt32>(range.begin - 1));
    return up_to_end - before_begin;
}

/// The rows of the selected ranges of a part in the rows of `range`.
size_t countSelectedRows(const RangesInDataPart & part, const RowsRange & range)
{
    const auto & index_granularity = *part.data_part->index_granularity;
    size_t rows = 0;

    for (const auto & mark_range : part.ranges)
    {
        size_t begin = std::max(index_granularity.getMarkStartingRow(mark_range.begin), range.begin);
        size_t end = std::min(index_granularity.getMarkStartingRow(mark_range.end), range.end + 1);
        if (begin < end)
            rows += end - begin;
    }

    return rows;
}

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

            if (candidates && countInRange(*candidates, token_info.ranges[block_idx]) == 0)
                continue;

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

/// Counts the rows matched by the posting-list cursors in `range`, without materializing the postings.
/// The cursors are applied window by window to a reusable byte filter, the same way the lazy reader fills its columns.
template <typename CheckCancelledCallback>
UInt64 countWithCursors(
    const std::vector<PostingListCursorPtr> & cursors,
    TextSearchMode search_mode,
    TextIndexPostingsIntersectionAlgorithm intersection_algorithm,
    const RowsRange & range,
    const CheckCancelledCallback & check_cancelled)
{
    static constexpr size_t window_size = 65536;

    std::vector<PostingListCursor *> sorted_cursors;
    sorted_cursors.reserve(cursors.size());

    for (const auto & cursor : cursors)
        sorted_cursors.push_back(cursor.get());

    if (search_mode == TextSearchMode::Any)
    {
        sortCursorsForUnion(sorted_cursors);
    }
    else
    {
        sortCursorsForIntersection(sorted_cursors);
        intersection_algorithm = chooseIntersectionAlgorithm(sorted_cursors, intersection_algorithm);
    }

    LazyPostingsStats stats;
    auto filter = ColumnUInt8::create();
    auto & filter_data = filter->getData();
    UInt64 count = 0;

    for (size_t row_offset = range.begin; row_offset <= range.end; row_offset += window_size)
    {
        check_cancelled();

        const size_t num_rows = std::min(window_size, range.end - row_offset + 1);
        filter_data.clear();
        filter_data.resize_fill(num_rows, 0);

        const bool may_be_true = search_mode == TextSearchMode::Any
            ? lazyUnionPostingLists(*filter, sorted_cursors, 0, row_offset, num_rows)
            : lazyIntersectPostingLists(*filter, sorted_cursors, 0, row_offset, num_rows, intersection_algorithm, stats);

        if (may_be_true)
            count += countBytesInFilter(filter_data);
    }

    return count;
}

/// The text index granule of a part, shared by the slices of the part.
struct SharedGranule
{
    std::once_flag once;
    std::shared_ptr<const MergeTreeIndexGranuleText> granule;
};

/// Counts matching rows of one part in the row range `slice` from the text-index posting metadata, without reading rows.
/// `check_cancelled` is polled between posting blocks and tokens so a large part stays interruptible.
template <typename CheckCancelledCallback>
UInt64 computeCountForPart(
    const RangesInDataPart & part_with_ranges,
    const RowsRange & slice,
    SharedGranule & shared_granule,
    const ReadFromTextIndexCount::ResolvedQuery & resolved,
    const MergeTreeReaderSettings & reader_settings,
    const CheckCancelledCallback & check_cancelled)
{
    const auto & data_part = part_with_ranges.data_part;
    const auto & index = resolved.index;

    if (data_part->rows_count == 0)
        return 0;

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
        .text_index_read_postings = !single_token,
        .reader_settings = reader_settings,
    };

    const auto substreams = index.index->getSubstreams();

    /// The deserialization analyzes the tokens in the dictionary and folds their small postings for the whole part,
    /// so it is done once per part, by the first of its slices.
    std::call_once(shared_granule.once, [&]
    {
        auto sparse_index_stream = makeTextIndexInputStream(
            part_info,
            index.index->getFileName(),
            substreams[0],
            reader_settings,
            /*expected_buffer_size=*/ std::nullopt);

        sparse_index_stream->seekToStart();

        /// The analysis opens the dictionary and postings streams itself
        MergeTreeIndexInputStreams streams;
        streams[MergeTreeIndexSubstream::Type::Regular] = sparse_index_stream.get();

        auto granule_ptr = index.index->createIndexGranule();
        granule_ptr->deserializeBinaryWithMultipleStreams(streams, state);
        shared_granule.granule = std::dynamic_pointer_cast<const MergeTreeIndexGranuleText>(granule_ptr);
    });

    const auto & granule = shared_granule.granule;
    if (!granule)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index {} produced a granule of unexpected type", index.index->index.name);

    const auto & analyzer = granule->getAnalyzer();
    const auto & tokens = resolved.query->getTokens();

    /// One granule per part (GRANULARITY ignored), so the dictionary cardinality is the exact part-wide count; no posting I/O.
    /// The parts are not split into slices for a single token.
    if (single_token)
    {
        chassert(slice.begin == 0 && slice.end + 1 == data_part->rows_count);
        const auto & token_infos = analyzer.getAllTokenInfos();
        auto it = token_infos.find(tokens.front());
        return it == token_infos.end() ? 0 : static_cast<UInt64>(it->second->cardinality);
    }

    const auto & query_builder = analyzer.getQueryBuilder(*resolved.query);
    if (query_builder.is_failed)
        return 0;

    /// `analyzePostings` already folded the small (single-block) postings into `query_builder.postings` by search mode.
    std::vector<const TokenPostingsInfo *> tokens_to_read;
    tokens_to_read.reserve(query_builder.tokens.size());

    for (const auto & [token, token_info] : query_builder.tokens)
    {
        if (!analyzer.hasReadPostings(token))
            tokens_to_read.push_back(token_info.get());
    }

    /// The count of the whole part is reported by the slice at its beginning.
    if (tokens_to_read.empty())
        return query_builder.postings && slice.begin == 0 ? query_builder.postings->cardinality() : 0;

    const auto & settings = resolved.condition->getContext()->getSettingsRef();
    const auto search_mode = resolved.query->getSearchMode();

    /// Same conditions as the lazy mode of `MergeTreeReaderTextIndex`: cursors need the per-segment block index of compressed postings.
    const bool use_lazy_mode = settings[Setting::text_index_posting_list_apply_mode] == TextIndexPostingListApplyMode::Lazy
        && granule->getPostingsCodecType() != IPostingListCodec::Type::None
        && granule->getSerializationVersion() >= MergeTreeTextIndexSerializationVersion::V1_WithCodec;

    if (use_lazy_mode)
    {
        /// `rows_range` is the hull of the tokens' rows for `Any` and their overlap for `All`; an empty overlap fails the query.
        chassert(query_builder.rows_range);
        auto range = query_builder.rows_range->intersectWith(slice);
        if (!range)
            return 0;

        /// Each cursor seeks its own stream, so the cursors do not invalidate each other's buffered reads.
        std::vector<std::unique_ptr<MergeTreeReaderStream>> cursor_streams;
        std::vector<PostingListCursorPtr> cursors;
        cursor_streams.reserve(tokens_to_read.size());
        cursors.reserve(tokens_to_read.size() + 1);

        for (const auto * token_info : tokens_to_read)
        {
            if (!(token_info->header & PostingsSerialization::Flags::IsCompressed))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected uncompressed multi-block posting list in text index {}", index.index->index.name);

            cursor_streams.push_back(makeTextIndexInputStream(
                part_info,
                index.index->getFileName(),
                substreams[2],
                reader_settings,
                estimatePostingListBufferSize(*token_info)));

            cursors.push_back(std::make_shared<PostingListCursor>(
                *cursor_streams.back(),
                *token_info,
                resolved.condition->postingsCache().get(),
                granule->getIndexIdForCaches()));
        }

        /// The folded small postings join the intersection or union as one more cursor over a flat array.
        if (query_builder.postings && !query_builder.postings->isEmpty())
            cursors.push_back(std::make_shared<PostingListCursor>(query_builder.getFlatPostings()));

        auto intersection_algorithm = settings[Setting::text_index_postings_intersection_algorithm];
        return countWithCursors(cursors, search_mode, intersection_algorithm, *range, check_cancelled);
    }

    auto postings_serialization = PostingsSerialization(
        PostingListCodecFactory::createPostingListCodec(granule->getPostingsCodecType()),
        granule->getSerializationVersion());

    /// The blocks of a list are read one by one after a seek each, so the buffer fits the largest segment among the lists.
    size_t largest_segment_bytes = 0;
    for (const auto * token_info : tokens_to_read)
        largest_segment_bytes = std::max(largest_segment_bytes, estimatePostingListBufferSize(*token_info));

    auto postings_stream = makeTextIndexInputStream(
        part_info,
        index.index->getFileName(),
        substreams[2],
        reader_settings,
        largest_segment_bytes);

    const PostingBlockReader<CheckCancelledCallback> posting_reader(
        *postings_stream,
        state,
        postings_serialization,
        granule->getIndexIdForCaches(),
        check_cancelled);

    if (search_mode != TextSearchMode::All)
    {
        std::optional<PostingList> merged_postings = query_builder.postings;
        for (const auto * token_info : tokens_to_read)
        {
            check_cancelled();
            auto token_postings = posting_reader.read(*token_info, slice, nullptr);
            if (!merged_postings)
                merged_postings = std::move(token_postings);
            else
                *merged_postings |= token_postings;
        }
        return merged_postings ? countInRange(*merged_postings, slice) : 0;
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
        candidates = posting_reader.read(*tokens_to_read.front(), slice, nullptr);
        next = 1;
    }

    while (next < tokens_to_read.size() && !candidates->isEmpty())
    {
        check_cancelled();
        const RowsRange candidate_range(candidates->minimum(), candidates->maximum());
        *candidates &= posting_reader.read(*tokens_to_read[next], candidate_range, &*candidates);
        ++next;
    }

    return candidates ? countInRange(*candidates, slice) : 0;
}

/// Emits one chunk with a single `count()` aggregate state per slice of rows of a part, claiming slices from a shared queue.
class TextIndexCountSource : public ISource
{
public:
    struct State
    {
        RangesInDataParts parts;
        std::vector<SharedGranule> granules;
        /// The index of the part and the range of its rows.
        std::vector<std::pair<size_t, RowsRange>> slices;
        std::atomic<size_t> next_slice{0};
    };
    using StatePtr = std::shared_ptr<State>;

    TextIndexCountSource(
        SharedHeader header,
        StatePtr state_,
        std::shared_ptr<const ReadFromTextIndexCount::ResolvedQuery> resolved_,
        MergeTreeReaderSettings reader_settings_,
        QueryStatusPtr query_status_)
        : ISource(std::move(header), /*enable_auto_progress=*/ false)
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

        size_t slice_idx = state->next_slice.fetch_add(1);
        if (slice_idx >= state->slices.size())
            return {};

        auto check_cancelled = [this]
        {
            if (isCancelled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");
            if (query_status)
                query_status->checkTimeLimit();
        };

        const auto & [part_idx, rows] = state->slices[slice_idx];
        const auto & part = state->parts[part_idx];
        UInt64 count = computeCountForPart(part, rows, state->granules[part_idx], *resolved, reader_settings, check_cancelled);

        /// Report the rows of the selected ranges as processed instead of the one-row output chunk,
        /// the same as `ReadFromMergeTree` does, so `read_rows` and the `READ_ROWS` quota match the unoptimized read.
        progress(countSelectedRows(part, rows), 0);
        auto agg_count = std::make_shared<AggregateFunctionCount>(DataTypes{});
        return Chunk(Columns{createSingleCountStateColumn(agg_count, count)}, 1);
    }

private:
    StatePtr state;
    std::shared_ptr<const ReadFromTextIndexCount::ResolvedQuery> resolved;
    MergeTreeReaderSettings reader_settings;
    QueryStatusPtr query_status;
};

/// The size of a slice of rows of a part counted by one source.
constexpr size_t count_slice_rows = 10'000'000;

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
    state->granules = std::vector<SharedGranule>(state->parts.size());

    /// With the cursors of the lazy mode, the parts are split into slices claimed by all streams,
    /// so a large part is counted in parallel. Otherwise each part is one slice.
    const auto & query_settings = resolved->condition->getContext()->getSettingsRef();
    const auto & text_index = typeid_cast<const MergeTreeIndexText &>(*resolved->index.index);
    const bool split_parts = query_settings[Setting::text_index_posting_list_apply_mode] == TextIndexPostingListApplyMode::Lazy
        && text_index.getPostingListCodec()->getType() != IPostingListCodec::Type::None
        && resolved->query->getTokens().size() > 1;

    for (size_t part_idx = 0; part_idx < state->parts.size(); ++part_idx)
    {
        const size_t part_rows = state->parts[part_idx].data_part->rows_count;
        const size_t slice_rows = split_parts ? count_slice_rows : part_rows;

        for (size_t begin = 0; begin < part_rows; begin += slice_rows)
            state->slices.emplace_back(part_idx, RowsRange(begin, std::min(begin + slice_rows, part_rows) - 1));
    }

    /// The first slices of all parts go first, so the parts are analyzed in parallel.
    std::stable_sort(state->slices.begin(), state->slices.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.second.begin < rhs.second.begin; });

    size_t streams = std::max<size_t>(1, std::min(num_streams, state->slices.size()));

    size_t total_rows = state->parts.getRowsCountAllParts();

    Pipes pipes;
    for (size_t i = 0; i < streams; ++i)
    {
        auto source = std::make_shared<TextIndexCountSource>(getOutputHeader(), state, resolved, reader_settings, settings.process_list_element);

        if (i == 0)
            source->addTotalRowsApprox(total_rows);

        pipes.emplace_back(std::move(source));
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    for (const auto & processor : pipe.getProcessors())
        processors.emplace_back(processor);

    pipeline.init(std::move(pipe));
}

}
