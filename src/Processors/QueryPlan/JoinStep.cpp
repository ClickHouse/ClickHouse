#include <Processors/QueryPlan/JoinStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>
#include <Processors/Transforms/ColumnPermuteTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

std::vector<std::pair<String, String>> describeJoinActions(const JoinPtr & join)
{
    std::vector<std::pair<String, String>> description;
    const auto & table_join = join->getTableJoin();

    description.emplace_back("Type", toString(table_join.kind()));
    description.emplace_back("Strictness", toString(table_join.strictness()));
    description.emplace_back("Algorithm", join->getName());

    if (table_join.strictness() == JoinStrictness::Asof)
        description.emplace_back("ASOF inequality", toString(table_join.getAsofInequality()));

    if (!table_join.getClauses().empty())
        description.emplace_back("Clauses", TableJoin::formatClauses(table_join.getClauses(), true /*short_format*/));

    return description;
}

size_t getPrefixLength(const NameSet & prefix, const Names & names)
{
    size_t i = 0;
    for (; i < names.size(); ++i)
    {
        if (!prefix.contains(names[i]))
            break;
    }
    LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{}: [{}] [{}] -> {}", __FILE__, __LINE__, fmt::join(names, ", "), fmt::join(prefix, ", "), i);
    return i;
}

std::vector<size_t> getPermutationToRotate(size_t prefix_size, size_t total_size)
{
    std::vector<size_t> permutation(total_size);
    size_t i = prefix_size;
    for (auto & elem : permutation)
    {
        elem = i;
        i = (i + 1) % total_size;
    }
    return permutation;
}

Block rotateBlock(const Block & block, size_t prefix_size)
{
    auto columns = block.getColumnsWithTypeAndName();
    std::rotate(columns.begin(), columns.begin() + prefix_size, columns.end());
    auto res = Block(std::move(columns));
    return res;
}

NameSet getNameSetFromBlock(const Block & block)
{
    NameSet names;
    for (const auto & column : block)
        names.insert(column.name);
    return names;
}

Block rotateBlock(const Block & block, const Block & prefix_block)
{
    NameSet prefix_names_set = getNameSetFromBlock(prefix_block);
    size_t prefix_size = getPrefixLength(prefix_names_set, block.getNames());
    return rotateBlock(block, prefix_size);
}

}

JoinStep::JoinStep(
    const DataStream & left_stream_,
    const DataStream & right_stream_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    bool keep_left_read_in_order_)
    : join(std::move(join_)), max_block_size(max_block_size_), max_streams(max_streams_), keep_left_read_in_order(keep_left_read_in_order_)
{
    updateInputStreams(DataStreams{left_stream_, right_stream_});
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    NameSet rhs_names = getNameSetFromBlock(pipelines[1]->getHeader());

    if (swap_streams)
        std::swap(pipelines[0], pipelines[1]);

    if (join->pipelineType() == JoinPipelineType::YShaped)
    {
        auto joined_pipeline = QueryPipelineBuilder::joinPipelinesYShaped(
            std::move(pipelines[0]), std::move(pipelines[1]), join, join_algorithm_header, max_block_size, &processors);
        joined_pipeline->resize(max_streams);
        return joined_pipeline;
    }

    auto pipeline = QueryPipelineBuilder::joinPipelinesRightLeft(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        join_algorithm_header,
        max_block_size,
        max_streams,
        keep_left_read_in_order,
        &processors);

    const auto & result_names = pipeline->getHeader().getNames();
    size_t prefix_size = getPrefixLength(rhs_names, result_names);
    if (0 < prefix_size && prefix_size < result_names.size())
    {
        auto column_permutation = getPermutationToRotate(prefix_size, result_names.size());
        pipeline->addSimpleTransform([column_perm = std::move(column_permutation)](const Block & header)
        {
            return std::make_shared<ColumnPermuteTransform>(header, std::move(column_perm));
        });
    }

    return pipeline;
}

bool JoinStep::allowPushDownToRight() const
{
    return join->pipelineType() == JoinPipelineType::YShaped || join->pipelineType() == JoinPipelineType::FillRightFirst;
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void JoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    for (const auto & [name, value] : describeJoinActions(join))
        settings.out << prefix << name << ": " << value << '\n';
}

void JoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions(join))
        map.add(name, value);
}

void JoinStep::updateOutputStream()
{
    const auto & header = swap_streams ? input_streams[1].header : input_streams[0].header;

    Block result_header = JoiningTransform::transformHeader(header, join);

    join_algorithm_header = result_header;
    if (swap_streams)
        result_header = rotateBlock(result_header, input_streams[1].header);

    output_stream = DataStream { .header = result_header };
}

static ITransformingStep::Traits getStorageJoinTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

FilledJoinStep::FilledJoinStep(const DataStream & input_stream_, JoinPtr join_, size_t max_block_size_)
    : ITransformingStep(
        input_stream_,
        JoiningTransform::transformHeader(input_stream_.header, join_),
        getStorageJoinTraits())
    , join(std::move(join_))
    , max_block_size(max_block_size_)
{
    if (!join->isFilled())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "FilledJoinStep expects Join to be filled");
}

void FilledJoinStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    bool default_totals = false;
    if (!pipeline.hasTotals() && join->getTotals())
    {
        pipeline.addDefaultTotals();
        default_totals = true;
    }

    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(pipeline.getNumStreams());

    pipeline.addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        auto counter = on_totals ? nullptr : finish_counter;
        return std::make_shared<JoiningTransform>(header, output_stream->header, join, max_block_size, on_totals, default_totals, counter);
    });
}

void FilledJoinStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), JoiningTransform::transformHeader(input_streams.front().header, join), getDataStreamTraits());
}

void FilledJoinStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    for (const auto & [name, value] : describeJoinActions(join))
        settings.out << prefix << name << ": " << value << '\n';
}

void FilledJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions(join))
        map.add(name, value);
}

}
