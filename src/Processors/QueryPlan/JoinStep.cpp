#include <Processors/QueryPlan/JoinStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>

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

}

JoinStep::JoinStep(
    const Header & left_header_,
    const Header & right_header_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    bool keep_left_read_in_order_)
    : join(std::move(join_)), max_block_size(max_block_size_), max_streams(max_streams_), keep_left_read_in_order(keep_left_read_in_order_)
{
    updateInputHeaders({left_header_, right_header_});
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    if (join->pipelineType() == JoinPipelineType::YShaped)
    {
        auto joined_pipeline = QueryPipelineBuilder::joinPipelinesYShaped(
            std::move(pipelines[0]), std::move(pipelines[1]), join, *output_header, max_block_size, &processors);
        joined_pipeline->resize(max_streams);
        return joined_pipeline;
    }

    return QueryPipelineBuilder::joinPipelinesRightLeft(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        *output_header,
        max_block_size,
        max_streams,
        keep_left_read_in_order,
        &processors);
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

void JoinStep::updateOutputHeader()
{
    output_header = JoiningTransform::transformHeader(input_headers.front(), join);
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

FilledJoinStep::FilledJoinStep(const Header & input_header_, JoinPtr join_, size_t max_block_size_)
    : ITransformingStep(
        input_header_,
        JoiningTransform::transformHeader(input_header_, join_),
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
        return std::make_shared<JoiningTransform>(header, *output_header, join, max_block_size, on_totals, default_totals, counter);
    });
}

void FilledJoinStep::updateOutputHeader()
{
    output_header = JoiningTransform::transformHeader(input_headers.front(), join);
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
