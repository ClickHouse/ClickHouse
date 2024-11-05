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

std::vector<size_t> getPermutationForBlock(
    const Block & block,
    const Block & lhs_block,
    const Block & rhs_block,
    const NameSet & name_filter)
{
    std::vector<size_t> permutation;
    permutation.reserve(block.columns());
    Block::NameMap name_map = block.getNamesToIndexesMap();

    bool is_trivial = true;
    for (const auto & other_block : {lhs_block, rhs_block})
    {
        for (const auto & col : other_block)
        {
            if (!name_filter.contains(col.name))
                continue;
            if (auto it = name_map.find(col.name); it != name_map.end())
            {
                is_trivial = is_trivial && it->second == permutation.size();
                permutation.push_back(it->second);
            }
        }
    }

    if (is_trivial && permutation.size() == block.columns())
        return {};

    return permutation;
}

}

JoinStep::JoinStep(
    const Header & left_header_,
    const Header & right_header_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t max_streams_,
    NameSet required_output_,
    bool keep_left_read_in_order_,
    bool use_new_analyzer_)
    : join(std::move(join_))
    , max_block_size(max_block_size_)
    , max_streams(max_streams_)
    , required_output(std::move(required_output_))
    , keep_left_read_in_order(keep_left_read_in_order_)
    , use_new_analyzer(use_new_analyzer_)
{
    updateInputHeaders({left_header_, right_header_});
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    Block lhs_header = pipelines[0]->getHeader();
    Block rhs_header = pipelines[1]->getHeader();

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

    if (!use_new_analyzer)
        return pipeline;

    auto column_permutation = getPermutationForBlock(pipeline->getHeader(), lhs_header, rhs_header, required_output);
    if (!column_permutation.empty())
    {
        pipeline->addSimpleTransform([&column_permutation](const Block & header)
        {
            return std::make_shared<ColumnPermuteTransform>(header, column_permutation);
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
    if (swap_streams)
        settings.out << prefix << "Swapped: true\n";
}

void JoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions(join))
        map.add(name, value);
    if (swap_streams)
        map.add("Swapped", true);
}

void JoinStep::setJoin(JoinPtr join_, bool swap_streams_)
{
    join_algorithm_header.clear();
    swap_streams = swap_streams_;
    join = std::move(join_);
    updateOutputHeader();
}

void JoinStep::updateOutputHeader()
{
    if (join_algorithm_header)
        return;

    const auto & header = swap_streams ? input_headers[1] : input_headers[0];

    Block result_header = JoiningTransform::transformHeader(header, join);
    join_algorithm_header = result_header;

    if (!use_new_analyzer)
    {
        if (swap_streams)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot swap streams without new analyzer");
        output_header = result_header;
        return;
    }

    auto column_permutation = getPermutationForBlock(result_header, input_headers[0], input_headers[1], required_output);
    if (!column_permutation.empty())
        result_header = ColumnPermuteTransform::permute(result_header, column_permutation);

    output_header = result_header;
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
