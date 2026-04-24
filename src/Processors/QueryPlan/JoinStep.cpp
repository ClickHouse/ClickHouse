#include <Formats/FormatSettings.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>
#include <Core/BlockNameMap.h>
#include <Processors/Transforms/ColumnPermuteTransform.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

std::vector<std::pair<String, String>> describeJoinActions(const JoinPtr & join, const ExplainFormatSettings & settings)
{
    std::vector<std::pair<String, String>> description;
    const auto & table_join = join->getTableJoin();

    auto to_lower = [](String & s)
    {
        std::transform(s.begin(), s.end(), s.begin(),
            [](unsigned char c) { return std::tolower(c); });
    };

    String kind = toString(table_join.kind());
    String strictness = toString(table_join.strictness());

    if (settings.pretty)
    {
        to_lower(kind);
        to_lower(strictness);
    }

    description.emplace_back("Type", kind);
    description.emplace_back("Strictness", strictness);
    description.emplace_back("Algorithm", join->getName());

    if (table_join.strictness() == JoinStrictness::Asof)
        description.emplace_back("ASOF inequality", toString(table_join.getAsofInequality()));

    if (!table_join.getClauses().empty())
    {
        if (settings.pretty)
            description.emplace_back("Join conditions", TableJoin::formatClausesPretty(table_join.getClauses(), settings));
        else
            description.emplace_back("Clauses", TableJoin::formatClauses(table_join.getClauses(), true /*short_format*/));
    }

    if (const auto & mixed_expression = table_join.getMixedJoinExpression())
        description.emplace_back("Residual filter", mixed_expression->getSampleBlock().dumpNames());

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
    BlockNameMap name_map = getNamesToIndexesMap(block);

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
    const SharedHeader & left_header_,
    const SharedHeader & right_header_,
    JoinPtr join_,
    size_t max_block_size_,
    size_t min_block_size_rows_,
    size_t min_block_size_bytes_,
    size_t max_streams_,
    NameSet required_output_,
    bool keep_left_read_in_order_,
    bool use_new_analyzer_,
    bool use_join_disjunctions_push_down_)
    : join(std::move(join_))
    , max_block_size(max_block_size_)
    , min_block_size_rows(min_block_size_rows_)
    , min_block_size_bytes(min_block_size_bytes_)
    , max_streams(max_streams_)
    , required_output(std::move(required_output_))
    , keep_left_read_in_order(keep_left_read_in_order_)
    , use_new_analyzer(use_new_analyzer_)
    , use_join_disjunctions_push_down(use_join_disjunctions_push_down_)
    , disjunctions_optimization_applied(false)
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

    std::unique_ptr<QueryPipelineBuilder> joined_pipeline;
    /// Sharding requires both pipelines to have the same number of streams.
    /// When stream counts don't match, fall back to the
    /// regular join pipeline which handles different stream counts
    bool use_sharding = !primary_key_sharding.empty() && pipelines[0]->getNumStreams() == pipelines[1]->getNumStreams();
    if (!use_sharding)
    {
        if (join->pipelineType() == JoinPipelineType::YShaped)
        {
            joined_pipeline = QueryPipelineBuilder::joinPipelinesYShaped(
                std::move(pipelines[0]), std::move(pipelines[1]), join, join_algorithm_header, max_block_size, &processors);
            joined_pipeline->resize(max_streams);
        }
        else
        {
            joined_pipeline = QueryPipelineBuilder::joinPipelinesRightLeft(
                std::move(pipelines[0]),
                std::move(pipelines[1]),
                join,
                join_algorithm_header,
                max_block_size,
                min_block_size_rows,
                min_block_size_bytes,
                max_streams,
                keep_left_read_in_order,
                &processors);
        }
    }
    else
    {
        if (join->pipelineType() == JoinPipelineType::YShaped)
        {
            joined_pipeline = QueryPipelineBuilder::joinPipelinesYShapedByShards(
                std::move(pipelines[0]), std::move(pipelines[1]), join, join_algorithm_header, max_block_size, &processors);
        }
        else
        {
            joined_pipeline = QueryPipelineBuilder::joinPipelinesByShards(
                std::move(pipelines[0]),
                std::move(pipelines[1]),
                join,
                join_algorithm_header,
                max_block_size,
                &processors);
        }
    }

    if (!use_new_analyzer)
        return joined_pipeline;

    auto column_permutation = getPermutationForBlock(joined_pipeline->getHeader(), lhs_header, rhs_header, required_output);
    if (!column_permutation.empty())
    {
        joined_pipeline->addSimpleTransform([&column_permutation](const SharedHeader & header)
        {
            return std::make_shared<ColumnPermuteTransform>(header, column_permutation);
        });
    }

    if (join->supportParallelJoin() && (min_block_size_rows > 0 || min_block_size_bytes > 0))
    {
        joined_pipeline->addSimpleTransform(
            [&](const SharedHeader & header)
            { return std::make_shared<SimpleSquashingChunksTransform>(header, min_block_size_rows, min_block_size_bytes); });
    }

    const auto & pipeline_output_header = joined_pipeline->getHeader();
    const auto & expected_output_header = getOutputHeader();
    if (!isCompatibleHeader(pipeline_output_header, *expected_output_header))
    {
        assertBlocksHaveEqualStructure(pipeline_output_header, *expected_output_header,
            fmt::format("JoinStep: [{}] and [{}]", pipeline_output_header.dumpNames(), expected_output_header->dumpNames()));
    }

    return joined_pipeline;
}

bool JoinStep::allowPushDownToRight() const
{
    return join->pipelineType() == JoinPipelineType::YShaped || join->pipelineType() == JoinPipelineType::FillRightFirst;
}

void JoinStep::keepLeftPipelineInOrder(bool disable_squashing)
{
    if (disable_squashing)
    {
        min_block_size_rows = 0;
        min_block_size_bytes = 0;
    }
    keep_left_read_in_order = true;
}

void JoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void JoinStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    auto description = describeJoinActions(join, settings);
    const size_t inline_count = settings.pretty ? 3 : 0;

    if (settings.pretty)
    {
        if (!join_readable_relation_name.empty())
            settings.out << prefix << join_readable_relation_name << '\n';

        settings.out << prefix;
        for (size_t i = 0; i < inline_count; ++i)
        {
            if (i > 0)
                settings.out << " | ";
            auto [name, value] = description[i];
            settings.out << name << ": " << value;
        }
        settings.out << '\n';

        if (result_rows_estimation)
            settings.out << prefix << "Result rows: " << toString(*result_rows_estimation) << '\n';

        if (locality != JoinLocality::Unspecified)
            settings.out << prefix << "Locality: " << toString(locality) << '\n';
    }

    for (size_t i = inline_count; i < description.size(); ++i)
    {
        const auto & [name, value] = description[i];
        settings.out << prefix << name << ": " << value << '\n';
    }
    if (swap_streams)
        settings.out << prefix << "Swapped: true\n";
    if (!primary_key_sharding.empty())
    {
        settings.out << prefix << "Sharding: [";
        bool first = true;
        for (const auto & [lhs, rhs] : primary_key_sharding)
        {
            if (!first)
                settings.out << ", ";
            first = false;

            settings.out << "(" << lhs << " = " << rhs << ")";
        }

        settings.out << "]\n";
    }

    if (settings.pretty)
        QueryPlanFormat::formatJoinOutputColumns(settings.out, *this, prefix);
}

void JoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    WriteBufferFromOwnString dummy;
    ExplainFormatSettings dummy_settings{.out = dummy, .header_prefix = "", .detail_prefix = "", .pretty_names = {}, .runtime_filter_names = {}};

    for (const auto & [name, value] : describeJoinActions(join, dummy_settings))
        map.add(name, value);
    if (swap_streams)
        map.add("Swapped", true);
    if (!primary_key_sharding.empty())
    {
        auto array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & [lhs, rhs] : primary_key_sharding)
        {
            auto item = std::make_unique<JSONBuilder::JSONArray>();
            item->add(lhs);
            item->add(rhs);
            array->add(std::move(item));
        }
        map.add("Sharding", std::move(array));
    }
}

void JoinStep::setJoin(JoinPtr join_, bool swap_streams_)
{
    join_algorithm_header.reset();
    swap_streams = swap_streams_;
    join = std::move(join_);
    updateOutputHeader();
}

void JoinStep::setLogicalJoinInfo(LogicalJoinInfo && logical_join_info)
{
    join_readable_relation_name = std::move(logical_join_info.readable_relation_name);
    result_rows_estimation = logical_join_info.result_rows_estimation;
    locality = logical_join_info.locality;
}

void JoinStep::updateOutputHeader()
{
    if (join_algorithm_header && !join_algorithm_header->empty())
        return;

    const auto & header = swap_streams ? input_headers[1] : input_headers[0];

    join_algorithm_header = std::make_shared<const Block>(JoiningTransform::transformHeader(*header, join));

    if (!use_new_analyzer)
    {
        if (swap_streams)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot swap streams without the analyzer");
        output_header = join_algorithm_header;
        return;
    }

    auto column_permutation = getPermutationForBlock(*join_algorithm_header, *input_headers[0], *input_headers[1], required_output);
    if (!column_permutation.empty())
        output_header = std::make_shared<const Block>(ColumnPermuteTransform::permute(*join_algorithm_header, column_permutation));
    else
        output_header = join_algorithm_header;
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

FilledJoinStep::FilledJoinStep(const SharedHeader & input_header_, JoinPtr join_, size_t max_block_size_)
    : ITransformingStep(
        input_header_,
        std::make_shared<const Block>(JoiningTransform::transformHeader(*input_header_, join_)),
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
    if (!pipeline.hasTotals() && !join->getTotals().empty())
    {
        pipeline.addDefaultTotals();
        default_totals = true;
    }

    auto finish_counter = std::make_shared<FinishCounter>(pipeline.getNumStreams());

    pipeline.addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)
    {
        bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
        auto counter = on_totals ? nullptr : finish_counter;
        return std::make_shared<JoiningTransform>(header, output_header, join, max_block_size, on_totals, default_totals, counter);
    });
}

void FilledJoinStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(JoiningTransform::transformHeader(*input_headers.front(), join));
}

void FilledJoinStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    for (const auto & [name, value] : describeJoinActions(join, settings))
        settings.out << prefix << name << ": " << value << '\n';
}

void FilledJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    WriteBufferFromOwnString dummy;
    ExplainFormatSettings dummy_settings{.out = dummy, .header_prefix = "", .detail_prefix = "", .pretty_names = {}, .runtime_filter_names = {}};

    for (const auto & [name, value] : describeJoinActions(join, dummy_settings))
        map.add(name, value);
}

}
