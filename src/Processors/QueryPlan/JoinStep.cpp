#include <Processors/QueryPlan/JoinStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/isNull.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/ActionsDAG.h>
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
        description.emplace_back("Clauses", table_join.formatClauses(table_join.getClauses(), true /*short_format*/));

    return description;
}

}

using FunctionEquals = FunctionComparison<EqualsOp, NameEquals>;

JoinStep::~JoinStep() = default;

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

static ActionsDAGPtr buildFilterDefaultDAG(const Block & in_header, const Block & out_header)
{
    auto dag = std::make_shared<ActionsDAG>();
    ActionsDAG::NodeRawConstPtrs atoms;
    atoms.reserve(in_header.columns());

    FunctionOverloadResolverPtr func_builder_equals
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionEquals>(false));

    FunctionOverloadResolverPtr func_builder_is_null
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionIsNull>());

    dag->getOutputs().reserve(1 + in_header.columns());
    dag->getOutputs().push_back(nullptr);

    for (const auto & in_col : in_header)
    {
        const auto * out_col = out_header.findByName(in_col.name);

        if (!out_col)
            continue;

        const ActionsDAG::Node * atom = &dag->addInput(*out_col);
        dag->getOutputs().push_back(atom);

        if (atom->result_type->isNullable())
        {
            atom = &dag->addFunction(func_builder_is_null, {atom}, {});
        }
        else
        {
            ColumnPtr default_col = out_col->type->createColumnConstWithDefaultValue(0);
            const ActionsDAG::Node * default_node = &dag->addColumn({default_col, out_col->type, {}});

            atom = &dag->addFunction(func_builder_equals, {atom, default_node}, {});
        }

        atoms.push_back(atom);
    }

    if (atoms.empty())
        return nullptr;

    const ActionsDAG::Node * cond_node;

    if (atoms.size() > 1)
    {
        FunctionOverloadResolverPtr func_builder_and
            = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());

        cond_node = &dag->addFunction(func_builder_and, std::move(atoms), {});
    }
    else
        cond_node = atoms.at(0);

    FunctionOverloadResolverPtr func_builder_not
        = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionNot>());

    cond_node = &dag->addFunction(func_builder_not, {cond_node}, {});

    dag->getOutputs()[0] = cond_node;
    return dag;
}

QueryPipelineBuilderPtr JoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinStep expect two input steps");

    if (join->pipelineType() == JoinPipelineType::YShaped)
    {
        auto joined_pipeline = QueryPipelineBuilder::joinPipelinesYShaped(
            std::move(pipelines[0]), std::move(pipelines[1]), join, output_stream->header, max_block_size, &processors);
        joined_pipeline->resize(max_streams);
        return joined_pipeline;
    }

    auto res = QueryPipelineBuilder::joinPipelinesRightLeft(
        std::move(pipelines[0]),
        std::move(pipelines[1]),
        join,
        output_stream->header,
        max_block_size,
        max_streams,
        keep_left_read_in_order,
        &processors);

    if (filter_defaults_idx)
    {
        const auto & in_header = getInputStreams()[*filter_defaults_idx].header;
        const auto & out_header = getOutputStream().header;
        // std::cerr << in_header.dumpStructure() << std::endl;
        // std::cerr << out_header.dumpStructure() << std::endl;

        if (auto filter_defaults = buildFilterDefaultDAG(in_header, out_header))
        {
            auto filter_defaults_expr = std::make_shared<ExpressionActions>(filter_defaults, settings.getActionsSettings());
            // std::cerr << "===================== filter_defaults\n";
            // std::cerr << res->getHeader().dumpStructure() << std::endl;
            // std::cerr << filter_defaults_expr->dumpActions() << std::endl;
            res->addSimpleTransform([&](const Block & header, QueryPipelineBuilder::StreamType stream_type)
            {
                bool on_totals = stream_type == QueryPipelineBuilder::StreamType::Totals;
                return std::make_shared<FilterTransform>(header, filter_defaults_expr, filter_defaults->getOutputs().at(0)->result_name, true, on_totals);
            });
        }
    }

    return res;
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
    output_stream = DataStream
    {
        .header = JoiningTransform::transformHeader(input_streams[0].header, join),
    };
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
