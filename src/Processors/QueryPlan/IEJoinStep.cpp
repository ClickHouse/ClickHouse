#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/IEJoinStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/IEJoinTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IEJoinStep::IEJoinStep(
    const SharedHeader & left_header_,
    const SharedHeader & right_header_,
    IEJoinConditions conditions_,
    size_t max_block_size_)
    : conditions(conditions_)
    , max_block_size(max_block_size_)
{
    updateInputHeaders({left_header_, right_header_});
}

QueryPipelineBuilderPtr IEJoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "IEJoinStep expects two input pipelines, got {}", pipelines.size());

    SharedHeaders inputs = {pipelines[0]->getSharedHeader(), pipelines[1]->getSharedHeader()};
    auto joining = std::make_shared<IEJoinTransform>(conditions, inputs, getOutputHeader(), max_block_size);
    return QueryPipelineBuilder::joinPipelinesPaired(std::move(pipelines[0]), std::move(pipelines[1]), std::move(joining), &processors);
}

void IEJoinStep::updateOutputHeader()
{
    Block header;
    for (const auto & input_header : input_headers)
        for (const auto & column : *input_header)
            header.insert(ColumnWithTypeAndName(column.type->createColumn(), column.type, column.name));
    output_header = std::make_shared<const Block>(std::move(header));
}

String IEJoinStep::formatConditions() const
{
    auto format_condition = [&](const IEJoinCondition & condition)
    {
        return fmt::format("{} {} {}",
            input_headers[0]->getByPosition(condition.left_key_position).name,
            toString(condition.op),
            input_headers[1]->getByPosition(condition.right_key_position).name);
    };
    return fmt::format("{} AND {}", format_condition(conditions[0]), format_condition(conditions[1]));
}

void IEJoinStep::describeActions(FormatSettings & settings) const
{
    settings.out << settings.detail_prefix << "Conditions: " << formatConditions() << '\n';
}

void IEJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Conditions", formatConditions());
}

void IEJoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
