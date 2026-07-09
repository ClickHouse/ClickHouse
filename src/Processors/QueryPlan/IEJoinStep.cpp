#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/IEJoinStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/ColumnPermuteTransform.h>
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

static std::pair<IEJoinKind, bool> toIEJoinKind(JoinKind kind, JoinStrictness strictness)
{
    if (strictness == JoinStrictness::All)
    {
        switch (kind)
        {
            case JoinKind::Inner: return {IEJoinKind::Inner, false};
            case JoinKind::Left: return {IEJoinKind::Left, false};
            case JoinKind::Right: return {IEJoinKind::Right, false};
            case JoinKind::Full: return {IEJoinKind::Full, false};
            default: throw Exception(ErrorCodes::LOGICAL_ERROR, "IEJoin does not support {} {} JOIN", toString(strictness), toString(kind));
        }
    }

    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
    {
        if (kind != JoinKind::Left && kind != JoinKind::Right)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "IEJoin does not support {} {} JOIN", toString(strictness), toString(kind));
        IEJoinKind ie_kind = strictness == JoinStrictness::Semi ? IEJoinKind::LeftSemi : IEJoinKind::LeftAnti;
        return {ie_kind, kind == JoinKind::Right};
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "IEJoin does not support {} {} JOIN", toString(strictness), toString(kind));
}

bool IEJoinStep::isSupportedJoinType(JoinKind kind, JoinStrictness strictness)
{
    if (strictness == JoinStrictness::All)
        return kind == JoinKind::Inner || kind == JoinKind::Left || kind == JoinKind::Right || kind == JoinKind::Full;
    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
        return kind == JoinKind::Left || kind == JoinKind::Right;
    return false;
}

IEJoinStep::IEJoinStep(
    const SharedHeader & left_header_,
    const SharedHeader & right_header_,
    IEJoinConditions conditions_,
    JoinKind kind_,
    JoinStrictness strictness_,
    bool inputs_sorted_by_first_key_,
    size_t max_block_size_)
    : conditions(conditions_)
    , inputs_sorted_by_first_key(inputs_sorted_by_first_key_)
    , max_block_size(max_block_size_)
{
    auto ie_kind = toIEJoinKind(kind_, strictness_);
    kind = ie_kind.first;
    swap_inputs = ie_kind.second;

    updateInputHeaders({left_header_, right_header_});
}


static IEJoinConditions reverseIEJoinConditions(IEJoinConditions conditions)
{
    for (auto & condition : conditions)
    {
        std::swap(condition.left_key_position, condition.right_key_position);
        condition.op = reverseInequalityOperator(condition.op);
    }
    return conditions;
}

/// The concatenation of the input columns: what the join transform itself outputs.
static SharedHeader concatHeaders(const SharedHeaders & headers)
{
    Block result;
    for (const auto & header : headers)
        for (const auto & column : *header)
            result.insert(ColumnWithTypeAndName(column.type->createColumn(), column.type, column.name));
    return std::make_shared<const Block>(std::move(result));
}

QueryPipelineBuilderPtr IEJoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "IEJoinStep expects two input pipelines, got {}", pipelines.size());

    if (swap_inputs)
        std::swap(pipelines[0], pipelines[1]);

    SharedHeaders inputs = {pipelines[0]->getSharedHeader(), pipelines[1]->getSharedHeader()};
    auto executed_conditions = swap_inputs ? reverseIEJoinConditions(conditions) : conditions;
    auto joining = std::make_shared<IEJoinTransform>(
        kind, executed_conditions, inputs_sorted_by_first_key, inputs, concatHeaders(inputs), max_block_size);
    auto pipeline = QueryPipelineBuilder::joinPipelinesPaired(std::move(pipelines[0]), std::move(pipelines[1]), std::move(joining), &processors);

    if (swap_inputs)
    {
        /// With swapped inputs the joined stream carries the right table's columns first.
        const size_t num_left = input_headers[0]->columns();
        const size_t num_right = input_headers[1]->columns();
        std::vector<size_t> permutation(num_left + num_right);
        for (size_t i = 0; i < num_left; ++i)
            permutation[i] = num_right + i;
        for (size_t i = 0; i < num_right; ++i)
            permutation[num_left + i] = i;
        pipeline->addSimpleTransform([&permutation](const SharedHeader & header)
        {
            return std::make_shared<ColumnPermuteTransform>(header, permutation);
        });
    }

    return pipeline;
}

void IEJoinStep::updateOutputHeader()
{
    output_header = concatHeaders(input_headers);
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
    settings.out << settings.detail_prefix << "Type: " << toString(kind) << '\n';
    settings.out << settings.detail_prefix << "Conditions: " << formatConditions() << '\n';
    if (swap_inputs)
        settings.out << settings.detail_prefix << "Swapped: true\n";
}

void IEJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Type", toString(kind));
    map.add("Conditions", formatConditions());
    if (swap_inputs)
        map.add("Swapped", true);
}

void IEJoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
