#include <optional>

#include <Core/Block.h>
#include <IO/Operators.h>
#include <Processors/QueryPlan/BandJoinStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/BandJoinTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// The executed kind, or std::nullopt for a combination the band join does not execute.
/// The single source of truth for the supported join type matrix.
static std::optional<BandJoinKind> toBandJoinKind(JoinKind kind, JoinStrictness strictness)
{
    if (kind == JoinKind::Inner && strictness == JoinStrictness::All)
        return BandJoinKind::Inner;
    return {};
}

bool BandJoinStep::isSupportedJoinType(JoinKind kind, JoinStrictness strictness)
{
    return toBandJoinKind(kind, strictness).has_value();
}

BandJoinStep::BandJoinStep(
    const SharedHeader & left_header_,
    const SharedHeader & right_header_,
    BandJoinConditions conditions_,
    JoinKind kind_,
    JoinStrictness strictness_,
    const SizeLimits & size_limits_,
    size_t max_joined_block_rows_,
    size_t max_joined_block_bytes_)
    : conditions(conditions_)
    , size_limits(size_limits_)
    , max_joined_block_rows(max_joined_block_rows_)
    , max_joined_block_bytes(max_joined_block_bytes_)
{
    auto band_kind = toBandJoinKind(kind_, strictness_);
    if (!band_kind)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Band join does not support {} {} JOIN", toString(strictness_), toString(kind_));
    kind = *band_kind;

    for (const auto & condition : conditions)
    {
        if (condition.point_key_position >= left_header_->columns() || condition.interval_key_position >= right_header_->columns())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Band join key positions {} and {} are out of range for inputs with {} and {} columns",
                condition.point_key_position, condition.interval_key_position, left_header_->columns(), right_header_->columns());

        /// The planner casts both sides of each bound to a common type.
        const auto & point_type = left_header_->getByPosition(condition.point_key_position).type;
        const auto & interval_type = right_header_->getByPosition(condition.interval_key_position).type;
        if (!point_type->equals(*interval_type))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Band join key types do not match: {} and {}",
                point_type->getName(), interval_type->getName());
    }

    updateInputHeaders({left_header_, right_header_});
}

/// The concatenation of the input columns: what the probe transform outputs.
static SharedHeader concatHeaders(const SharedHeaders & headers)
{
    Block result;
    for (const auto & header : headers)
        for (const auto & column : *header)
            result.insert(ColumnWithTypeAndName(column.type->createColumn(), column.type, column.name));
    return std::make_shared<const Block>(std::move(result));
}

QueryPipelineBuilderPtr BandJoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BandJoinStep expects two input pipelines, got {}", pipelines.size());

    auto probe_pipeline = std::move(pipelines[0]);
    auto build_pipeline = std::move(pipelines[1]);

    auto state = std::make_shared<BandJoinSharedState>();
    auto build_transform = std::make_shared<BandJoinBuildTransform>(
        build_pipeline->getSharedHeader(), conditions, size_limits, state);

    SharedHeader probe_header = probe_pipeline->getSharedHeader();
    auto probe_transform_factory = [this, probe_header, state]()
    {
        return std::make_shared<BandJoinProbeTransform>(
            probe_header, output_header, conditions, kind, state, max_joined_block_rows, max_joined_block_bytes);
    };

    return QueryPipelineBuilder::joinPipelinesBuildProbe(
        std::move(build_pipeline), std::move(probe_pipeline), std::move(build_transform), probe_transform_factory, &processors);
}

void BandJoinStep::updateOutputHeader()
{
    output_header = concatHeaders(input_headers);
}

String BandJoinStep::formatConditions() const
{
    auto format_condition = [&](const BandJoinCondition & condition)
    {
        return fmt::format("{} {} {}",
            input_headers[0]->getByPosition(condition.point_key_position).name,
            toString(condition.op),
            input_headers[1]->getByPosition(condition.interval_key_position).name);
    };
    return fmt::format("{} AND {}", format_condition(conditions[0]), format_condition(conditions[1]));
}

void BandJoinStep::describeActions(FormatSettings & settings) const
{
    settings.out << settings.detail_prefix << "Type: " << toString(kind) << '\n';
    settings.out << settings.detail_prefix << "Conditions: " << formatConditions() << '\n';
    settings.out << settings.detail_prefix << "PointSide: Left\n";
}

void BandJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Type", toString(kind));
    map.add("Conditions", formatConditions());
    map.add("PointSide", "Left");
}

void BandJoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
