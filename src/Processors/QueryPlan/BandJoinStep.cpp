#include <optional>

#include <Core/Block.h>
#include <IO/Operators.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/BandJoinStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/BandJoinTransform.h>
#include <Processors/Transforms/ColumnPermuteTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// The executed kind and whether the inputs must be swapped so the point side probes, or
/// std::nullopt for a combination the band join does not execute. The single source of truth
/// for the supported join type matrix. Kinds that keep unmatched rows of the interval side
/// (RIGHT/FULL relative to the point side, and SEMI/ANTI keeping the interval side) are out
/// of scope and decline.
static std::optional<std::pair<BandJoinKind, bool>> toBandJoinKind(JoinKind kind, JoinStrictness strictness, bool point_side_is_right)
{
    const JoinKind point_side_kind = point_side_is_right ? JoinKind::Right : JoinKind::Left;

    if (strictness == JoinStrictness::All)
    {
        if (kind == JoinKind::Inner)
            return {{BandJoinKind::Inner, point_side_is_right}};
        if (kind == point_side_kind)
            return {{BandJoinKind::Left, point_side_is_right}};
        return {};
    }

    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
    {
        if (kind != point_side_kind)
            return {};
        BandJoinKind band_kind = strictness == JoinStrictness::Semi ? BandJoinKind::LeftSemi : BandJoinKind::LeftAnti;
        return {{band_kind, point_side_is_right}};
    }

    return {};
}

bool BandJoinStep::isSupportedJoinType(JoinKind kind, JoinStrictness strictness, bool point_side_is_right)
{
    return toBandJoinKind(kind, strictness, point_side_is_right).has_value();
}

BandJoinStep::BandJoinStep(
    const SharedHeader & left_header_,
    const SharedHeader & right_header_,
    BandJoinConditions conditions_,
    ExpressionActionsPtr residual_condition_,
    JoinKind kind_,
    JoinStrictness strictness_,
    bool point_side_is_right_,
    const SizeLimits & size_limits_,
    size_t max_joined_block_rows_,
    size_t max_joined_block_bytes_)
    : conditions(conditions_)
    , size_limits(size_limits_)
    , max_joined_block_rows(max_joined_block_rows_)
    , max_joined_block_bytes(max_joined_block_bytes_)
{
    auto band_kind = toBandJoinKind(kind_, strictness_, point_side_is_right_);
    if (!band_kind)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Band join does not support {} {} JOIN", toString(strictness_), toString(kind_));
    kind = band_kind->first;
    swap_inputs = band_kind->second;

    if (residual_condition_)
        residual = resolveJoinResidualCondition(std::move(residual_condition_), *left_header_, *right_header_);

    const auto & point_header = point_side_is_right_ ? right_header_ : left_header_;
    const auto & interval_header = point_side_is_right_ ? left_header_ : right_header_;
    for (const auto & condition : conditions)
    {
        if (condition.point_key_position >= point_header->columns() || condition.interval_key_position >= interval_header->columns())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Band join key positions {} and {} are out of range for inputs with {} and {} columns",
                condition.point_key_position, condition.interval_key_position, point_header->columns(), interval_header->columns());

        /// The planner casts both sides of each bound to a common type.
        const auto & point_type = point_header->getByPosition(condition.point_key_position).type;
        const auto & interval_type = interval_header->getByPosition(condition.interval_key_position).type;
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

    if (swap_inputs)
        std::swap(pipelines[0], pipelines[1]);

    auto probe_pipeline = std::move(pipelines[0]);
    auto build_pipeline = std::move(pipelines[1]);

    auto state = std::make_shared<BandJoinSharedState>();
    auto build_transform = std::make_shared<BandJoinBuildTransform>(
        build_pipeline->getSharedHeader(), conditions, size_limits, state);

    auto executed_residual = residual;
    if (swap_inputs && executed_residual)
    {
        /// The expression and positions are orientation-independent, only the source sides flip.
        for (auto & source : executed_residual->inputs)
            source.side = 1 - source.side;
    }

    SharedHeader probe_header = probe_pipeline->getSharedHeader();
    /// The probe transform emits the point-side columns before the interval-side ones.
    SharedHeader joined_header = concatHeaders({probe_header, build_pipeline->getSharedHeader()});
    auto probe_transform_factory = [this, probe_header, joined_header, state, executed_residual]()
    {
        return std::make_shared<BandJoinProbeTransform>(
            probe_header, joined_header, conditions, kind, executed_residual, state, max_joined_block_rows, max_joined_block_bytes);
    };

    auto pipeline = QueryPipelineBuilder::joinPipelinesBuildProbe(
        std::move(build_pipeline), std::move(probe_pipeline), std::move(build_transform), probe_transform_factory, &processors);

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

void BandJoinStep::updateOutputHeader()
{
    output_header = concatHeaders(input_headers);
}

String BandJoinStep::formatConditions() const
{
    const auto & point_header = input_headers[swap_inputs ? 1 : 0];
    const auto & interval_header = input_headers[swap_inputs ? 0 : 1];
    auto format_condition = [&](const BandJoinCondition & condition)
    {
        return fmt::format("{} {} {}",
            point_header->getByPosition(condition.point_key_position).name,
            toString(condition.op),
            interval_header->getByPosition(condition.interval_key_position).name);
    };
    return fmt::format("{} AND {}", format_condition(conditions[0]), format_condition(conditions[1]));
}

void BandJoinStep::describeActions(FormatSettings & settings) const
{
    settings.out << settings.detail_prefix << "Type: " << toString(kind) << '\n';
    settings.out << settings.detail_prefix << "Conditions: " << formatConditions() << '\n';
    if (residual)
        settings.out << settings.detail_prefix << "Residual filter: " << residual->actions->getSampleBlock().getByPosition(0).name << '\n';
    settings.out << settings.detail_prefix << "PointSide: " << (swap_inputs ? "Right" : "Left") << '\n';
    if (swap_inputs)
        settings.out << settings.detail_prefix << "Swapped: true\n";
}

void BandJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Type", toString(kind));
    map.add("Conditions", formatConditions());
    if (residual)
        map.add("Residual filter", residual->actions->getSampleBlock().getByPosition(0).name);
    map.add("PointSide", swap_inputs ? "Right" : "Left");
    if (swap_inputs)
        map.add("Swapped", true);
}

void BandJoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
