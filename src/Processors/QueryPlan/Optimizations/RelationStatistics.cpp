#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>

#include <limits>
#include <string_view>
#include <unordered_set>

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>

namespace DB::QueryPlanOptimizations
{

void addTransformation(std::unordered_map<String, ColumnStats> & column_stats, ColumnStatsTransformation transformation)
{
    for (auto & [_, stats] : column_stats)
    {
        stats.ndv_provenance.add(transformation);
        stats.range_provenance.add(transformation);
    }
}

bool isExactDistinctCount(const ColumnStatsProvenance & provenance)
{
    constexpr UInt16 allowed = ValuePreservingExpression | ExactRowCountClamp;
    return provenance.origin == ColumnStatsOrigin::PartStatistics && provenance.hasOnly(allowed);
}

bool isDistinctCountUpperBound(const ColumnStatsProvenance & provenance)
{
    constexpr UInt16 forbidden = PartialPartCoverage | EstimatedRowCountClamp | Unsupported;
    return (provenance.origin == ColumnStatsOrigin::PartStatistics || provenance.origin == ColumnStatsOrigin::ExactRowCount)
        && !provenance.hasAny(forbidden);
}

bool isExactValueRange(const ColumnStatsProvenance & provenance)
{
    constexpr UInt16 forbidden = RowSubset | NonUniformRowSubset | NDVBoundExpression | PartialPartCoverage | Unsupported;
    return provenance.origin == ColumnStatsOrigin::PartStatistics && !provenance.hasAny(forbidden);
}

bool isValueRangeSuperset(const ColumnStatsProvenance & provenance)
{
    constexpr UInt16 forbidden = NDVBoundExpression | PartialPartCoverage | Unsupported;
    return provenance.origin == ColumnStatsOrigin::PartStatistics && !provenance.hasAny(forbidden);
}

bool isRepresentativeValueRange(const ColumnStatsProvenance & provenance)
{
    constexpr UInt16 forbidden = RowSubset | NonUniformRowSubset | NDVBoundExpression | PartialPartCoverage | Unsupported;
    return provenance.origin == ColumnStatsOrigin::PartStatistics && !provenance.hasAny(forbidden);
}

void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions)
{
    /// Column statistics are usually absent; do not pay for a full lineage walk of the
    /// `ActionsDAG` when there is nothing to remap.
    if (mapped.empty())
        return;

    std::unordered_map<String, ColumnStats> original;
    original.swap(mapped);

    const auto lineage = traceActionsDAGLineage(actions);
    const auto & inputs = actions.getInputs();
    const auto & outputs = actions.getOutputs();

    /// The result map is still name-keyed. If multiple positional outputs use the same
    /// name, retaining either output's statistics would silently attribute them to both.
    /// Drop the name regardless of whether the duplicate outputs have equal lineage.
    std::unordered_set<std::string_view> output_names;
    std::unordered_set<std::string_view> ambiguous_output_names;
    output_names.reserve(outputs.size());
    for (const auto * output : outputs)
    {
        const std::string_view output_name = output->result_name;
        if (!output_names.insert(output_name).second)
            ambiguous_output_names.insert(output_name);
    }

    for (const auto & output_lineage : lineage)
    {
        const auto & output = *outputs[output_lineage.output_position];
        if (!output_lineage.input || ambiguous_output_names.contains(output.result_name))
            continue;

        const auto stats_it = original.find(inputs[output_lineage.input->input_position]->result_name);
        if (stats_it == original.end())
            continue;

        ColumnStats stats = stats_it->second;
        /// Add the offset, guarding against overflow when the source NDV is near the maximum.
        if (stats.num_distinct_values <= std::numeric_limits<UInt64>::max() - output_lineage.input->ndv_delta)
            stats.num_distinct_values += output_lineage.input->ndv_delta;
        if (output_lineage.input->kind == ActionsDAGLineageKind::DistinctValuesBound)
        {
            stats.ndv_provenance.add(NDVBoundExpression);
            stats.range_provenance.add(NDVBoundExpression);
        }
        else if (
            output_lineage.input->kind == ActionsDAGLineageKind::ValuePreserving
            || &output != inputs[output_lineage.input->input_position])
        {
            /// The pointer inequality distinguishes an alias chain from an input exported directly;
            /// aliases intentionally remain `Identity` in the generic lineage utility.
            stats.ndv_provenance.add(ValuePreservingExpression);
            stats.range_provenance.add(ValuePreservingExpression);
        }
        /// A hop that changes the type (e.g. `toString(k)`) changes the value bytes, so drop the
        /// width to unknown. Keep ranges and NULL fractions as diagnostic facts; consumers reject
        /// them through `NDVBoundExpression` when the expression may have changed their meaning.
        if (!output_lineage.input->preserves_width)
            stats.avg_bytes = 0;
        mapped[output.result_name] = stats;
    }
}

}
