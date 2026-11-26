#include <Processors/QueryPlan/Profiling/Capture/CapturedPlan.h>

#include <base/find_symbols.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/UTF8Helpers.h>
#include <Common/typeid_cast.h>

#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/Profiling/Analysis/AnalyzePlanStats.h>
#include <IO/WriteBufferFromString.h>
#include <base/types.h>

#include <memory>

namespace DB
{

namespace
{

/// Applies `query_masking_rules` to one captured string, the way the `query` column of the same
/// `system.query_log` row is masked. A step renders the literals of the query into its text --
/// filter constants, `IN` lists, output constants -- so without this the plan would show what the
/// `query` column hides.
///
/// Each string is masked on its own, before it becomes a value of the plan document: the keys are
/// added later by the renderer, and a rule never sees a quote or an escape of the `JSON` it ends
/// up in, so it cannot make the document invalid.
void maskSensitiveData(String & text, const SensitiveDataMasker * masker)
{
    if (masker)
        masker->wipeSensitiveData(text);
}

/// Cuts a captured string to `max_bytes`, which is `query_plan_max_step_description_length`. Zero
/// means no limit.
///
/// A step renders its literals into the text it describes itself with, so a query carrying a large
/// `IN` list produces a correspondingly large string, and this one is stored on a
/// `system.query_log` row rather than streamed to the client as `EXPLAIN` does. The capture also
/// runs with the memory tracker blocked, so without the cut the size of a row would answer to
/// nothing.
///
/// The cut is moved back to a UTF-8 boundary: the bytes end up in a `JSON` column, and half of a
/// multi-byte character is not a character.
String truncateCapturedText(std::string_view text, size_t max_bytes)
{
    if (!max_bytes || text.size() <= max_bytes)
        return String(text);

    const auto * begin = reinterpret_cast<const UInt8 *>(text.data());
    const auto * cut = begin + max_bytes;
    UTF8::syncBackward(cut, begin);

    return String(text.substr(0, static_cast<size_t>(cut - begin)));
}

/// The step's own account of itself, one string per line. Like: `Filter column: ...`,
/// `Sort description: ...`, `Limit ...`.
VectorWithMemoryTracking<String> stepDetails(
    const IQueryPlanStep & step,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const PrettyNames * plan_pretty_names,
    const SensitiveDataMasker * masker)
{
    if (!options.actions)
        return {};

    PrettyNames empty_pretty_names;
    WriteBufferFromOwnString out;

    IQueryPlanStep::FormatSettings settings{
        .out = out,
        .header_prefix = "",
        .detail_prefix = "",
        /// Compact and pretty are needed to avoid UNKNOWN_IDENTIFIER throw when using describeActions.
        .compact = true,
        .pretty = true,
        .pretty_names = plan_pretty_names ? plan_pretty_names->pretty_names : empty_pretty_names.pretty_names,
        .runtime_filter_names = plan_pretty_names ? plan_pretty_names->runtime_filter_names : empty_pretty_names.runtime_filter_names};

    step.describeActions(settings);

    /// Masked as a whole, before it is split and cut: a match spanning a line, or one the cut
    /// would split in half, is still masked.
    String text = std::move(out.str());
    maskSensitiveData(text, masker);

    /// Split the resulting lines into a vector of strings, dropping the empty ones.
    VectorWithMemoryTracking<String> details;
    splitInto<'\n'>(details, text, /*token_compress=*/ true);

    for (auto & line : details)
        line = truncateCapturedText(line, max_description_length);

    return details;
}

/// How the step describes itself in one line, trimmed and cut to length.
String stepDescription(
    const IQueryPlanStep & step,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const SensitiveDataMasker * masker)
{
    if (!options.description)
        return {};

    String description = options.pretty
        ? QueryPlanFormat::trimColumnIdentifier(step.getStepDescription())
        : String(step.getStepDescription());

    /// Before the cut, as in `stepDetails`.
    maskSensitiveData(description, masker);

    return truncateCapturedText(description, max_description_length);
}

/// Only a `ReadFromMergeTree` analyses indexes, and the analysis is data it already holds --
/// `describeIndexes` is one rendering of it, this capture is another.
PlanIndexStats stepIndexes(const IQueryPlanStep & step, const ExplainPlanOptions & options, const SensitiveDataMasker * masker)
{
    const auto * read_from_merge_tree = typeid_cast<const ReadFromMergeTree *>(&step);
    if (!options.indexes || !read_from_merge_tree)
        return {};

    /// The condition holds the constants the key was compared with.
    PlanIndexStats indexes = read_from_merge_tree->getIndexStats();
    for (auto & index : indexes)
    {
        maskSensitiveData(index.name, masker);
        maskSensitiveData(index.part_name, masker);
        maskSensitiveData(index.description, masker);
        maskSensitiveData(index.condition, masker);
        for (auto & key : index.used_keys)
            maskSensitiveData(key, masker);

        /// The logged plan does not say which replica analysed what, see `indexStatsToJSON`.
        for (auto & replica : index.distributed)
            replica.address.clear();
    }

    return indexes;
}

/// See `stepIndexes`: the same data, from the same step. A query served from a projection says so
/// here, as `EXPLAIN` does.
PlanProjectionStats stepProjections(const IQueryPlanStep & step, const ExplainPlanOptions & options, const SensitiveDataMasker * masker)
{
    const auto * read_from_merge_tree = typeid_cast<const ReadFromMergeTree *>(&step);
    if (!options.projections || !read_from_merge_tree)
        return {};

    PlanProjectionStats projections = read_from_merge_tree->getProjectionStats();
    for (auto & projection : projections)
    {
        maskSensitiveData(projection.name, masker);
        maskSensitiveData(projection.description, masker);
        maskSensitiveData(projection.condition, masker);
    }

    return projections;
}

/// What the pipeline measured for this step. Absent when there was no pipeline to measure -- a
/// query that failed before finishing is captured without statistics.
std::optional<AnalyzedStepData> stepStatistics(
    const IQueryPlanStep & step, const AnalyzeStepsStats * steps_to_stats)
{
    if (!steps_to_stats)
        return {};

    return steps_to_stats->analyzeStep(&step);
}

/// The names are scoped per plan: a sub-plan is its own naming scope and has its own entry.
/// A miss is not a mistake: `ReadFromMerge` builds its child plans in `initializePipeline`, which
/// runs after `buildPrettyNamesPerPlan`, so a child plan reached here may have no entry.
const PrettyNames * findPrettyNames(const PrettyNamesPerPlan * pretty_names, const QueryPlan * plan)
{
    if (!pretty_names)
        return nullptr;

    auto it = pretty_names->names.find(plan);
    return it == pretty_names->names.end() ? nullptr : &it->second;
}

/// Everything the document needs about one step, read while the step and its pipeline are alive.
/// `children` is left empty: only the walk knows the shape.
CapturedStep captureStep(
    const IQueryPlanStep & step,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const AnalyzeStepsStats * steps_to_stats,
    const PrettyNames * plan_pretty_names,
    const SensitiveDataMasker * masker)
{
    CapturedStep captured;

    captured.id = step.getUniqID();
    captured.type = step.getName();
    captured.description = stepDescription(step, options, max_description_length, masker);
    captured.details = stepDetails(step, options, max_description_length, plan_pretty_names, masker);
    captured.indexes = stepIndexes(step, options, masker);
    captured.projections = stepProjections(step, options, masker);
    captured.statistics = stepStatistics(step, steps_to_stats);

    return captured;
}

}

CapturedPlan CapturedPlan::capture(
    const PlanWalk & walk,
    const ExplainPlanOptions & options,
    size_t max_description_length,
    const AnalyzeStepsStats * steps_to_stats,
    const PrettyNamesPerPlan * pretty_names)
{
    CapturedPlan result;
    if (!walk.plan)
        return result;

    result.root_id = walk.root_id;

    /// Ids and step types are not masked: they hold no literal of the query, and a broad rule
    /// rewriting an id would break the links between nodes.
    const auto masker = SensitiveDataMasker::getInstance();

    result.nodes.reserve(walk.steps.size());
    for (const auto & visited : walk.steps)
    {
        auto captured = captureStep(
            *visited.step, options, max_description_length, steps_to_stats, findPrettyNames(pretty_names, visited.owning_plan), masker.get());
        captured.children = visited.children;
        result.nodes.push_back(std::move(captured));
    }

    if (steps_to_stats)
    {
        result.execution_time_ns = steps_to_stats->getExecutionTimeNs();
        result.max_threads = steps_to_stats->getMaxThreads();
    }

    /// The columns the query produces.
    if (options.pretty)
    {
        const auto * root_pretty_names = findPrettyNames(pretty_names, walk.plan);
        PrettyNames empty_pretty_names;
        const auto & names
            = root_pretty_names ? root_pretty_names->pretty_names : empty_pretty_names.pretty_names;

        const auto & root_step = *walk.plan->getRootNode()->step;
        if (root_step.hasOutputHeader() && root_step.getOutputHeader())
            for (const auto & column : *root_step.getOutputHeader())
            {
                /// A constant column is named by its value.
                auto & name = result.output.emplace_back(QueryPlanFormat::formatColumnPretty(column.name, names));
                maskSensitiveData(name, masker.get());
            }
    }

    return result;
}

}
