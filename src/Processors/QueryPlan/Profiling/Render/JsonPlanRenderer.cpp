#include <Processors/QueryPlan/Profiling/Render/JsonPlanRenderer.h>

#include <Common/JSONBuilder.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Processors/QueryPlan/Profiling/Capture/PlanIndexStats.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>
#include <base/types.h>

#include <memory>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

namespace DB
{

namespace
{

/// Keys of the serialized form. Deliberately not `toString`, which returns display names such as
/// `I/O` and `input rows`: a key holding a space or a slash has to be backtick-quoted in a JSON
/// path, which would make the stored statistics awkward to query.
std::string_view jsonKey(MetricGroupKey key)
{
    switch (key)
    {
        case MetricGroupKey::IO: return "IO";
        case MetricGroupKey::Time: return "Time";
        case MetricGroupKey::Concurrency: return "Concurrency";
        case MetricGroupKey::Left: return "Left";
        case MetricGroupKey::Right: return "Right";
        case MetricGroupKey::HashTable: return "HashTable";
        case MetricGroupKey::Memory: return "Memory";
        case MetricGroupKey::Buffer: return "Buffer";
        case MetricGroupKey::Spill: return "Spill";
        case MetricGroupKey::Build: return "Build";
        case MetricGroupKey::Probe: return "Probe";
        case MetricGroupKey::Cost: return "Cost";
        case MetricGroupKey::Selectivity: return "Selectivity";
        case MetricGroupKey::Output: return "Output";
        case MetricGroupKey::InputLeft: return "InputLeft";
        case MetricGroupKey::InputRight: return "InputRight";
    }
}

std::string_view jsonKey(MetricKey key)
{
    switch (key)
    {
        /// A metric the step reports without a name of its own, being the only one in its group.
        /// The text form prints it bare; JSON has nowhere to put a nameless value.
        case MetricKey::Unnamed: return "Value";

        case MetricKey::InputRows: return "InputRows";
        case MetricKey::OutputRows: return "OutputRows";
        case MetricKey::InputBytes: return "InputBytes";
        case MetricKey::OutputBytes: return "OutputBytes";

        case MetricKey::Time: return "Time";
        case MetricKey::TimeShare: return "TimeShare";

        case MetricKey::Concurrency: return "Concurrency";

        case MetricKey::Rows: return "Rows";
        case MetricKey::RowsEstimated: return "RowsEstimated";
        case MetricKey::Matched: return "Matched";
        case MetricKey::MatchRate: return "MatchRate";
        case MetricKey::Fanout: return "Fanout";

        /// Estimated against actual, as the join-order optimizer's cost model saw it.
        case MetricKey::Estimated: return "Estimated";
        case MetricKey::Actual: return "Actual";
        case MetricKey::EstimatedNDV: return "EstimatedNDV";
        case MetricKey::ActualCartesian: return "ActualCartesian";
        case MetricKey::QError: return "QError";

        case MetricKey::UniqueKeys: return "UniqueKeys";
        case MetricKey::Memory: return "Memory";
        case MetricKey::Bytes: return "Bytes";
        case MetricKey::Buckets: return "Buckets";
        case MetricKey::Rehashes: return "Rehashes";

        case MetricKey::LeftSpilled: return "LeftSpilled";
        case MetricKey::RightSpilled: return "RightSpilled";
        case MetricKey::Spilled: return "Spilled";
        case MetricKey::Compressed: return "Compressed";

        case MetricKey::Size: return "Size";
        case MetricKey::Blocks: return "Blocks";
        case MetricKey::Storage: return "Storage";

        case MetricKey::SortTime: return "SortTime";
        case MetricKey::SortTimeShare: return "SortShare";

        case MetricKey::Min: return "Min";
        case MetricKey::Median: return "Median";
        case MetricKey::Max: return "Max";
        case MetricKey::Sum: return "Sum";
    }
}

/// A metric the step did not collect is written as null rather than as zero, which would read as a
/// measurement that was taken and came out empty. The text form prints `not collected` for it.
JSONBuilder::ItemPtr metricValueToJSON(const MetricValue & value)
{
    return std::visit([](const auto & concrete) -> JSONBuilder::ItemPtr
    {
        using T = std::decay_t<decltype(concrete)>;

        if constexpr (std::is_same_v<T, std::monostate>)
            return std::make_unique<JSONBuilder::JSONNull>();
        else if constexpr (std::is_same_v<T, std::string>)
            return std::make_unique<JSONBuilder::JSONString>(concrete);
        else if constexpr (std::is_same_v<T, Fraction>)
        {
            auto fraction = std::make_unique<JSONBuilder::JSONMap>();
            fraction->add("Numerator", concrete.numerator);
            fraction->add("Denominator", concrete.denominator);
            return fraction;
        }
        else
            return std::make_unique<JSONBuilder::JSONNumber<T>>(concrete);
    }, value);
}

std::unique_ptr<JSONBuilder::JSONMap> metricsToJSON(const MetricList & metrics)
{
    auto map = std::make_unique<JSONBuilder::JSONMap>();

    for (const auto & metric : metrics)
        map->add(String(jsonKey(metric.key)), metricValueToJSON(metric.value));

    return map;
}

/// The Time and Concurrency groups hold one value for the step and then one for its branch, under
/// repeating metric keys; a JSON object cannot repeat a key, so they are written with their own layout.
/// All values are empty when the work intervals were not collected; such a group is left out.
std::unique_ptr<JSONBuilder::JSONMap> stepAndBranchGroupToJSON(const MetricGroup & group)
{
    if (!hasCollectedMetrics(group))
        return nullptr;

    auto map = std::make_unique<JSONBuilder::JSONMap>();
    if (group.key == MetricGroupKey::Time)
    {
        chassert(group.metrics.size() == 4, "unexpected layout of the Time group");
        map->add("StepTimeNs", metricValueToJSON(group.metrics[0].value));
        map->add("StepTimeShare", metricValueToJSON(group.metrics[1].value));
        map->add("BranchTimeNs", metricValueToJSON(group.metrics[2].value));
        map->add("BranchTimeShare", metricValueToJSON(group.metrics[3].value));
    }
    else
    {
        chassert(group.metrics.size() == 2, "unexpected layout of the Concurrency group");
        map->add("Step", metricValueToJSON(group.metrics[0].value));
        map->add("Branch", metricValueToJSON(group.metrics[1].value));
    }
    return map;
}

/// The `Statistics` of one step. Raw values throughout: the column is meant to be aggregated by
/// a query, which formatted ones would prevent.
std::unique_ptr<JSONBuilder::JSONMap> stepStatisticsToJSON(const AnalyzedStepData & step_data)
{
    auto map = std::make_unique<JSONBuilder::JSONMap>();

    /// Which groups a step reports depends on what it is: every step reports I/O, only a join
    /// reports its sides, only a hash join its table. Absent groups are left out rather than
    /// written empty.
    for (const auto & group : step_data.step_metric_groups)
    {
        if (group.key == MetricGroupKey::Time || group.key == MetricGroupKey::Concurrency)
        {
            if (auto group_map = stepAndBranchGroupToJSON(group))
                map->add(String(jsonKey(group.key)), std::move(group_map));
            continue;
        }
        map->add(String(jsonKey(group.key)), metricsToJSON(group.metrics));
    }

    /// Always present, so that a query over the column can index into it without checking. A step
    /// whose processors never ran reports no stages.
    auto stages = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & stage : step_data.stage_reports)
    {
        auto stage_map = std::make_unique<JSONBuilder::JSONMap>();

        /// Empty for a step with a single unnamed stage, which is most of them. `label_stages` is
        /// not carried over: it only tells the text form whether to print a `Stage (name):` prefix.
        stage_map->add("Name", stage.name);
        stage_map->add("GroupId", stage.group_id);
        stage_map->add("WallClockTimeNs", stage.wall_clock_time_ns);
        stage_map->add("Processors", stage.total_num_processors);

        if (!stage.inline_metrics.empty())
            stage_map->add("Metrics", metricsToJSON(stage.inline_metrics));

        /// The distribution of elapsed time over the processors of this stage: min, median, max, sum.
        if (!stage.processor_distribution.empty())
            stage_map->add("ProcessorTimeNs", metricsToJSON(stage.processor_distribution));

        stages->add(std::move(stage_map));
    }

    map->add("Stages", std::move(stages));

    return map;
}

/// One node of the flat `Nodes` array.
JSONBuilder::ItemPtr capturedStepToJSON(const CapturedStep & step)
{
    auto map = std::make_unique<JSONBuilder::JSONMap>();

    map->add("Node Type", step.type);
    map->add("Node Id", step.id);

    if (!step.description.empty())
        map->add("Description", step.description);

    auto details = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & line : step.details)
        details->add(line);
    map->add("Details", std::move(details));

    if (step.statistics)
        map->add("Statistics", stepStatisticsToJSON(*step.statistics));

    if (auto indexes = indexStatsToJSON(step.indexes))
        map->add("Indexes", std::move(indexes));
    if (auto projections = projectionStatsToJSON(step.projections))
        map->add("Projections", std::move(projections));

    auto children = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & child : step.children)
        children->add(child);
    map->add("Children", std::move(children));

    return map;
}

JSONBuilder::ItemPtr capturedPlanToJSON(const CapturedPlan & captured)
{
    auto nodes_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & node : captured.nodes)
        nodes_array->add(capturedStepToJSON(node));

    auto result = std::make_unique<JSONBuilder::JSONMap>();
    result->add("Version", QUERY_PLAN_JSON_VERSION);
    result->add("Root", captured.root_id);

    if (captured.execution_time_ns)
        result->add("ExecutionTimeNs", *captured.execution_time_ns);
    if (captured.max_threads)
        result->add("MaxThreads", *captured.max_threads);

    auto output_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : captured.output)
        output_array->add(column);
    result->add("Output", std::move(output_array));

    result->add("Nodes", std::move(nodes_array));

    return result;
}

}

namespace
{

JSONBuilder::FormatSettings jsonFormatSettings(FormatSettings & format_settings)
{
    /// The consumer of this column is a program, and a 64-bit integer quoted as a string is one
    /// more thing for it to undo before it can aggregate.
    format_settings.json.quote_64bit_integers = false;
    return JSONBuilder::FormatSettings{.settings = format_settings};
}

}

void JsonPlanRenderer::renderError(const String & message, WriteBuffer & out)
{
    JSONBuilder::JSONMap error_map;
    error_map.add("Error", message);

    FormatSettings format_settings;
    auto json_format_settings = jsonFormatSettings(format_settings);
    JSONBuilder::FormatContext format_context{.out = out};
    error_map.format(json_format_settings, format_context);
}

void JsonPlanRenderer::render(const CapturedPlan & captured, WriteBuffer & out)
{
    FormatSettings format_settings;
    auto json_format_settings = jsonFormatSettings(format_settings);
    JSONBuilder::FormatContext format_context{.out = out};
    capturedPlanToJSON(captured)->format(json_format_settings, format_context);
}

}
