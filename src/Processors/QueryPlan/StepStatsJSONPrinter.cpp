#include <Processors/QueryPlan/StepStatsJSONPrinter.h>

#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <base/types.h>

#include <memory>
#include <string_view>
#include <type_traits>
#include <variant>


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
        case MetricGroupKey::Left: return "Left";
        case MetricGroupKey::Right: return "Right";
        case MetricGroupKey::HashTable: return "HashTable";
        case MetricGroupKey::Buffer: return "Buffer";
        case MetricGroupKey::Spill: return "Spill";
        case MetricGroupKey::Build: return "Build";
        case MetricGroupKey::Probe: return "Probe";
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

        case MetricKey::Rows: return "Rows";
        case MetricKey::Matched: return "Matched";
        case MetricKey::MatchRate: return "MatchRate";
        case MetricKey::Fanout: return "Fanout";

        case MetricKey::UniqueKeys: return "UniqueKeys";
        case MetricKey::Memory: return "Memory";
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
        case MetricKey::SortShare: return "SortShare";

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

}

std::unique_ptr<JSONBuilder::JSONMap> StepStatsJSONPrinter::toJSON(const AnalyzedStepData & step_data)
{
    auto map = std::make_unique<JSONBuilder::JSONMap>();

    /// Which groups a step reports depends on what it is: every step reports I/O, only a join
    /// reports its sides, only a hash join its table. Absent groups are left out rather than
    /// written empty.
    for (const auto & group : step_data.step_metric_groups)
        map->add(String(jsonKey(group.key)), metricsToJSON(group.metrics));

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

}
