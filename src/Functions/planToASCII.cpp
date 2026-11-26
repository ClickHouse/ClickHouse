#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>
#include <Processors/QueryPlan/Profiling/Render/FlatJSONPlanRenderer.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/checkStackSize.h>
#include <Common/formatReadable.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/JSONException.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <string_view>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

using JSONObjectPtr = Poco::JSON::Object::Ptr;
using JSONArrayPtr = Poco::JSON::Array::Ptr;

/// Printed in place of a parallelism or concurrency value when there was no work to measure it on,
/// as `EXPLAIN ANALYZE` does.
constexpr std::string_view unknown_value_text = "Unknown";

/// The groups that `FlatJSONPlanRenderer` writes under `jsonKey`, in the order `EXPLAIN ANALYZE`
/// prints them. `system.query_log` stores the plan in a `JSON` column, which sorts the keys of every
/// object, so the order of the document cannot be relied on. `IO`, `Time` and `Concurrency` are
/// printed by `EXPLAIN ANALYZE` after every other group and have layouts of their own.
constexpr std::array group_keys = {
    MetricGroupKey::Cost,
    MetricGroupKey::Selectivity,
    MetricGroupKey::Output,
    MetricGroupKey::Left,
    MetricGroupKey::Right,
    MetricGroupKey::HashTable,
    MetricGroupKey::Buffer,
    MetricGroupKey::Spill,
    MetricGroupKey::Build,
    MetricGroupKey::RuntimeFilter,
    MetricGroupKey::Probe,
    MetricGroupKey::Memory,
    MetricGroupKey::InputLeft,
    MetricGroupKey::InputRight,
};

/// The metrics of a group, in the order the analyzers report them. A join side reports `QError`
/// right after `Rows` (`prependSideEstimate`), and the join output after `Estimated` and `Actual`.
constexpr std::array metric_keys = {
    MetricKey::Unnamed,
    MetricKey::Estimated,
    MetricKey::Actual,
    MetricKey::EstimatedNDV,
    MetricKey::ActualCartesian,
    MetricKey::RowsEstimated,
    MetricKey::Rows,
    MetricKey::QError,
    MetricKey::Matched,
    MetricKey::MatchRate,
    MetricKey::Fanout,
    MetricKey::FilterType,
    MetricKey::Reason,
    MetricKey::ReplacedFilterType,
    MetricKey::UniqueKeys,
    MetricKey::Memory,
    MetricKey::Bytes,
    MetricKey::Buckets,
    MetricKey::Rehashes,
    MetricKey::Compressed,
    MetricKey::LeftSpilled,
    MetricKey::RightSpilled,
    MetricKey::Spilled,
    MetricKey::Size,
    MetricKey::SetBits,
    MetricKey::RowsChecked,
    MetricKey::RowsPassed,
    MetricKey::PassRate,
    MetricKey::RowsSkipped,
    MetricKey::RowsBeforeReady,
    MetricKey::Blocks,
    MetricKey::Storage,
    MetricKey::SortTime,
    MetricKey::SortTimeShare,
    MetricKey::InputRows,
    MetricKey::OutputRows,
    MetricKey::InputBytes,
    MetricKey::OutputBytes,
    MetricKey::Time,
    MetricKey::TimeShare,
    MetricKey::Concurrency,
};

/// A value as it is quoted in an error message. `Poco::Dynamic::Var::toString` throws on the empty
/// value that a JSON `null` parses to.
String valueText(const Poco::Dynamic::Var & value)
{
    return value.isEmpty() ? "null" : value.toString();
}

/// The document is the argument of the function, so a value that is not a finite number is an error
/// of the argument. A `JSON` column may quote 64-bit integers when it is turned back into text, so a
/// number can also arrive as a string.
double toDouble(const Poco::Dynamic::Var & value, std::string_view key)
{
    double result = 0;
    if (value.isString())
    {
        if (!tryParse(result, value.extract<std::string>()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of `{}` in the query plan is not a number: {}", key, valueText(value));
    }
    else if (!value.isEmpty() && value.isNumeric())
        result = value.convert<double>();
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of `{}` in the query plan is not a number: {}", key, valueText(value));

    if (!std::isfinite(result))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of `{}` in the query plan is not a finite number: {}", key, valueText(value));
    return result;
}

/// Checked rather than `Poco::Dynamic::Var::convert`, which lets a double equal to 2^64 through its
/// range check and then casts it.
UInt64 toUInt64(const Poco::Dynamic::Var & value, std::string_view key)
{
    const auto not_unsigned = [&]
    {
        return Exception(ErrorCodes::BAD_ARGUMENTS,
            "The value of `{}` in the query plan is not a non-negative integer that fits `UInt64`: {}", key, valueText(value));
    };

    if (value.isEmpty())
        throw not_unsigned();
    if (value.isString())
    {
        UInt64 result = 0;
        if (!tryParse(result, value.extract<std::string>()))
            throw not_unsigned();
        return result;
    }
    if (value.isInteger())
    {
        if (value.isSigned())
        {
            const Int64 signed_value = value.convert<Int64>();
            if (signed_value < 0)
                throw not_unsigned();
            return static_cast<UInt64>(signed_value);
        }
        return value.convert<UInt64>();
    }
    if (value.isNumeric())
    {
        /// 2^64 is exactly representable as a double, unlike the maximum of `UInt64`.
        constexpr double two_to_64 = 18446744073709551616.0;
        const double number = value.convert<double>();
        if (!std::isfinite(number) || number < 0 || number >= two_to_64 || number != std::floor(number))
            throw not_unsigned();
        return static_cast<UInt64>(number);
    }
    throw not_unsigned();
}

std::optional<double> findNumber(const Poco::JSON::Object & object, const std::string & key)
{
    if (!object.has(key) || object.isNull(key))
        return std::nullopt;
    return toDouble(object.get(key), key);
}

double numberOrZero(const Poco::JSON::Object & object, const std::string & key)
{
    return findNumber(object, key).value_or(0.0);
}

String stringOrEmpty(const Poco::JSON::Object & object, const std::string & key)
{
    if (!object.has(key) || object.isNull(key))
        return {};
    return object.get(key).toString();
}

/// Mirrors `formatStepMetricValue` of `EXPLAIN ANALYZE`, reading the value from the document.
/// A metric the step did not collect is written as null, or left out entirely by a `JSON` column.
String formatMetricValue(MetricKey key, const Poco::JSON::Object & group, const std::string & json_key)
{
    if (!group.has(json_key) || group.isNull(json_key))
        return String(missingValueText(key));

    if (JSONObjectPtr fraction = group.getObject(json_key))
        return fmt::format("{:.2f}/{:.0f}", numberOrZero(*fraction, "Numerator"), numberOrZero(*fraction, "Denominator"));

    const auto & value = group.get(json_key);
    const MetricFormat format = formatOf(key);

    if (format == MetricFormat::Raw || format == MetricFormat::Fraction)
        return value.toString();

    const double numeric = toDouble(value, json_key);
    switch (format)
    {
        case MetricFormat::Bytes:
            return formatReadableSizeWithDecimalSuffix(numeric);
        case MetricFormat::Quantity:
            return formatReadableQuantity(numeric);
        case MetricFormat::Time:
            return formatReadableTime(numeric);
        case MetricFormat::Percent:
            return fmt::format("{:.2f}%", numeric);
        case MetricFormat::Ratio:
            return fmt::format("{:.2f}", numeric);
        case MetricFormat::Selectivity:
            return fmt::format("{:.4g}", numeric);
        case MetricFormat::Fraction:
        case MetricFormat::Raw:
            return value.toString();
    }
    return value.toString();
}

/// The metrics of a group, as `name value` parts in the order the step reports them, followed by
/// any keys this function does not know, under their own names.
VectorWithMemoryTracking<String> formatMetrics(const Poco::JSON::Object & group)
{
    VectorWithMemoryTracking<String> parts;
    UnorderedSetWithMemoryTracking<std::string_view> known;

    for (const MetricKey key : metric_keys)
    {
        const std::string_view json_key = jsonKey(key);
        known.insert(json_key);
        const std::string key_string(json_key);
        if (!group.has(key_string))
            continue;

        const std::string_view name = toString(key);
        String value = formatMetricValue(key, group, key_string);
        parts.push_back(name.empty() ? std::move(value) : fmt::format("{} {}", name, value));
    }

    for (const auto & [json_key, value] : group)
        if (!known.contains(json_key))
            parts.push_back(fmt::format("{} {}", json_key, value.isEmpty() ? "null" : value.toString()));

    return parts;
}

void writeJoined(WriteBuffer & out, const VectorWithMemoryTracking<String> & parts)
{
    bool first = true;
    for (const auto & part : parts)
    {
        if (!first)
            out << " · ";
        first = false;
        out << part;
    }
}

/// Renders one plan of the document in the layout of `EXPLAIN ANALYZE`.
class PlanRenderer
{
public:
    PlanRenderer(const Poco::JSON::Object & plan_, WriteBuffer & out_)
        : plan(plan_)
        , out(out_)
    {
        execution_time_ns = findNumber(plan, "ExecutionTimeNs");
        if (plan.has("MaxThreads") && !plan.isNull("MaxThreads"))
            max_threads = toUInt64(plan.get("MaxThreads"), "MaxThreads");

        JSONArrayPtr nodes = plan.getArray("Nodes");
        if (!nodes)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The query plan has no `Nodes` array");

        for (size_t i = 0; i < nodes->size(); ++i)
        {
            JSONObjectPtr node = nodes->getObject(static_cast<unsigned>(i));
            if (!node)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Element {} of `Nodes` of the query plan is not an object", i);
            const String id = stringOrEmpty(*node, "NodeId");
            if (!nodes_by_id.emplace(id, node).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The query plan has more than one node with id '{}'", id);
        }
    }

    void render()
    {
        if (!plan.has("Root") || plan.isNull("Root"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The query plan has no `Root`");

        if (JSONArrayPtr output = plan.getArray("Output"); output && output->size() != 0)
        {
            out << "Output: ";
            for (size_t i = 0; i < output->size(); ++i)
            {
                if (i)
                    out << ", ";
                out << output->get(static_cast<unsigned>(i)).toString();
            }
            out << "\n\n";
        }

        renderNode(plan.get("Root").toString(), "", /*is_root=*/ true, /*is_last=*/ true);
    }

private:
    const Poco::JSON::Object & plan;
    WriteBuffer & out;

    std::optional<double> execution_time_ns;
    std::optional<UInt64> max_threads;

    UnorderedMapWithMemoryTracking<String, JSONObjectPtr> nodes_by_id;
    UnorderedSetWithMemoryTracking<String> visited;

    [[nodiscard]] const Poco::JSON::Object & findNode(const String & id) const
    {
        auto it = nodes_by_id.find(id);
        if (it == nodes_by_id.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The query plan refers to node '{}', which is not in `Nodes`", id);
        return *it->second;
    }

    /// `parent_prefix` is what every line of this node and its subtree starts with: one segment for
    /// each ancestor below the root, a bar where that ancestor has siblings still to come below it.
    void renderNode(const String & id, const String & parent_prefix, bool is_root, bool is_last)
    {
        checkStackSize();

        if (!visited.insert(id).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Node '{}' appears more than once in the query plan tree", id);

        const auto & node = findNode(id);

        VectorWithMemoryTracking<String> children;
        if (JSONArrayPtr children_array = node.getArray("Children"))
            for (size_t i = 0; i < children_array->size(); ++i)
                children.push_back(children_array->get(static_cast<unsigned>(i)).toString());

        const char * continuation = is_last ? "   " : "│  ";
        String header_prefix = is_root ? parent_prefix : parent_prefix + (is_last ? "└──" : "├──");
        String detail_prefix = (is_root ? parent_prefix : parent_prefix + continuation) + (children.empty() ? "   " : "│  ");

        out << header_prefix << stringOrEmpty(node, "NodeType");
        if (String description = stringOrEmpty(node, "Description"); !description.empty())
            out << " (" << description << ")";
        out << "\n";

        JSONObjectPtr statistics = node.getObject("Statistics");

        renderDetails(node, statistics, detail_prefix);
        renderIndexes(node, detail_prefix);
        renderProjections(node, detail_prefix);
        if (statistics)
            renderStatistics(*statistics, detail_prefix);

        const String children_prefix = is_root ? parent_prefix : parent_prefix + continuation;
        for (size_t i = 0; i < children.size(); ++i)
            renderNode(children[i], children_prefix, /*is_root=*/ false, /*is_last=*/ i + 1 == children.size());
    }

    /// A join writes its estimates among its details, as `Cost: estimated ...`, and its statistics
    /// again under the same group. `EXPLAIN ANALYZE` prints the group in place of the detail, so a
    /// detail line that a group of the statistics replaces is left out.
    void renderDetails(const Poco::JSON::Object & node, const JSONObjectPtr & statistics, const String & prefix)
    {
        JSONArrayPtr details = node.getArray("Details");
        if (!details)
            return;

        VectorWithMemoryTracking<String> replaced_prefixes;
        if (statistics)
            for (const MetricGroupKey key : group_keys)
                if (statistics->has(std::string(jsonKey(key))))
                    replaced_prefixes.push_back(fmt::format("{}: ", toString(key)));

        for (size_t i = 0; i < details->size(); ++i)
        {
            const String line = details->get(static_cast<unsigned>(i)).toString();
            const bool replaced = std::ranges::any_of(replaced_prefixes, [&](const String & p) { return line.starts_with(p); });
            if (!replaced)
                out << prefix << line << "\n";
        }
    }

    /// Mirrors `ReadFromMergeTree::describeIndexes`. The number of selected ranges is not in the
    /// document, so the `Ranges:` line is not printed.
    void renderIndexes(const Poco::JSON::Object & node, const String & prefix)
    {
        JSONArrayPtr indexes = node.getArray("Indexes");
        if (!indexes || indexes->size() == 0)
            return;

        const String indent = "  ";
        out << prefix << "Indexes:\n";

        for (size_t i = 0; i < indexes->size(); ++i)
        {
            JSONObjectPtr index = indexes->getObject(static_cast<unsigned>(i));
            if (!index)
                continue;

            out << prefix << indent << stringOrEmpty(*index, "Type") << "\n";

            if (String name = stringOrEmpty(*index, "Name"); !name.empty())
                out << prefix << indent << indent << "Name: " << name << "\n";
            if (String description = stringOrEmpty(*index, "Description"); !description.empty())
                out << prefix << indent << indent << "Description: " << description << "\n";

            if (JSONArrayPtr keys = index->getArray("Keys"); keys && keys->size() != 0)
            {
                out << prefix << indent << indent << "Keys:\n";
                for (size_t k = 0; k < keys->size(); ++k)
                    out << prefix << indent << indent << indent << keys->get(static_cast<unsigned>(k)).toString() << "\n";
            }

            if (String condition = stringOrEmpty(*index, "Condition"); !condition.empty())
                out << prefix << indent << indent << "Condition: " << condition << "\n";

            const auto print_selected = [&](std::string_view title, const std::string & selected_key, const std::string & initial_key)
            {
                if (!index->has(selected_key))
                    return;
                out << prefix << indent << indent << title << ": " << stringOrEmpty(*index, selected_key);
                if (index->has(initial_key))
                    out << "/" << stringOrEmpty(*index, initial_key);
                out << "\n";
            };
            print_selected("Parts", "Selected Parts", "Initial Parts");
            print_selected("Granules", "Selected Granules", "Initial Granules");

            if (String algorithm = stringOrEmpty(*index, "Search Algorithm"); !algorithm.empty())
                out << prefix << indent << indent << "Search Algorithm: " << algorithm << "\n";

            if (JSONArrayPtr distributed = index->getArray("Distributed"); distributed && distributed->size() != 0)
            {
                out << prefix << indent << indent << "Distributed:\n";
                for (size_t n = 0; n < distributed->size(); ++n)
                {
                    JSONObjectPtr replica = distributed->getObject(static_cast<unsigned>(n));
                    if (!replica)
                        continue;
                    for (const char * key : {"Address", "Parts send", "Parts received", "Granules send", "Granules received"})
                        if (replica->has(key))
                            out << prefix << indent << indent << indent << key << ": " << stringOrEmpty(*replica, key) << "\n";
                }
            }
        }
    }

    /// Mirrors `ReadFromMergeTree::describeProjections`, which `EXPLAIN ANALYZE` prints with
    /// `projections = 1`. The document always holds them, so they are always printed.
    void renderProjections(const Poco::JSON::Object & node, const String & prefix)
    {
        JSONArrayPtr projections = node.getArray("Projections");
        if (!projections || projections->size() == 0)
            return;

        const String indent = "  ";
        out << prefix << "Projections:\n";

        for (size_t i = 0; i < projections->size(); ++i)
        {
            JSONObjectPtr projection = projections->getObject(static_cast<unsigned>(i));
            if (!projection)
                continue;

            out << prefix << indent << "Name: " << stringOrEmpty(*projection, "Name") << "\n";

            for (const char * key : {"Description", "Condition", "Search Algorithm"})
                if (String value = stringOrEmpty(*projection, key); !value.empty())
                    out << prefix << indent << indent << key << ": " << value << "\n";

            const std::array<std::pair<const char *, const char *>, 5> counts = {{
                {"Parts", "Selected Parts"},
                {"Marks", "Selected Marks"},
                {"Ranges", "Selected Ranges"},
                {"Rows", "Selected Rows"},
                {"Filtered Parts", "Filtered Parts"},
            }};
            for (const auto & [title, key] : counts)
                if (projection->has(key))
                    out << prefix << indent << indent << title << ": " << stringOrEmpty(*projection, key) << "\n";
        }
    }

    void renderStatistics(const Poco::JSON::Object & statistics, const String & prefix)
    {
        UnorderedSetWithMemoryTracking<std::string_view> known = {"IO", "Time", "Concurrency", "Stages"};

        for (const MetricGroupKey key : group_keys)
        {
            const std::string_view json_key = jsonKey(key);
            known.insert(json_key);
            if (JSONObjectPtr group = statistics.getObject(std::string(json_key)))
            {
                if (key == MetricGroupKey::RuntimeFilter)
                    renderRuntimeFilter(*group, prefix);
                else
                    renderGroup(toString(key), *group, prefix);
            }
        }

        /// A group added after this function was written: print it under its key rather than drop it.
        for (const auto & [json_key, value] : statistics)
            if (!known.contains(json_key))
                if (JSONObjectPtr group = statistics.getObject(json_key))
                    renderGroup(json_key, *group, prefix);

        if (JSONObjectPtr io = statistics.getObject("IO"))
            renderIO(*io, prefix);
        if (JSONObjectPtr time = statistics.getObject("Time"))
            renderTime(*time, prefix);
        if (JSONObjectPtr concurrency = statistics.getObject("Concurrency"))
            renderConcurrency(*concurrency, prefix);

        if (JSONArrayPtr stages = statistics.getArray("Stages"))
            renderStages(*stages, prefix);
    }

    void renderGroup(std::string_view name, const Poco::JSON::Object & group, const String & prefix)
    {
        const auto parts = formatMetrics(group);
        if (parts.empty())
            return;

        out << prefix << name << ": ";
        writeJoined(out, parts);
        out << "\n";
    }

    /// Mirrors `printRuntimeFilterGroup`: the type, then the reason if the filter is disabled, then the
    /// optional details, e.g. `Runtime filter (disabled: bloom filter saturated, size 524.29 KB, set bits 0.07%)`.
    void renderRuntimeFilter(const Poco::JSON::Object & group, const String & prefix)
    {
        if (!group.has("Type"))
            return;

        out << prefix << toString(MetricGroupKey::RuntimeFilter) << " (" << formatMetricValue(MetricKey::FilterType, group, "Type");
        if (group.has("Reason"))
            out << ": " << formatMetricValue(MetricKey::Reason, group, "Reason");
        if (group.has("Replaced"))
            out << ", replaced " << formatMetricValue(MetricKey::ReplacedFilterType, group, "Replaced");
        /// Not rounded, to compare it with `join_runtime_filter_exact_values_limit`.
        if (group.has("UniqueKeys") && !group.isNull("UniqueKeys"))
            out << ", " << toUInt64(group.get("UniqueKeys"), "UniqueKeys") << " " << toString(MetricKey::UniqueKeys);

        UnorderedSetWithMemoryTracking<std::string_view> known = {"Type", "Reason", "Replaced", "UniqueKeys"};
        for (const MetricKey key : metric_keys)
        {
            const std::string_view json_key = jsonKey(key);
            known.insert(json_key);
            const std::string key_string(json_key);
            if (key == MetricKey::FilterType || key == MetricKey::Reason || key == MetricKey::ReplacedFilterType
                || key == MetricKey::UniqueKeys || !group.has(key_string))
                continue;
            out << ", " << toString(key) << " " << formatMetricValue(key, group, key_string);
        }
        for (const auto & [json_key, value] : group)
            if (!known.contains(json_key))
                out << ", " << json_key << " " << (value.isEmpty() ? "null" : value.toString());
        out << ")\n";
    }

    /// Mirrors `printIOGroup`.
    void renderIO(const Poco::JSON::Object & io, const String & prefix)
    {
        const double input_rows = numberOrZero(io, "InputRows");
        const double output_rows = numberOrZero(io, "OutputRows");
        const double input_bytes = numberOrZero(io, "InputBytes");
        const double output_bytes = numberOrZero(io, "OutputBytes");

        const auto precision = [](double value) -> UInt8 { return value < 1000 ? 0 : 2; };

        out << prefix << "I/O: rows "
            << formatReadableQuantity(input_rows, precision(input_rows)) << " → "
            << formatReadableQuantity(output_rows, precision(output_rows));

        if (input_rows != output_rows && input_rows != 0)
            out << fmt::format(" ({:.2f}%)", 100.0 * output_rows / input_rows);

        if (input_bytes != 0 || output_bytes != 0)
            out << " · " << formatReadableSizeWithDecimalSuffix(input_bytes, precision(input_bytes))
                << " → " << formatReadableSizeWithDecimalSuffix(output_bytes, precision(output_bytes));

        out << "\n";
    }

    /// Mirrors `printTimeGroup`.
    void renderTime(const Poco::JSON::Object & time, const String & prefix)
    {
        const auto print_part = [&](std::string_view name, const std::string & time_key, const std::string & share_key)
        {
            out << name << " " << formatMetricValue(MetricKey::Time, time, time_key);
            if (time.has(share_key) && !time.isNull(share_key))
                out << " (" << formatMetricValue(MetricKey::TimeShare, time, share_key) << ")";
        };

        out << prefix << "Time: ";
        print_part("step", "StepTimeNs", "StepTimeShare");
        out << " · ";
        print_part("branch", "BranchTimeNs", "BranchTimeShare");
        out << "\n";
    }

    /// Mirrors `printConcurrencyGroup`.
    void renderConcurrency(const Poco::JSON::Object & concurrency, const String & prefix)
    {
        const auto format_part = [&](const std::string & key) -> String
        {
            if (!concurrency.has(key) || concurrency.isNull(key))
                return String(unknown_value_text);
            return formatMetricValue(MetricKey::Concurrency, concurrency, key);
        };

        out << prefix << "Concurrency: step " << format_part("Step") << " · branch " << format_part("Branch") << "\n";
    }

    /// Mirrors `printStage`, recomputing the share of the query's time and the parallelism of the
    /// stage the way `buildAnalyzedStages` does, from the values the document keeps.
    void renderStages(const Poco::JSON::Array & stages, const String & prefix)
    {
        bool label_stages = stages.size() > 1;
        if (stages.size() == 1)
            if (JSONObjectPtr only = stages.getObject(0))
                label_stages = !stringOrEmpty(*only, "Name").empty();

        for (size_t i = 0; i < stages.size(); ++i)
        {
            JSONObjectPtr stage = stages.getObject(static_cast<unsigned>(i));
            if (!stage)
                continue;

            out << prefix << "  ";
            if (label_stages)
            {
                out << "Stage";
                if (String name = stringOrEmpty(*stage, "Name"); !name.empty())
                    out << " (" << name << ")";
                out << ": ";
            }

            const double wall_clock_time_ns = numberOrZero(*stage, "WallClockTimeNs");
            out << "time " << formatReadableTime(wall_clock_time_ns);

            if (execution_time_ns)
            {
                const double share = *execution_time_ns != 0 ? 100.0 * wall_clock_time_ns / *execution_time_ns : 0.0;
                out << fmt::format(" ({:.2f}%)", share);
            }

            out << " · parallelism ";
            if (wall_clock_time_ns != 0)
            {
                double sum_elapsed_ns = 0;
                if (JSONObjectPtr distribution = stage->getObject("ProcessorTimeNs"))
                    sum_elapsed_ns = numberOrZero(*distribution, "Sum");

                const UInt64 processors = stage->has("Processors") && !stage->isNull("Processors") ? toUInt64(stage->get("Processors"), "Processors") : 0;
                const UInt64 max_parallelism = max_threads ? std::min(*max_threads, processors) : processors;
                out << fmt::format("{:.2f}/{}", sum_elapsed_ns / wall_clock_time_ns, max_parallelism);
            }
            else
                out << unknown_value_text;

            if (JSONObjectPtr metrics = stage->getObject("Metrics"))
                for (const auto & part : formatMetrics(*metrics))
                    out << " · " << part;

            out << "\n";
        }
    }
};

void renderDocument(const Poco::JSON::Object & document, WriteBuffer & out)
{
    /// Written by `FlatJSONPlanRenderer::renderError` in place of a plan that could not be logged.
    if (document.has("Error") && !document.has("Version"))
    {
        out << "Error: " << stringOrEmpty(document, "Error") << "\n";
        return;
    }

    if (!document.has("Version"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The query plan has no `Version`");

    /// `system.query_log` keeps the rows of older versions until their TTL, and a query over the logs of
    /// several servers can meet a newer version during a rolling upgrade. One such row must not fail the
    /// whole query, so a version this server does not read renders as an error of its own row. For the
    /// same reason, a later version of this function has to keep reading every version written before.
    const UInt64 version = toUInt64(document.get("Version"), "Version");
    if (version != QUERY_PLAN_JSON_VERSION)
    {
        out << "Error: unsupported query plan version " << version << ", this server reads version " << QUERY_PLAN_JSON_VERSION << "\n";
        return;
    }

    PlanRenderer(document, out).render();
}

/** planToASCII(query_plan) - renders the `query_plan` column of `system.query_log` as the text tree
  * that `EXPLAIN ANALYZE` prints. Best effort: it shows what the document holds, so the query
  * summary header, which needs values that are not in the plan, is not printed.
  */
class FunctionPlanToASCII final : public IFunction
{
public:
    static constexpr auto name = "planToASCII";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPlanToASCII>(); }

    [[nodiscard]] String getName() const override { return name; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 1; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

    /// The default implementation would apply the function to the default value of the argument,
    /// which renders to an empty string anyway.
    [[nodiscard]] bool canBeExecutedOnDefaultArguments() const override { return false; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"query_plan",
             static_cast<FunctionArgumentDescriptor::TypeValidator>([](const IDataType & type) { return isString(type) || isObject(type); }),
             nullptr,
             "String or JSON"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args);
        return std::make_shared<DataTypeString>();
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & argument = arguments[0];
        const bool is_string = isString(argument.type);
        const auto serialization = argument.type->getDefaultSerialization();
        const FormatSettings format_settings;

        auto result = ColumnString::create();
        result->reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            String document;
            if (is_string)
                document = String(argument.column->getDataAt(row));
            else
            {
                WriteBufferFromString buf(document);
                serialization->serializeText(*argument.column, row, buf, format_settings);
                buf.finalize();
            }

            const String rendered = render(document);
            result->insertData(rendered.data(), rendered.size());
        }

        return result;
    }

private:
    static String render(const String & document)
    {
        /// A `QueryStart` row, or one of a query run without `log_query_plans`, has no plan.
        if (document.empty())
            return {};

        JSONObjectPtr object;
        try
        {
            Poco::JSON::Parser parser;
            object = parser.parse(document).extract<JSONObjectPtr>();
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse the query plan as a JSON object: {}", e.displayText());
        }
        catch (const std::exception & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse the query plan as a JSON object: {}", e.what());
        }

        if (!object)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The query plan is not a JSON object");

        if (object->size() == 0)
            return {};

        WriteBufferFromOwnString out;
        try
        {
            renderDocument(*object, out);
        }
        catch (const Exception &)
        {
            /// `DB::Exception` derives from `Poco::Exception`: let the errors of the renderer through as they are.
            throw;
        }
        catch (const Poco::Exception & e)
        {
            /// A value of an unexpected type, such as an object where a number was expected.
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot render the query plan: {}", e.displayText());
        }

        /// Every line ends with a newline; the last one is dropped, as in the output of `EXPLAIN`, so
        /// that a row-per-line format does not add an empty line after the tree.
        String rendered = out.str();
        if (rendered.ends_with('\n'))
            rendered.pop_back();
        return rendered;
    }
};

}

REGISTER_FUNCTION(PlanToASCII)
{
    FunctionDocumentation::Description description = R"(
Renders a query plan, as written to the `query_plan` column of [`system.query_log`](/reference/system-tables/query_log) when the [`log_query_plans`](/reference/settings/session-settings/log-query#log_query_plans) setting is enabled, as the text tree that `EXPLAIN ANALYZE` prints.

The rendering is best effort: it shows the steps, their details, indexes, projections and per-step statistics that the plan holds. Projections are always shown, as `EXPLAIN ANALYZE projections = 1` does. The query summary that `EXPLAIN ANALYZE` prints above the tree is not shown, because the planning time, the rows and bytes read and the peak memory usage are not part of the plan. They are in other columns of `system.query_log`.

An empty plan, such as the one of a `QueryStart` row, renders as an empty string. A plan that could not be logged renders as `Error: ` followed by the reason, and so does a plan of a version that this server does not read, so that one such row does not fail a query over many rows.
    )";
    FunctionDocumentation::Syntax syntax = "planToASCII(query_plan)";
    FunctionDocumentation::Arguments arguments = {
        {"query_plan", "The query plan as JSON, as stored in the `query_plan` column of `system.query_log`.", {"JSON", "String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The query plan as a text tree.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Render the plan of the last query that logged one",
            R"(
SELECT count() FROM numbers(1000) WHERE number % 3 = 0 SETTINGS log_query_plans = 1;
SYSTEM FLUSH LOGS query_log;

SELECT planToASCII(query_plan)
FROM system.query_log
WHERE type = 'QueryFinish' AND query_plan::String != '{}'
ORDER BY event_time_microseconds DESC
LIMIT 1
FORMAT TSVRaw
            )",
            R"DOCS_MD(
Output: count()

Expression ((Project names + Projection))
│  I/O: rows 1 → 1 · 8 B → 8 B
│    time 1.15 us (0.27%) · parallelism 0.95/1
└──Aggregating
   │  Keys:
   │  Aggregates: count()
   │  Skip merging: 0
   │  Memory: bytes 4.10 KB
   │  I/O: rows 334 → 1 (0.30%) · 0 B → 8 B
   │    Stage (partial aggregation): time 48.86 us (11.48%) · parallelism 1.00/1
   │    Stage (final aggregation): time 6.38 us (1.50%) · parallelism 0.98/1
   └──Expression (Before GROUP BY)
      │  I/O: rows 334 → 334
      │    time 1.09 us (0.26%) · parallelism 0.95/1
      └──Filter ((WHERE + Change column names to column identifiers))
         │  Filter column: number MOD 3 = 0
         │  I/O: rows 1.00 thousand → 334 (33.40%) · 8.00 KB → 0 B
         │    time 13.05 us (3.07%) · parallelism 0.99/1
         └──ReadFromSystemNumbers
               Output: number
               I/O: rows 0 → 1.00 thousand · 0 B → 8.00 KB
                 time 17.52 us (4.12%) · parallelism 0.99/1
            )DOCS_MD"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPlanToASCII>(documentation);
}

}
