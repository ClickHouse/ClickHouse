#include <string_view>
#include <type_traits>
#include <variant>
#include <Processors/QueryPlan/StepStatisticsASCIIPrinter.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>
#include <Common/formatReadable.h>
#include <base/types.h>

namespace DB
{

namespace
{

/// Printed in place of a parallelism or concurrency value when there was no work to measure it on.
constexpr std::string_view unknown_value_text = "Unknown";

String formatStepMetricValue(const StepMetric & metric)
{
    if (std::holds_alternative<std::monostate>(metric.value))
        return String(missingValueText(metric.key));

    if (const auto * fraction = std::get_if<Fraction>(&metric.value))
        return fmt::format("{:.2f}/{:.0f}", fraction->numerator, fraction->denominator);

    const MetricFormat format = formatOf(metric.key);

    if (format == MetricFormat::Raw)
        return std::visit([](const auto & value) -> String
        {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::string>)
                return value;
            else if constexpr (std::is_arithmetic_v<T>)
                return fmt::format("{}", value);
            else
                return {};
        }, metric.value);

    const double numeric = std::visit([](const auto & value) -> double
    {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_arithmetic_v<T>)
            return static_cast<double>(value);
        else
            return 0.0;
    }, metric.value);

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
            return {};
    }
    return {};
}

void printMetricGroup(const MetricGroup & metric_group, WriteBuffer & out, const std::string & prefix)
{
    if (metric_group.metrics.empty())
        return;

    out << prefix << toString(metric_group.key) << ": ";

    bool first = true;
    for (const auto & metric : metric_group.metrics)
    {
        if (!first)
            out << " · ";
        first = false;
        const std::string_view name = toString(metric.key);
        if (name.empty())
            out << formatStepMetricValue(metric);
        else
            out << name << " " << formatStepMetricValue(metric);
    }
    out << "\n";
}

/// The group is built by makeIOGroup, so a missing metric means the two went out of sync
UInt64 getQuantity(const MetricGroup & metric_group, MetricKey key)
{
    const auto quantity = findQuantity(metric_group, key);
    chassert(quantity, "metric is missing from the I/O group");
    return quantity.value_or(0);
}

void printIOGroup(const MetricGroup & io_group, WriteBuffer & out, const std::string & prefix)
{
    const UInt64 input_rows = getQuantity(io_group, MetricKey::InputRows);
    const UInt64 output_rows = getQuantity(io_group, MetricKey::OutputRows);
    const UInt64 input_bytes = getQuantity(io_group, MetricKey::InputBytes);
    const UInt64 output_bytes = getQuantity(io_group, MetricKey::OutputBytes);

    const UInt8 precision_rows_in = input_rows < 1000 ? 0 : 2;
    const UInt8 precision_rows_out = output_rows < 1000 ? 0 : 2;

    out << prefix << "I/O: rows "
        << formatReadableQuantity(static_cast<double>(input_rows), precision_rows_in) << " → "
        << formatReadableQuantity(static_cast<double>(output_rows), precision_rows_out);

    if (input_rows != output_rows && input_rows != 0)
        out << fmt::format(" ({:.2f}%)", 100.0 * static_cast<double>(output_rows) / static_cast<double>(input_rows));

    if (input_bytes != 0 || output_bytes != 0)
    {
        const UInt8 precision_bytes_in = input_bytes < 1000 ? 0 : 2;
        const UInt8 precision_bytes_out = output_bytes < 1000 ? 0 : 2;
        out << " · " << formatReadableSizeWithDecimalSuffix(static_cast<double>(input_bytes), precision_bytes_in)
            << " → " << formatReadableSizeWithDecimalSuffix(static_cast<double>(output_bytes), precision_bytes_out);
    }
    out << "\n";
}

/// A group of placeholders means the work intervals were not collected; such a group is not printed.
bool hasCollectedMetrics(const MetricGroup & group)
{
    for (const auto & metric : group.metrics)
        if (!std::holds_alternative<std::monostate>(metric.value))
            return true;
    return false;
}

/// The group is built by `makeTimingReport`: a (time, share) pair for the step, then one for its branch.
void printTimeGroup(const MetricGroup & time_group, WriteBuffer & out, const std::string & prefix)
{
    if (!hasCollectedMetrics(time_group))
        return;

    chassert(time_group.metrics.size() == 4, "unexpected layout of the Time group");

    auto print_part = [&](std::string_view name, const StepMetric & time, const StepMetric & share)
    {
        out << name << " " << formatStepMetricValue(time);
        if (!std::holds_alternative<std::monostate>(share.value))
            out << " (" << formatStepMetricValue(share) << ")";
    };

    out << prefix << toString(time_group.key) << ": ";
    print_part("step", time_group.metrics[0], time_group.metrics[1]);
    out << " · ";
    print_part("branch", time_group.metrics[2], time_group.metrics[3]);
    out << "\n";
}

/// The group is built by `makeConcurrencyReport`: the step value, then the branch value.
void printConcurrencyGroup(const MetricGroup & concurrency_group, WriteBuffer & out, const std::string & prefix)
{
    /// Empty when work intervals were not collected, see `makeConcurrencyReport`.
    if (concurrency_group.metrics.empty())
        return;

    chassert(concurrency_group.metrics.size() == 2, "unexpected layout of the Concurrency group");

    /// An empty value means the step (or its subtree) did no work, so there is no concurrency to report.
    const auto format_part = [](const StepMetric & metric) -> String
    {
        if (std::holds_alternative<std::monostate>(metric.value))
            return String(unknown_value_text);
        return formatStepMetricValue(metric);
    };

    out << prefix << toString(concurrency_group.key) << ": "
        << "step " << format_part(concurrency_group.metrics[0]) << " · "
        << "branch " << format_part(concurrency_group.metrics[1]) << "\n";
}

void printStage(const AnalyzedStage & stage, bool label_stages, WriteBuffer & out, const std::string & prefix, bool processors_info)
{
    out << prefix << "  ";
    if (label_stages)
    {
        out << "Stage";
        if (!stage.name.empty())
            out << " (" << stage.name << ")";
        out << ": ";
    }
    out << "time " << formatReadableTime(static_cast<double>(stage.wall_clock_time_ns))
        << " (" << formatStepMetricValue({MetricKey::TimeShare, stage.share_of_query_time}) << ")" << " · parallelism "
        << (stage.wall_clock_time_ns ? fmt::format("{:.2f}/{}", stage.parallelism, stage.max_parallelism) : String(unknown_value_text));

    for (const auto & metric : stage.inline_metrics)
        out << " · " << toString(metric.key) << " " << formatStepMetricValue(metric);

    out << "\n";

    if (processors_info)
    {
        out << prefix << "    Time per processor (" << stage.total_num_processors << "): ";
        bool first = true;
        for (const auto & metric : stage.processor_distribution)
        {
            if (!first)
                out << " · ";
            first = false;
            out << toString(metric.key) << " " << formatStepMetricValue(metric);
        }
        out << "\n";
    }
}

}
void StepStatisticsASCIIPrinter::print(const AnalyzedStepData & step_data, WriteBuffer & out, const std::string & prefix, bool processors_info)
{
    for (const auto & group : step_data.step_metric_groups)
    {
        if (group.key == MetricGroupKey::IO)
        {
            printIOGroup(group, out, prefix);
            continue;
        }

        if (group.key == MetricGroupKey::Time)
        {
            printTimeGroup(group, out, prefix);
            continue;
        }

        if (group.key == MetricGroupKey::Concurrency)
        {
            printConcurrencyGroup(group, out, prefix);
            continue;
        }

        printMetricGroup(group, out, prefix);
    }

    for (const auto & stage : step_data.stage_reports)
        printStage(stage, step_data.label_stages, out, prefix, processors_info);
}

}
