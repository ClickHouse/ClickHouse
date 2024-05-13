#include "ProgressTable.h"

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Interpreters/ProfileEventsExt.h>
#include <base/terminalColors.h>
#include <Common/ProfileEvents.h>
#include <Common/TerminalSize.h>
#include <Common/formatReadable.h>

#include <format>

namespace DB
{

namespace
{

constexpr UInt64 THREAD_GROUP_ID = 0;

constexpr std::string_view CLEAR_TO_END_OF_LINE = "\033[K";
constexpr std::string_view CLEAR_TO_END_OF_SCREEN = "\033[0J";
constexpr std::string_view RESET_COLOR = "\033[0m";

std::string MoveUpNLines(size_t N)
{
    return std::format("\033[{}A", N);
}

std::string formatReadableValue(ProfileEvents::ValueType value_type, double value)
{
    switch (value_type)
    {
        case ProfileEvents::ValueType::NUMBER:
            return formatReadableQuantity(value);
        case ProfileEvents::ValueType::BYTES:
            return formatReadableSizeWithDecimalSuffix(value);
        case ProfileEvents::ValueType::NANOSECONDS:
            return formatReadableTime(value);
        case ProfileEvents::ValueType::MICROSECONDS:
            return formatReadableTime(value * 1e3);
        case ProfileEvents::ValueType::MILLISECONDS:
            return formatReadableTime(value * 1e6);
    }
}

auto createEventToValueTypeMap()
{
    std::unordered_map<std::string_view, ProfileEvents::ValueType> event_to_value_type;
    for (ProfileEvents::Event event = ProfileEvents::Event(0); event < ProfileEvents::end(); ++event)
    {
        std::string_view name = ProfileEvents::getName(event);
        ProfileEvents::ValueType value_type = getValueType(event);
        event_to_value_type[name] = value_type;
    }
    event_to_value_type[MemoryTracker::USAGE_EVENT_NAME] = ProfileEvents::ValueType::BYTES;
    return event_to_value_type;
}

std::string_view setColorForProgress(double progress, double max_progress)
{
    static const std::string_view colors[] = {
        "",
        "\033[38;5;34m", /// Green
        "\033[38;5;154m", /// Yellow-green
        "\033[38;5;220m", /// Yellow
        "\033[38;5;214m", /// Yellow-orange
        "\033[38;5;208m", /// Orange
        "\033[38;5;202m", /// Orange-red
        "\033[38;5;160m", /// Red
    };

    static const double fractions[] = {
        0.0,
        0.2,
        0.4,
        0.5,
        0.7,
        0.9,
        1.0,
    };

    if (max_progress == 0)
        return colors[7];

    const double fraction = progress / max_progress;
    for (size_t i = 0; i < 7; ++i)
        if (fraction <= fractions[i])
            return colors[i];
    return colors[7];
}

template <typename Out>
void writeWithWidth(Out & out, std::string_view s, size_t width)
{
    if (s.size() >= width)
        out << s << " ";
    else
        out << s << std::string(width - s.size(), ' ');
}

}

const std::unordered_map<std::string_view, ProfileEvents::ValueType> ProgressTable::event_to_value_type = createEventToValueTypeMap();

void ProgressTable::writeTable(WriteBufferFromFileDescriptor & message)
{
    std::lock_guard lock{mutex};

    size_t terminal_width = getTerminalWidth();
    if (terminal_width < column_event_name_width + COLUMN_VALUE_WIDTH + COLUMN_PROGRESS_WIDTH)
        return;

    if (metrics.empty())
        return;

    message << "\n";
    writeWithWidth(message, COLUMN_EVENT_NAME, column_event_name_width);
    writeWithWidth(message, COLUMN_VALUE, COLUMN_VALUE_WIDTH);
    writeWithWidth(message, COLUMN_PROGRESS, COLUMN_PROGRESS_WIDTH);
    message << CLEAR_TO_END_OF_LINE;

    double elapsed_sec = watch.elapsedSeconds();

    for (auto & [name, per_host_info] : metrics)
    {
        message << "\n";
        writeWithWidth(message, name, column_event_name_width);

        auto value = per_host_info.getSummaryValue();
        auto value_type = event_to_value_type.at(name);
        writeWithWidth(message, formatReadableValue(value_type, value), COLUMN_VALUE_WIDTH);

        /// Get the maximum progress before it is updated in getSummaryProgress.
        auto max_progress = per_host_info.getMaxProgress();
        auto progress = per_host_info.getSummaryProgress(elapsed_sec);
        message << setColorForProgress(progress, max_progress);

        writeWithWidth(message, formatReadableValue(value_type, progress) + " / s", COLUMN_PROGRESS_WIDTH);
        message << RESET_COLOR;
        message << CLEAR_TO_END_OF_LINE;
    }

    message << MoveUpNLines(tableSize());
    message.next();
}

void ProgressTable::writeFinalTable()
{
    std::lock_guard lock{mutex};

    size_t terminal_width = getTerminalWidth();
    if (terminal_width < column_event_name_width + COLUMN_VALUE_WIDTH)
        return;

    if (metrics.empty())
        return;

    std::cout << "\n";
    writeWithWidth(std::cout, COLUMN_EVENT_NAME, column_event_name_width);
    writeWithWidth(std::cout, COLUMN_VALUE, COLUMN_VALUE_WIDTH);

    for (auto & [name, per_host_info] : metrics)
    {
        std::cout << "\n";
        writeWithWidth(std::cout, name, column_event_name_width);

        auto value = per_host_info.getSummaryValue();
        auto value_type = event_to_value_type.at(name);
        writeWithWidth(std::cout, formatReadableValue(value_type, value), COLUMN_VALUE_WIDTH);
    }
}

void ProgressTable::updateTable(const Block & block)
{
    const auto & array_thread_id = typeid_cast<const ColumnUInt64 &>(*block.getByName("thread_id").column).getData();
    const auto & names = typeid_cast<const ColumnString &>(*block.getByName("name").column);
    const auto & host_names = typeid_cast<const ColumnString &>(*block.getByName("host_name").column);
    const auto & array_values = typeid_cast<const ColumnInt64 &>(*block.getByName("value").column).getData();
    const auto & array_type = typeid_cast<const ColumnInt8 &>(*block.getByName("type").column).getData();

    const double time_now = watch.elapsedSeconds();
    size_t max_event_name_width = COLUMN_EVENT_NAME.size();

    std::lock_guard lock{mutex};
    for (size_t row_num = 0, rows = block.rows(); row_num < rows; ++row_num)
    {
        auto thread_id = array_thread_id[row_num];

        /// In ProfileEvents packets thread id 0 specifies common profiling information
        /// for all threads executing current query on specific host. So instead of summing per thread
        /// consumption it's enough to look for data with thread id 0.
        if (thread_id != THREAD_GROUP_ID)
            continue;

        auto value = array_values[row_num];
        auto name = names.getDataAt(row_num).toString();
        auto host_name = host_names.getDataAt(row_num).toString();
        auto type = static_cast<ProfileEvents::Type>(array_type[row_num]);

        /// Got unexpected event name.
        if (!event_to_value_type.contains(name))
            continue;

        /// Store non-zero values.
        if (value == 0)
            continue;

        auto it = metrics.find(name);

        /// If the table has already been written, then do not add new metrics to avoid jitter.
        if (it == metrics.end() && written_first_block)
            continue;

        if (!written_first_block)
            it = metrics.try_emplace(name).first;

        it->second.updateHostValue(host_name, type, value, time_now);

        max_event_name_width = std::max(max_event_name_width, name.size());
    }

    if (!written_first_block)
        column_event_name_width = max_event_name_width + 1;

    written_first_block = true;
}

void ProgressTable::clearTableOutput(WriteBufferFromFileDescriptor & message)
{
    message << CLEAR_TO_END_OF_SCREEN;
    message.next();
}

void ProgressTable::resetTable()
{
    std::lock_guard lock{mutex};
    watch.restart();
    metrics.clear();
    written_first_block = false;
}

size_t ProgressTable::tableSize() const
{
    /// Number of lines + header.
    return metrics.empty() ? 0 : metrics.size() + 1;
}

ProgressTable::MetricInfo::MetricInfo(ProfileEvents::Type t) : type(t)
{
}

void ProgressTable::MetricInfo::updateValue(Int64 new_value, double new_time)
{
    /// If the value has not been updated for a long time
    /// reset the time in snapshots.
    if (new_time - new_snapshot.time >= 0.5 || new_snapshot.time == 0)
    {
        prev_shapshot = {new_snapshot.value, new_time - 2.0};
        cur_shapshot = {new_snapshot.value, new_time - 1.0};
    }

    switch (type)
    {
        case ProfileEvents::Type::INCREMENT:
            new_snapshot.value = new_snapshot.value + new_value;
            break;
        case ProfileEvents::Type::GAUGE:
            new_snapshot.value = new_value;
            break;
    }
    new_snapshot.time = new_time;

    if (new_snapshot.time - cur_shapshot.time >= 0.5)
        prev_shapshot = std::exchange(cur_shapshot, new_snapshot);
}

double ProgressTable::MetricInfo::calculateProgress(double time_now) const
{
    /// If the value has not been updated for a long time, the progress is 0.
    if (time_now - new_snapshot.time >= 0.5)
        return 0;

    switch (type)
    {
        case ProfileEvents::Type::INCREMENT:
            return (cur_shapshot.value - prev_shapshot.value) / (cur_shapshot.time - prev_shapshot.time);
        case ProfileEvents::Type::GAUGE:
            return cur_shapshot.value - prev_shapshot.value;
    }
}

double ProgressTable::MetricInfo::getValue() const
{
    return new_snapshot.value;
}

void ProgressTable::MetricInfoPerHost::updateHostValue(const HostName & host, ProfileEvents::Type type, Int64 new_value, double new_time)
{
    auto it = host_to_metric.find(host);
    if (it == host_to_metric.end())
        it = host_to_metric.emplace(host, type).first;
    it->second.updateValue(new_value, new_time);
}

double ProgressTable::MetricInfoPerHost::getSummaryValue()
{
    return std::accumulate(
        host_to_metric.cbegin(),
        host_to_metric.cend(),
        0.0,
        [](double acc, const auto & host_data)
        {
            const MetricInfo & info = host_data.second;
            return acc + info.getValue();
        });
}

double ProgressTable::MetricInfoPerHost::getSummaryProgress(double time_now)
{
    auto progress = std::accumulate(
        host_to_metric.cbegin(),
        host_to_metric.cend(),
        0.0,
        [time_now](double acc, const auto & host_data)
        {
            const MetricInfo & info = host_data.second;
            return acc + info.calculateProgress(time_now);
        });
    max_progress = std::max(max_progress, progress);
    return progress;
}

double ProgressTable::MetricInfoPerHost::getMaxProgress() const
{
    return max_progress;
}

}
