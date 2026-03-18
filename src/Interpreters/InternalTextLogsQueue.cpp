#include "InternalTextLogsQueue.h"
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>

#include <Poco/Message.h>


namespace DB
{

InternalTextLogsQueue::InternalTextLogsQueue()
        : ConcurrentBoundedQueue<MutableColumns>(std::numeric_limits<int>::max()),
          max_priority(Poco::Message::Priority::PRIO_INFORMATION) {}


Block InternalTextLogsQueue::getSampleBlock()
{
    return Block {
        {std::make_shared<DataTypeDateTime>(), "event_time"},
        {std::make_shared<DataTypeUInt32>(),   "event_time_microseconds"},
        {std::make_shared<DataTypeString>(),   "host_name"},
        {std::make_shared<DataTypeString>(),   "query_id"},
        {std::make_shared<DataTypeUInt64>(),   "thread_id"},
        {std::make_shared<DataTypeInt8>(),     "priority"},
        {std::make_shared<DataTypeString>(),   "source"},
        {std::make_shared<DataTypeString>(),   "text"}
    };
}

MutableColumns InternalTextLogsQueue::getSampleColumns()
{
    static Block sample_block = getSampleBlock();
    return sample_block.cloneEmptyColumns();
}

void InternalTextLogsQueue::pushBlock(Block && log_block)
{
    static Block sample_block = getSampleBlock();

    if (blocksHaveEqualStructure(sample_block, log_block))
        (void)(emplace(log_block.mutateColumns()));
    else
        LOG_WARNING(getLogger("InternalTextLogsQueue"), "Log block have different structure");
}

std::string_view InternalTextLogsQueue::getPriorityName(int priority)
{
    using namespace std::literals;

    /// See Poco::Message::Priority
    static constexpr std::array PRIORITIES =
    {
        "Unknown"sv,
        "Fatal"sv,
        "Critical"sv,
        "Error"sv,
        "Warning"sv,
        "Notice"sv,
        "Information"sv,
        "Debug"sv,
        "Trace"sv,
        "Test"sv,
    };
    return (priority >= 1 && priority < static_cast<int>(PRIORITIES.size())) ? PRIORITIES[priority] : PRIORITIES[0];
}

bool InternalTextLogsQueue::isNeeded(int priority, const String & source) const
{
    bool is_needed = priority <= max_priority;

    if (is_needed && source_regexp)
        is_needed = re2::RE2::PartialMatch(source, *source_regexp);

    return is_needed;
}

void InternalTextLogsQueue::setSourceRegexp(const String & regexp)
{
    source_regexp = std::make_unique<re2::RE2>(regexp);
}

}
