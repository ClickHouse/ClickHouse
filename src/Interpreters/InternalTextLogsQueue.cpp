#include <Interpreters/InternalTextLogsQueue.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/DNSResolver.h>
#include <Common/logger_useful.h>

#include <Poco/Message.h>

#include <chrono>


namespace DB
{

InternalTextLogsQueue::InternalTextLogsQueue(size_t max_entries_)
        : ConcurrentBoundedQueue<MutableColumns>(max_entries_),
          max_priority(Poco::Message::Priority::PRIO_INFORMATION) {}

void InternalTextLogsQueue::pushOrDrop(MutableColumns && columns)
{
    /// The unbounded queue never fills, so take the plain push with a zero-timeout
    if (maxFill() == UNBOUNDED) [[likely]]
    {
        [[maybe_unused]] bool pushed = emplace(std::move(columns));
        return;
    }

    if (!tryEmplace(/*milliseconds=*/ 0, std::move(columns)))
        dropped_logs.fetch_add(1, std::memory_order_relaxed);
}


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

void InternalTextLogsQueue::pushMessage(int priority, std::string_view source, const String & query_id, const String & text)
{
    MutableColumns columns = getSampleColumns();
    const auto now = std::chrono::system_clock::now().time_since_epoch();

    size_t i = 0;
    columns[i++]->insert(static_cast<UInt64>(std::chrono::duration_cast<std::chrono::seconds>(now).count()));
    columns[i++]->insert(static_cast<UInt64>(std::chrono::duration_cast<std::chrono::microseconds>(now).count() % 1000000));
    columns[i++]->insert(DNSResolver::instance().getHostName());
    columns[i++]->insert(query_id);
    columns[i++]->insert(static_cast<UInt64>(0)); /// thread_id
    columns[i++]->insert(static_cast<Int64>(priority));
    columns[i++]->insert(String(source));
    columns[i++]->insert(text);

    pushOrDrop(std::move(columns));
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
