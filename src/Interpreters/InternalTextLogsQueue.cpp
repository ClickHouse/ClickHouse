#include "InternalTextLogsQueue.h"
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>
#include <Common/OvercommitTracker.h>

#include <Poco/Message.h>


namespace DB
{

InternalTextLogsQueue::InternalTextLogsQueue()
    : queue(std::numeric_limits<int>::max())
    , max_priority(Poco::Message::Priority::PRIO_INFORMATION)
{}


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
        emplace(log_block.mutateColumns());
    else
        LOG_WARNING(&Poco::Logger::get("InternalTextLogsQueue"), "Log block have different structure");
}

const char * InternalTextLogsQueue::getPriorityName(int priority)
{
    /// See Poco::Message::Priority

    static constexpr const char * const PRIORITIES[] =
    {
        "Unknown",
        "Fatal",
        "Critical",
        "Error",
        "Warning",
        "Notice",
        "Information",
        "Debug",
        "Trace"
    };

    return (priority >= 1 && priority <= 8) ? PRIORITIES[priority] : PRIORITIES[0];
}

void InternalTextLogsQueue::emplace(MutableColumns && columns)
{
    /// Even though OvercommitTracker doing logging under
    /// OvercommitTrackerBlockerInThread it is not enough to avoid deadlocks.
    ///
    /// Consider the following callchain:
    ///
    ///     MemoryTracker
    ///       OvercommitTracker
    ///         LOG_*()
    ///           InternalTextLogsQueue::emplace()
    ///             MemoryTracker
    ///
    /// And this this acquires the following locks:
    ///
    ///     OvercommitTracker::global_mutex (ProcessList::mutex)
    ///       OvercommitTracker::overcommit_m
    ///         InternalTextLogsQueue::queue::queue_mutex
    ///
    /// This is safe, because OvercommitTracker doing logging under
    /// OvercommitTrackerBlockerInThread.
    ///
    /// However if you have the following callchain:
    ///
    ///     RemoteQueryExecutor::processPacket()
    ///       InternalTextLogsQueue::pushBlock()
    ///         InternalTextLogsQueue::emplace()
    ///           MemoryTracker
    ///
    /// It is not safe, since the lock-order-inversion is possible and deadlock
    /// will happen:
    ///
    ///     InternalTextLogsQueue::queue::queue_mutex
    ///       OvercommitTracker::global_mutex (ProcessList::mutex)
    ///         OvercommitTracker::overcommit_m
    OvercommitTrackerBlockerInThread blocker;
    (void)queue.emplace(std::move(columns));
}

bool InternalTextLogsQueue::tryPop(MutableColumns & columns)
{
    /// If you have the following callchain:
    ///
    ///     TCPHandler::sendLogs()
    ///       InternalTextLogsQueue::tryPop()
    ///         MemoryTracker
    ///
    /// It is not safe, since the lock-order-inversion is possible and deadlock
    /// will happen:
    ///
    ///     InternalTextLogsQueue::queue::queue_mutex
    ///       OvercommitTracker::global_mutex (ProcessList::mutex)
    ///         OvercommitTracker::overcommit_m
    ///
    /// See also comments above in InternalTextLogsQueue::emplace().
    OvercommitTrackerBlockerInThread blocker;
    return queue.tryPop(columns);
}

}
