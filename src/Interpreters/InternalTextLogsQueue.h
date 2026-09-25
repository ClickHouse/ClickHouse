#pragma once
#include <Core/Block.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/re2.h>
#include <base/types.h>

#include <atomic>
#include <limits>

namespace DB
{

class InternalTextLogsQueue : public ConcurrentBoundedQueue<MutableColumns>
{
public:
    /// You should not push logs in the queue if their priority greater max_priority
    int max_priority;

    /// Log lines dropped because the queue was full, read to report best-effort forwarding loss to
    /// the coordinator. Stays zero at the default capacity, which is never reached in practice.
    std::atomic<UInt64> dropped_logs{0};

    /// The default capacity: never reached in practice, so the queue behaves as unbounded.
    static constexpr size_t UNBOUNDED = std::numeric_limits<int>::max();

    /// `max_entries` bounds the queue where a consumer drains it only periodically (worker log
    /// forwarding).
    explicit InternalTextLogsQueue(size_t max_entries_ = UNBOUNDED);

    /// Enqueue a log-line batch. A bounded queue drops the batch and counts it in `dropped_logs`
    /// when full, so the logging thread is never stalled by a slow consumer.
    void pushOrDrop(MutableColumns && columns);

    bool isNeeded(int priority, const String & source) const;

    static Block getSampleBlock();
    static MutableColumns getSampleColumns();

    /// Is used to pass block from remote server to the client
    void pushBlock(Block && log_block);

    /// Build and enqueue a single synthetic log line (current wall-clock time, this host's name), to
    /// inject a message into a client's `send_logs_level` stream from code that is not itself attached
    /// to the queue.
    void pushMessage(int priority, std::string_view source, const String & query_id, const String & text);

    /// Converts priority from Poco::Message::Priority to a string
    static std::string_view getPriorityName(int priority);

    void setSourceRegexp(const String & regexp);
private:
    /// If not null, you should only push logs which are matched with this regexp
    std::unique_ptr<re2::RE2> source_regexp;
};

using InternalTextLogsQueuePtr = std::shared_ptr<InternalTextLogsQueue>;

}


