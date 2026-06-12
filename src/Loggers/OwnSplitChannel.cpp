#include <Columns/IColumn.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/TextLog.h>
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnSplitChannel.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <Common/IO.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/setThreadName.h>

#include <Poco/Message.h>

#include <base/sleep.h>

#if defined(MEMORY_SANITIZER)
#include <sanitizer/msan_interface.h>
#endif


namespace ProfileEvents
{
extern const Event AsyncLoggingTextLogDroppedMessages;
extern const Event AsyncLoggingTextLogTotalMessages;
}

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

void OwnSplitChannel::open()
{
    stop_logging = false;
}

void OwnSplitChannel::close()
{
    stop_logging = true;
}

void OwnSplitChannel::log(const Poco::Message & msg)
{
    log(Poco::Message(msg));
}

void OwnSplitChannel::log(Poco::Message && msg)
{
#if defined(MEMORY_SANITIZER)
    {
        auto fmt = msg.getFormatString();
        __msan_check_mem_is_initialized(&fmt, sizeof(fmt));
        if (fmt.data())
            __msan_check_mem_is_initialized(fmt.data(), fmt.size());
    }
#endif
    if (stop_logging)
        return;

    const auto & logs_queue = CurrentThread::getInternalTextLogsQueue();
    if (channels.empty() && (logs_queue == nullptr && !logs_queue->isNeeded(msg.getPriority(), msg.getSource())))
        return;

    if (const auto & masker = SensitiveDataMasker::getInstance())
    {
        auto message_text = msg.getText();
        auto matches = masker->wipeSensitiveDataThrow(message_text);
        if (matches > 0)
        {
            msg.setText(message_text);
            logSplit(ExtendedLogMessage::getFrom(msg), logs_queue, getThreadName());
            return;
        }
    }

    logSplit(ExtendedLogMessage::getFrom(msg), logs_queue, getThreadName());
}

namespace
{

/// How long the consumer threads sleep when their queue is empty before checking it again.
/// The queues serve the logging subsystem, which is rarely silent for long, so we expect them to be
/// non-empty most of the time. Therefore, instead of futex-based waiting (which would also make pushing
/// into an empty queue more expensive for producers), the consumers just poll the queues, sleeping
/// in between when there is nothing to do.
constexpr size_t sleep_on_empty_queue_ms = 10;

/// Dequeues a single message for a flush. Returns an empty notification if the queue is empty.
/// If the message at the head of the queue is still being written by a producer, waits for it to appear:
/// a message whose enqueue had completed must not be skipped by the flush (it could be hidden behind
/// such an unfinished slot).
AsyncLogMessagePtr dequeueMessageForFlush(NonblockingBoundedQueue<AsyncLogMessagePtr> & messages)
{
    AsyncLogMessagePtr message;
    while (!messages.tryPop(message))
    {
        if (messages.size() == 0)
            return nullptr;
        sleepForMilliseconds(1);
    }
    return message;
}

void pushExtendedMessageToInternalTCPTextLogQueue(
    const ExtendedLogMessage & msg_ext, const std::shared_ptr<InternalTextLogsQueue> & logs_queue)
{
    const Poco::Message & msg = *msg_ext.base;
    MutableColumns columns = InternalTextLogsQueue::getSampleColumns();

    size_t i = 0;
    columns[i++]->insert(msg_ext.time_seconds);
    columns[i++]->insert(msg_ext.time_microseconds);
    columns[i++]->insert(DNSResolver::instance().getHostName());
    columns[i++]->insert(msg_ext.query_id);
    columns[i++]->insert(msg_ext.thread_id);
    columns[i++]->insert(static_cast<Int64>(msg.getPriority()));
    columns[i++]->insert(msg.getSource());
    columns[i++]->insert(msg.getText());

    [[maybe_unused]] bool push_result = logs_queue->emplace(std::move(columns));
}

void logToSystemTextLogQueue(
    const std::shared_ptr<SystemLogQueue<TextLogElement>> & text_log_locked,
    const ExtendedLogMessage & msg_ext,
    ThreadName msg_thread_name)
{
    const Poco::Message & msg = *msg_ext.base;
    TextLogElement elem;

    elem.event_time = msg_ext.time_seconds;
    elem.event_time_microseconds = msg_ext.time_in_microseconds;

    elem.thread_name = msg_thread_name;
    elem.thread_id = msg_ext.thread_id;

    elem.query_id = msg_ext.query_id;

    elem.message = msg.getText();
    elem.logger_name = msg.getSource();
    elem.level = msg.getPriority();
    elem.source_file = msg.getSourceFile();

    elem.source_line = msg.getSourceLine();
    elem.message_format_string = msg.getFormatString();

#define SET_VALUE_IF_EXISTS(INDEX) \
    if ((INDEX) <= msg.getFormatStringArgs().size()) \
        (elem.value##INDEX) = msg.getFormatStringArgs()[(INDEX) - 1]

    SET_VALUE_IF_EXISTS(1);
    SET_VALUE_IF_EXISTS(2);
    SET_VALUE_IF_EXISTS(3);
    SET_VALUE_IF_EXISTS(4);
    SET_VALUE_IF_EXISTS(5);
    SET_VALUE_IF_EXISTS(6);
    SET_VALUE_IF_EXISTS(7);
    SET_VALUE_IF_EXISTS(8);
    SET_VALUE_IF_EXISTS(9);
    SET_VALUE_IF_EXISTS(10);

#undef SET_VALUE_IF_EXISTS

    text_log_locked->push(std::move(elem));
}
}

void OwnSplitChannel::logSplit(
    const ExtendedLogMessage & msg_ext, const std::shared_ptr<InternalTextLogsQueue> & logs_queue, ThreadName msg_thread_name)
{
    const Poco::Message & msg = *msg_ext.base;

    try
    {
        /// Log data to child channels
        for (auto & channel : channels | std::views::values)
        {
            auto priority = channel->getPriority();
            if (priority >= msg.getPriority())
                channel->logExtended(msg_ext);
        }

        /// Log to "TCP queue" if message is not too noisy
        if (logs_queue && logs_queue->isNeeded(msg.getPriority(), msg.getSource()))
            pushExtendedMessageToInternalTCPTextLogQueue(msg_ext, logs_queue);

        auto text_log_locked = text_log.lock();
        if (!text_log_locked)
            return;

        /// Also log to system.text_log table, if message is not too noisy
        auto text_log_max_priority_loaded = text_log_max_priority.load(std::memory_order_relaxed);
        if (text_log_max_priority_loaded && msg.getPriority() <= text_log_max_priority_loaded)
        {
            logToSystemTextLogQueue(text_log_locked, msg_ext, msg_thread_name);
        }
    }
    /// It is better to catch the errors here in order to avoid
    /// breaking some functionality because of unexpected "File not
    /// found" (or similar) error.
    ///
    /// For example DistributedAsyncInsertDirectoryQueue will mark batch
    /// as broken, some MergeTree code can also be affected.
    ///
    /// Also note, that we cannot log the exception here, since this
    /// will lead to recursion, using regular tryLogCurrentException().
    /// but let's log it into the stderr at least.
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);
        const std::string & message = msg.getText();

        /// NOTE: errors are ignored, since nothing can be done.
        writeRetry(STDERR_FILENO, "Cannot add message to the log: ");
        writeRetry(STDERR_FILENO, message.data(), message.size());
        writeRetry(STDERR_FILENO, "\n");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}


void OwnSplitChannel::addChannel(
    ChannelPtr channel, const std::string & name, int level, const ProfileEvents::Event &, const ProfileEvents::Event &)
{
    channel->setLevel(level);
    channels.emplace(name, channel);
}

void OwnSplitChannel::addTextLog(std::shared_ptr<SystemLogQueue<TextLogElement>> log_queue, int max_priority)
{
    text_log = log_queue;
    text_log_max_priority.store(max_priority, std::memory_order_relaxed);
}

void OwnSplitChannel::setLevel(const std::string & name, int level)
{
     auto it = channels.find(name);
     if (it != channels.end())
         it->second->setLevel(level);
}

void OwnSplitChannel::setChannelProperty(const std::string& channel_name, const std::string& name, const std::string& value)
{
    auto it = channels.find(channel_name);
    if (it != channels.end())
        it->second->setProperty(name, value);
}

OwnAsyncSplitChannel::OwnAsyncSplitChannel(size_t async_queue_size_)
    : async_queue_size(async_queue_size_)
    , text_log_queue(async_queue_size_, ProfileEvents::AsyncLoggingTextLogTotalMessages, ProfileEvents::AsyncLoggingTextLogDroppedMessages)
{
    if (async_queue_size_ == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Asynchronous log message queue cannot have zero size");
}

OwnAsyncSplitChannel::~OwnAsyncSplitChannel()
{
    OwnAsyncSplitChannel::close();
}

void OwnAsyncSplitChannel::open()
{
    is_open = true;
    if (text_log_max_priority && !text_log_thread)
    {
        text_log_thread = std::make_unique<Poco::Thread>("AsyncTextLog");
        text_log_thread->start(*text_log_runnable);
    }

    for (size_t i = 0; i < channels.size(); i++)
    {
        if (!threads[i])
        {
            threads[i] = std::make_unique<Poco::Thread>("AsyncLog");
            threads[i]->start(*runnables[i]);
        }
    }
}

void OwnAsyncSplitChannel::close()
{
    is_open = false;
    try
    {
        /// The consumer threads poll the queues (sleeping when there is nothing to do), so they will notice
        /// that the channel is closed on their own, flush whatever is left in the queues and exit.
        if (text_log_thread)
        {
            text_log_thread->join();
            text_log_thread.reset();
        }

        for (size_t i = 0; i < channels.size(); i++)
        {
            if (threads[i])
                threads[i]->join();
            threads[i].reset();
        }
    }
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);
        writeRetry(STDERR_FILENO, "Cannot close OwnAsyncSplitChannel: ");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}

class AsyncLogMessage
{
public:
    ALWAYS_INLINE explicit AsyncLogMessage(Message && msg_)
        : msg(std::move(msg_))
        , msg_ext(ExtendedLogMessage::getFrom(msg))
        , msg_thread_name(getThreadName())
    {
        if (const auto & masker = SensitiveDataMasker::getInstance())
        {
            auto message_text = msg.getText();
            auto matches = masker->wipeSensitiveDataThrow(message_text);
            if (matches > 0)
                msg.setText(message_text);
        }
    }

    Message msg; /// Need to keep a copy until we finish logging
    ExtendedLogMessage msg_ext;
    ThreadName msg_thread_name;
};


namespace
{

/// Builds the warning message about previously dropped messages, to be reported in the channel which dropped them.
AsyncLogMessagePtr makeDroppedMessagesWarning(size_t dropped)
{
    auto warning = std::make_shared<AsyncLogMessage>(Poco::Message(
        "AsyncLogger",
        fmt::format("We've dropped {} log messages in this channel due to queue overflow", dropped),
        Poco::Message::PRIO_WARNING));
    warning->msg_ext.query_id.clear();
    return warning;
}

}

void OwnAsyncSplitChannel::enqueueMessage(LogQueue & queue, AsyncLogMessagePtr message)
{
    ProfileEvents::incrementNoTrace(queue.event_on_passed_message);

    if (unlikely(!queue.messages.tryPush(std::move(message))))
    {
        /// The queue is full: the consumer doesn't keep up. Drop the message and remember the fact;
        /// the consumer will report the dropped messages with a warning once it catches up.
        queue.dropped_messages.fetch_add(1, std::memory_order_relaxed);
        ProfileEvents::incrementNoTrace(queue.event_on_dropped_message);
    }
}

void OwnAsyncSplitChannel::log(const Poco::Message & msg)
{
    log(Poco::Message(msg));
}

void OwnAsyncSplitChannel::log(Poco::Message && msg)
{
    try
    {
#if defined(MEMORY_SANITIZER)
        /// Catch which LOG call produces a message with uninitialized format string bytes.
        /// STID 1478-2063: arm_msan stress test reports use-of-uninitialized-value in TextLog
        {
            auto fmt = msg.getFormatString();
            __msan_check_mem_is_initialized(&fmt, sizeof(fmt));
            if (fmt.data())
                __msan_check_mem_is_initialized(fmt.data(), fmt.size());
        }
#endif
        /// Based on logger_useful.h this won't be called if the message is not needed
        /// so we can create the AsyncLogMessage as it won't penalize performance by being unused
        auto msg_priority = msg.getPriority();
        auto notification = std::make_shared<AsyncLogMessage>(std::move(msg));
        if (const auto & logs_queue = CurrentThread::getInternalTextLogsQueue();
            logs_queue && logs_queue->isNeeded(msg_priority, notification->msg.getSource()))
        {
            /// If we need to push to the TCP queue, do it now since it expects to receive all messages synchronously
            pushExtendedMessageToInternalTCPTextLogQueue(notification->msg_ext, logs_queue);
        }

        auto text_log_max_priority_loaded = text_log_max_priority.load(std::memory_order_relaxed);
        if (channels.empty() && !text_log_max_priority_loaded)
            return;

        for (size_t i = 0; i < queues.size(); i++)
        {
            if (channels[i]->getPriority() >= msg_priority)
                enqueueMessage(*queues[i], notification);
        }

        if (text_log_max_priority_loaded >= msg_priority)
            enqueueMessage(text_log_queue, std::move(notification));
    }
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);

        /// NOTE: errors are ignored, since nothing can be done.
        writeRetry(STDERR_FILENO, "Failed to add message to the log queue: ");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}

void OwnAsyncSplitChannel::flushTextLogs()
{
    auto text_log_locked = text_log.lock();
    if (!text_log_locked)
        return;

    /// If there is a query flushing already we must wait until it's done. Otherwise we will receive the notification to wake up
    /// once the previous flush is finished, which is not what we need
    /// This is not ideal and we could use some kind of flush id to wait only until the point when you entered this function
    /// But notice that even if you call in many threads, they will all wait and be processed together in the same block once this is unlocked
    text_log_flush_requested.wait(true, std::memory_order_seq_cst);

    /// The async thread checks the flag between messages and when it wakes up from the sleep on an empty queue,
    /// so it will notice the request on its own
    text_log_flush_requested = true;

    /// Now we simply wait for the async thread to notify it has finished flushing
    text_log_flush_requested.wait(true, std::memory_order_seq_cst);
}

AsyncLogQueueSizes OwnAsyncSplitChannel::getAsynchronousMetrics()
{
    AsyncLogQueueSizes metrics;
    for (const auto & [name, channel] : name_to_channels)
    {
        for (size_t i = 0; i < channels.size(); i++)
        {
            if (channels[i] == channel.get())
            {
                metrics.push_back({name, queues[i]->messages.size()});
                break;
            }
        }
    }

    if (text_log.lock())
        metrics.push_back({"TextLog", text_log_queue.messages.size()});

    return metrics;
}

void OwnAsyncSplitChannel::runChannel(size_t i)
{
    DB::setThreadName(ThreadName::ASYNC_LOGGER);
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    LogQueue & queue = *queues[i];
    const auto & extended_channel = channels[i];

    auto report_dropped_messages = [&]()
    {
        if (likely(queue.dropped_messages.load(std::memory_order_relaxed) == 0))
            return;

        size_t dropped = queue.dropped_messages.exchange(0, std::memory_order_relaxed);
        try
        {
            extended_channel->logExtended(makeDroppedMessagesWarning(dropped)->msg_ext);
        }
        catch (...)
        {
            /// Don't lose the count if we failed to report it
            queue.dropped_messages.fetch_add(dropped, std::memory_order_relaxed);
            throw;
        }
    };

    auto flush_queue = [&]()
    {
        /// Process only the messages which were in the queue when the flush started,
        /// so that concurrent producers cannot prolong the flush indefinitely
        size_t count = queue.messages.size();
        while (count-- > 0)
        {
            auto notif = dequeueMessageForFlush(queue.messages);
            if (!notif)
                break;
            extended_channel->logExtended(notif->msg_ext);
        }
    };

    while (is_open)
    {
        try
        {
            AsyncLogMessagePtr message;
            if (queue.messages.tryPop(message))
            {
                extended_channel->logExtended(message->msg_ext);
            }
            else
            {
                /// The queue is empty: wait for new messages. Since we've fully caught up, this is also
                /// the right moment to report the messages dropped during an overflow (if any): reporting
                /// only here coalesces all drops of one overflow episode into a single warning which cannot
                /// get ahead of the messages enqueued before it, and reporting cannot eat into the consumer
                /// throughput while it is still behind.
                sleepForMilliseconds(sleep_on_empty_queue_ms);
                report_dropped_messages();
            }
        }
        catch (...)
        {
            const std::string & exception_message = getCurrentExceptionMessage(true);
            writeRetry(STDERR_FILENO, "Cannot log message in OwnAsyncSplitChannel channel: ");
            writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
            writeRetry(STDERR_FILENO, "\n");
        }
    }

    try
    {
        /// Flush everything before closing and report the drops which were not reported yet
        flush_queue();
        report_dropped_messages();
    }
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);
        writeRetry(STDERR_FILENO, "Cannot flush messages in OwnAsyncSplitChannel channel: ");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}

void OwnAsyncSplitChannel::runTextLog()
{
    DB::setThreadName(ThreadName::ASYNC_TEXT_LOG);

    auto report_dropped_messages = [&](const std::shared_ptr<SystemLogQueue<TextLogElement>> & text_log_locked)
    {
        if (likely(text_log_queue.dropped_messages.load(std::memory_order_relaxed) == 0))
            return;

        size_t dropped = text_log_queue.dropped_messages.exchange(0, std::memory_order_relaxed);
        try
        {
            const auto warning = makeDroppedMessagesWarning(dropped);
            logToSystemTextLogQueue(text_log_locked, warning->msg_ext, warning->msg_thread_name);
        }
        catch (...)
        {
            /// Don't lose the count if we failed to report it
            text_log_queue.dropped_messages.fetch_add(dropped, std::memory_order_relaxed);
            throw;
        }
    };

    auto flush_queue = [&](const std::shared_ptr<SystemLogQueue<TextLogElement>> & text_log_locked)
    {
        /// Process only the messages which were in the queue when the flush started,
        /// so that concurrent producers cannot prolong the flush indefinitely
        size_t count = text_log_queue.messages.size();
        while (count-- > 0)
        {
            auto notif = dequeueMessageForFlush(text_log_queue.messages);
            if (!notif)
                break;
            logToSystemTextLogQueue(text_log_locked, notif->msg_ext, notif->msg_thread_name);
        }
    };

    while (is_open)
    {
        try
        {
            auto text_log_locked = text_log.lock();
            if (!text_log_locked)
                return;

            if (text_log_flush_requested)
            {
                flush_queue(text_log_locked);
                text_log_flush_requested = false;
                text_log_flush_requested.notify_all();
                continue;
            }

            AsyncLogMessagePtr message;
            if (text_log_queue.messages.tryPop(message))
            {
                logToSystemTextLogQueue(text_log_locked, message->msg_ext, message->msg_thread_name);
            }
            else
            {
                /// The queue is empty: wait for new messages. Since we've fully caught up, this is also
                /// the right moment to report the messages dropped during an overflow (if any): reporting
                /// only here coalesces all drops of one overflow episode into a single warning which cannot
                /// get ahead of the messages enqueued before it, and reporting cannot eat into the consumer
                /// throughput while it is still behind.
                sleepForMilliseconds(sleep_on_empty_queue_ms);
                report_dropped_messages(text_log_locked);
            }
        }
        catch (...)
        {
            const std::string & exception_message = getCurrentExceptionMessage(true);
            writeRetry(STDERR_FILENO, "Cannot log message in OwnAsyncSplitChannel text log: ");
            writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
            writeRetry(STDERR_FILENO, "\n");
        }
    }

    try
    {
        /// We want to flush everything already in the queue before closing so all messages are logged,
        /// and report the drops which were not reported yet
        auto text_log_locked = text_log.lock();
        if (!text_log_locked)
            return;

        flush_queue(text_log_locked);
        report_dropped_messages(text_log_locked);
    }
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);
        writeRetry(STDERR_FILENO, "Cannot flush queue in OwnAsyncSplitChannel text log: ");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}

void OwnAsyncSplitChannel::setChannelProperty(const std::string & channel_name, const std::string & name, const std::string & value)
{
    if (auto it = name_to_channels.find(channel_name); it != name_to_channels.end())
        it->second->setProperty(name, value);
}

void OwnAsyncSplitChannel::addChannel(
    ChannelPtr channel,
    const std::string & name,
    int level,
    const ProfileEvents::Event & event_on_passed_message_,
    const ProfileEvents::Event & event_on_dropped_message_)
{
    if (is_open)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempted to register channel '{}' while the split channel is open", name);

    auto element = name_to_channels.try_emplace(name, channel);
    if (!element.second)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Channel {} is already registered", name);
    channel->setLevel(level);

    channels.emplace_back(element.first->second.get());
    queues.emplace_back(std::make_unique<LogQueue>(async_queue_size, event_on_passed_message_, event_on_dropped_message_));
    threads.emplace_back(nullptr);
    const size_t i = threads.size() - 1;
    runnables.emplace_back(new OwnRunnableForChannel(*this, i));
}

void OwnAsyncSplitChannel::addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority)
{
    if (is_open)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempted to register channel for text_log while the split channel is open");
    text_log = log_queue;
    text_log_max_priority.store(max_priority, std::memory_order_relaxed);
    text_log_thread = nullptr;
    text_log_runnable = std::make_unique<OwnRunnableForTextLog>(*this);
}

void OwnAsyncSplitChannel::setLevel(const std::string & name, int level)
{
    if (auto it = name_to_channels.find(name); it != name_to_channels.end())
        it->second->setLevel(level);
}
}
