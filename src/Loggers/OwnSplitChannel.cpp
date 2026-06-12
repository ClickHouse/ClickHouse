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


AsyncLogMessageQueue::AsyncLogMessageQueue(
    size_t max_size_, const ProfileEvents::Event & event_on_passed_message_, const ProfileEvents::Event & event_on_drop_message_)
    : queue(max_size_)
    , event_on_passed_message(event_on_passed_message_)
    , event_on_drop_message(event_on_drop_message_)
{
    if (max_size_ == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Asynchronous log message queue cannot have zero size");
}

void AsyncLogMessageQueue::enqueueMessage(AsyncLogMessagePtr message)
{
    ProfileEvents::incrementNoTrace(event_on_passed_message);

    if (unlikely(!queue.tryPush(std::move(message))))
    {
        /// The queue is full: the consumer doesn't keep up. Drop the message and remember the fact
        /// to report it later, so that the warning messages don't contribute to filling the queue even more.
        dropped_messages.fetch_add(1, std::memory_order_relaxed);
        ProfileEvents::incrementNoTrace(event_on_drop_message);
        return;
    }

    /// If we dropped messages due to queue overflow before, report it now that there is room again.
    /// The warning is enqueued only after a successful push of a real message, so that under sustained
    /// overflow the warnings cannot displace real messages by stealing the slots freed by the consumer.
    if (size_t dropped = dropped_messages.load(std::memory_order_relaxed); unlikely(dropped > 0))
    {
        if (dropped_messages.compare_exchange_strong(dropped, 0, std::memory_order_relaxed))
        {
            try
            {
                String log = fmt::format("We've dropped {} log messages in this channel due to queue overflow", dropped);
                auto async_message = std::make_shared<AsyncLogMessage>(Poco::Message("AsyncLogMessageQueue", log, Poco::Message::PRIO_WARNING));
                async_message->msg_ext.query_id.clear();
                if (!queue.tryPush(std::move(async_message)))
                    dropped_messages.fetch_add(dropped, std::memory_order_relaxed);
            }
            catch (...)
            {
                /// Don't lose the count if we failed to construct or enqueue the warning.
                dropped_messages.fetch_add(dropped, std::memory_order_relaxed);
                throw;
            }
        }
    }
}

AsyncLogMessagePtr AsyncLogMessageQueue::waitDequeueMessage()
{
    AsyncLogMessagePtr message;
    if (queue.tryPop(message))
        return message;

    /// The queue is empty, which is expected to be rare (see the comment for sleep_on_empty_queue_ms).
    /// Sleep for a bit and try again; if there is still nothing, return an empty notification,
    /// so that the caller can react to shutdown and flush requests.
    sleepForMilliseconds(sleep_on_empty_queue_ms);
    queue.tryPop(message);
    return message;
}

AsyncLogMessagePtr AsyncLogMessageQueue::dequeueMessage()
{
    AsyncLogMessagePtr message;
    while (!queue.tryPop(message))
    {
        /// The pop fails either if the queue is empty, or if a producer has claimed the slot at the head
        /// of the queue but hasn't finished writing the message into it yet.
        if (queue.size() == 0)
            return nullptr;

        /// Wait for the unfinished slot: a message whose enqueue had completed must not be skipped
        /// by the flush (it could be hidden behind such a slot).
        sleepForMilliseconds(1);
    }
    return message;
}

size_t AsyncLogMessageQueue::getCurrentMessageSize() const
{
    return queue.size();
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
                queues[i]->enqueueMessage(notification);
        }

        if (text_log_max_priority_loaded >= msg_priority)
            text_log_queue.enqueueMessage(std::move(notification));
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
    text_log_queue.request_flush.wait(true, std::memory_order_seq_cst);

    /// The async thread checks the flag between messages and when it wakes up from the sleep on an empty queue,
    /// so it will notice the request on its own
    text_log_queue.request_flush = true;

    /// Now we simply wait for the async thread to notify it has finished flushing
    text_log_queue.request_flush.wait(true, std::memory_order_seq_cst);
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
                metrics.push_back({name, queues[i]->getCurrentMessageSize()});
                break;
            }
        }
    }

    if (text_log.lock())
        metrics.push_back({"TextLog", text_log_queue.getCurrentMessageSize()});

    return metrics;
}

void OwnAsyncSplitChannel::runChannel(size_t i)
{
    DB::setThreadName(ThreadName::ASYNC_LOGGER);
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    auto notification = queues[i]->waitDequeueMessage();
    const auto & extended_channel = channels[i];

    auto log_notification = [&](auto & async_message)
    {
        if (!async_message)
            return;
        if (const auto * own_notification = dynamic_cast<const AsyncLogMessage *>(async_message.get()))
            extended_channel->logExtended(own_notification->msg_ext);
    };

    auto flush_queue = [&]()
    {
        /// Process only the messages which were in the queue when the flush started,
        /// so that concurrent producers cannot prolong the flush indefinitely
        size_t count = queues[i]->getCurrentMessageSize();
        while (count-- > 0)
        {
            auto notif = queues[i]->dequeueMessage();
            if (!notif)
                break;
            log_notification(notif);
        }
    };

    while (is_open)
    {
        try
        {
            log_notification(notification);
            notification = queues[i]->waitDequeueMessage();
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
        /// Flush everything before closing
        log_notification(notification);
        flush_queue();
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

    auto log_notification = [](auto & message, const std::shared_ptr<SystemLogQueue<TextLogElement>> & text_log_locked)
    {
        if (const auto * own_notification = dynamic_cast<const AsyncLogMessage *>(message.get()))
            logToSystemTextLogQueue(text_log_locked, own_notification->msg_ext, own_notification->msg_thread_name);
    };

    auto flush_queue = [&](const std::shared_ptr<SystemLogQueue<TextLogElement>> & text_log_locked)
    {
        /// Process only the messages which were in the queue when the flush started,
        /// so that concurrent producers cannot prolong the flush indefinitely
        size_t count = text_log_queue.getCurrentMessageSize();
        while (count-- > 0)
        {
            auto notif = text_log_queue.dequeueMessage();
            if (!notif)
                break;
            log_notification(notif, text_log_locked);
        }
    };

    auto notification = text_log_queue.waitDequeueMessage();
    while (is_open)
    {
        try
        {
            if (text_log_queue.request_flush)
            {
                auto text_log_locked = text_log.lock();
                if (!text_log_locked)
                    return;

                if (notification)
                    log_notification(notification, text_log_locked);

                flush_queue(text_log_locked);

                text_log_queue.request_flush = false;
                text_log_queue.request_flush.notify_all();
            }
            else if (notification)
            {
                auto text_log_locked = text_log.lock();
                if (!text_log_locked)
                    return;
                log_notification(notification, text_log_locked);
            }

            notification = text_log_queue.waitDequeueMessage();
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
        /// We want to flush everything already in the queue before closing so all messages are logged
        auto text_log_locked = text_log.lock();
        if (!text_log_locked)
            return;

        if (notification)
            log_notification(notification, text_log_locked);
        flush_queue(text_log_locked);
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
    queues.emplace_back(std::make_unique<AsyncLogMessageQueue>(async_queue_size, event_on_passed_message_, event_on_dropped_message_));
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
