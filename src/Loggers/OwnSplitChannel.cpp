#include <Columns/IColumn.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/TextLog.h>
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnSplitChannel.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <Common/IO.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTrackerDebugBlockerInThread.h>
#include <Common/ProfileEvents.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/setThreadName.h>

#include <Poco/Message.h>


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
    const std::string & msg_thread_name)
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
    const ExtendedLogMessage & msg_ext, const std::shared_ptr<InternalTextLogsQueue> & logs_queue, const std::string & msg_thread_name)
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
        if (text_log_thread)
        {
            do
            {
                text_log_queue.wakeUp();
            } while (!text_log_thread->tryJoin(100));
            text_log_thread.reset();
        }

        for (size_t i = 0; i < channels.size(); i++)
        {
            if (threads[i])
            {
                do
                {
                    queues[i]->wakeUp();
                } while (!threads[i]->tryJoin(100));
            }
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
    std::string msg_thread_name;
};


AsyncLogMessageQueue::AsyncLogMessageQueue(
    size_t max_size_, const ProfileEvents::Event & event_on_passed_message_, const ProfileEvents::Event & event_on_drop_message_)
    : event_on_passed_message(event_on_passed_message_)
    , event_on_drop_message(event_on_drop_message_)
    , max_size(max_size_)
{
    if (max_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Asynchronous log message queue cannot have zero size");
}

void AsyncLogMessageQueue::enqueueMessage(AsyncLogMessagePtr message)
{
    ProfileEvents::incrementNoTrace(event_on_passed_message);
    std::unique_lock lock(mutex);
    size_t current_size = message_queue.size();
    if (unlikely(current_size >= max_size))
    {
        /// If the queue is full we start dropping messages until it's less than half of the max size
        /// in order to give the thread a change to recover and to reduce the amount of warning messages (about dropped messages)
        /// which would contribute to fill the queue even more
        dropped_messages++;
        lock.unlock();
        ProfileEvents::incrementNoTrace(event_on_drop_message);
        return;
    }

    if (unlikely(dropped_messages))
    {
        String log = "We've dropped " + toString(dropped_messages) + " log messages in this channel due to queue overflow";
        auto async_message = std::make_shared<AsyncLogMessage>(Poco::Message("AsyncLogMessageQueue", log, Poco::Message::PRIO_WARNING));
        async_message->msg_ext.query_id.clear();
        message_queue.push_back(async_message);
        dropped_messages = 0;
    }

    message_queue.push_back(std::move(message));
    /// Request the thread to flush as fast as possible (without acquiring the mutex every time)
    if (current_size > max_size / 2)
        request_flush = true;
    condition.notify_one();
}

AsyncLogMessagePtr AsyncLogMessageQueue::waitDequeueMessage()
{
    std::unique_lock lock(mutex);
    if (!message_queue.empty())
    {
        auto notification = std::move(message_queue.front());
        message_queue.pop_front();
        return notification;
    }

    condition.wait(lock);
    if (message_queue.empty())
        return nullptr;

    auto notification = std::move(message_queue.front());
    message_queue.pop_front();
    return notification;
}

AsyncLogMessageQueue::Queue AsyncLogMessageQueue::getCurrentQueueAndClear()
{
    std::unique_lock lock(mutex);
    Queue new_queue;
    std::swap(message_queue, new_queue);
    return new_queue;
}

void AsyncLogMessageQueue::wakeUp()
{
    std::unique_lock lock(mutex);
    condition.notify_one();
}

size_t AsyncLogMessageQueue::getCurrentMessageSize()
{
    std::unique_lock lock(mutex);
    return message_queue.size();
}

void OwnAsyncSplitChannel::log(const Poco::Message & msg)
{
    log(Poco::Message(msg));
}

void OwnAsyncSplitChannel::log(Poco::Message && msg)
{
    try
    {
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

    /// We need to send an empty notification to wake up the thread if necessary
    text_log_queue.request_flush = true;
    text_log_queue.wakeUp();

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
    setThreadName("AsyncLog");
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
        /// We want to process only what's currently in the queue and not block other logging
        auto queue = queues[i]->getCurrentQueueAndClear();
        while (!queue.empty())
        {
            auto notif = std::move(queue.front());
            queue.pop_front();
            log_notification(notif);
        }
    };

    while (is_open)
    {
        try
        {
            log_notification(notification);
            if (queues[i]->request_flush)
            {
                flush_queue();
                queues[i]->request_flush = false;
            }

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
    setThreadName("AsyncTextLog", true);

    auto log_notification = [](auto & message, const std::shared_ptr<SystemLogQueue<TextLogElement>> & text_log_locked)
    {
        if (const auto * own_notification = dynamic_cast<const AsyncLogMessage *>(message.get()))
            logToSystemTextLogQueue(text_log_locked, own_notification->msg_ext, own_notification->msg_thread_name);
    };

    auto flush_queue = [&](const std::shared_ptr<SystemLogQueue<TextLogElement>> & text_log_locked)
    {
        /// We want to process only what's currently in the queue and not block other logging
        auto queue = text_log_queue.getCurrentQueueAndClear();
        while (!queue.empty())
        {
            auto notif = std::move(queue.front());
            queue.pop_front();
            if (notif)
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
