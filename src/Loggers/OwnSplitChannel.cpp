#include <Columns/IColumn.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/TextLog.h>
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnSplitChannel.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <Common/IO.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/setThreadName.h>

#include <Poco/Message.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

void OwnSplitChannel::log(const Poco::Message & msg)
{
    const auto & logs_queue = CurrentThread::getInternalTextLogsQueue();
    if (channels.empty() && (logs_queue == nullptr && !logs_queue->isNeeded(msg.getPriority(), msg.getSource())))
        return;

    if (const auto & masker = SensitiveDataMasker::getInstance())
    {
        auto message_text = msg.getText();
        auto matches = masker->wipeSensitiveData(message_text);
        if (matches > 0)
        {
            const Message masked_message{msg, message_text};
            logSplit(ExtendedLogMessage::getFrom(masked_message), logs_queue, getThreadName());
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
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    const Poco::Message & msg = *msg_ext.base;

    try
    {
        /// Log data to child channels
        for (auto & [name, channel] : channels)
        {
            if (channel.second)
                channel.second->logExtended(msg_ext); // extended child
            else
                channel.first->log(msg); // ordinary child
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


void OwnSplitChannel::addChannel(Poco::AutoPtr<Poco::Channel> channel, const std::string & name)
{
    channels.emplace(name, ExtendedChannelPtrPair(std::move(channel), dynamic_cast<ExtendedLogChannel *>(channel.get())));
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
     {
         if (auto * channel = dynamic_cast<DB::OwnFormattingChannel *>(it->second.first.get()))
            channel->setLevel(level);
     }
}

void OwnSplitChannel::setChannelProperty(const std::string& channel_name, const std::string& name, const std::string& value)
{
    auto it = channels.find(channel_name);
    if (it != channels.end())
    {
        if (auto * channel = dynamic_cast<DB::OwnFormattingChannel *>(it->second.first.get()))
            channel->setProperty(name, value);
    }
}

OwnAsyncSplitChannel::OwnAsyncSplitChannel() = default;

OwnAsyncSplitChannel::~OwnAsyncSplitChannel()
{
    OwnAsyncSplitChannel::close();
}

void OwnAsyncSplitChannel::open()
{
    is_open = true;
    if (text_log_max_priority && text_log_thread && !text_log_thread->isRunning())
        text_log_thread->start(*text_log_runnable);

    for (size_t i = 0; i < channels.size(); i++)
        if (!threads[i]->isRunning())
            threads[i]->start(*runnables[i]);
}

void OwnAsyncSplitChannel::close()
{
    try
    {
        if (text_log_thread && text_log_thread->isRunning())
        {
            while (!text_log_queue.empty())
                Poco::Thread::sleep(100);

            do
            {
                text_log_queue.wakeUpAll();
            } while (!text_log_thread->tryJoin(100));
        }

        for (size_t i = 0; i < channels.size(); i++)
        {
            if (threads[i]->isRunning())
            {
                while (!queues[i]->empty())
                    Poco::Thread::sleep(100);

                do
                {
                    queues[i]->wakeUpAll();
                } while (!threads[i]->tryJoin(100));
            }
        }
    }
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);
        writeRetry(STDERR_FILENO, "Cannot close OwnAsyncSplitChannel: ");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
    is_open = false;
}

class OwnMessageNotification : public Poco::Notification
{
public:
    explicit OwnMessageNotification(const Message & msg_)
        : msg(msg_)
        , msg_ext(ExtendedLogMessage::getFrom(msg))
        , msg_thread_name(getThreadName())
    {
        if (const auto & masker = SensitiveDataMasker::getInstance())
        {
            auto message_text = msg_.getText();
            auto matches = masker->wipeSensitiveData(message_text);
            if (matches > 0)
            {
                msg = Poco::Message(msg_, message_text);
                msg_ext.base = &msg;
            }
        }
    }

    Message msg; /// Need to keep a copy until we finish logging
    ExtendedLogMessage msg_ext;
    std::string msg_thread_name;
};

void OwnAsyncSplitChannel::log(const Poco::Message & msg)
{
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    try
    {
        Poco::AutoPtr<OwnMessageNotification> notification;
        if (const auto & logs_queue = CurrentThread::getInternalTextLogsQueue();
            logs_queue && logs_queue->isNeeded(msg.getPriority(), msg.getSource()))
        {
            /// If we need to push to the TCP queue, do it now since it expects to receive all messages synchronously
            notification = new OwnMessageNotification(msg);
            pushExtendedMessageToInternalTCPTextLogQueue(notification->msg_ext, logs_queue);
        }

        auto text_log_max_priority_loaded = text_log_max_priority.load(std::memory_order_relaxed);
        if (channels.empty() && !text_log_max_priority_loaded)
            return;

        if (!notification)
            notification = new OwnMessageNotification(msg);

        if (msg.getPriority() <= text_log_max_priority_loaded)
            text_log_queue.enqueueNotification(notification);

        for (const auto & queue : queues)
            queue->enqueueNotification(notification);
    }
    catch (...)
    {
        const std::string & exception_message = getCurrentExceptionMessage(true);
        const std::string & message = msg.getText();

        /// NOTE: errors are ignored, since nothing can be done.
        writeRetry(STDERR_FILENO, "Cannot add message to the log queue: ");
        writeRetry(STDERR_FILENO, message.data(), message.size());
        writeRetry(STDERR_FILENO, "\n");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}

void OwnAsyncSplitChannel::flushTextLogs() const
{
    auto text_log_locked = text_log.lock();
    if (!text_log_locked)
        return;

    /// TODO: Improve this to avoid blocking infinitely
    /// Block flushing thread, get the queue and replace it with an empty one, flush the old queue completely, enable flushing thread
    while (!text_log_queue.empty())
        Poco::Thread::sleep(10);
}

void OwnAsyncSplitChannel::runChannel(size_t i)
{
    setThreadName("AsyncLog");
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    Poco::AutoPtr<Poco::Notification> notification = queues[i]->waitDequeueNotification();
    while (notification)
    {
        const OwnMessageNotification * own_notification = dynamic_cast<const OwnMessageNotification *>(notification.get());
        {
            if (own_notification)
            {
                if (channels[i].second)
                    channels[i].second->logExtended(own_notification->msg_ext); // extended child
                else
                    channels[i].first->log(*own_notification->msg_ext.base); // ordinary child
            }
        }
        notification = queues[i]->waitDequeueNotification();
    }
}

void OwnAsyncSplitChannel::runTextLog()
{
    setThreadName("AsyncTextLog", true);
    Poco::AutoPtr<Poco::Notification> notification = text_log_queue.waitDequeueNotification();
    while (notification)
    {
        const OwnMessageNotification * own_notification = dynamic_cast<const OwnMessageNotification *>(notification.get());
        {
            if (own_notification)
            {
                auto text_log_locked = text_log.lock();
                if (!text_log_locked)
                    return;
                logToSystemTextLogQueue(text_log_locked, own_notification->msg_ext, own_notification->msg_thread_name);
            }
        }
        notification = text_log_queue.waitDequeueNotification();
    }
}

void OwnAsyncSplitChannel::setChannelProperty(const std::string & channel_name, const std::string & name, const std::string & value)
{
    if (auto it = name_to_channels.find(channel_name); it != name_to_channels.end())
    {
        if (auto * channel = dynamic_cast<DB::OwnFormattingChannel *>(it->second.first.get()))
            channel->setProperty(name, value);
    }
}

void OwnAsyncSplitChannel::addChannel(Poco::AutoPtr<Poco::Channel> channel, const std::string & name)
{
    if (is_open)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempted to register channel '{}' while the split channel is open", name);

    auto extended = ExtendedChannelPtrPair(std::move(channel), dynamic_cast<ExtendedLogChannel *>(channel.get()));
    auto element = name_to_channels.try_emplace(name, extended);
    if (!element.second)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Channel {} is already registered", name);

    channels.emplace_back(extended);
    queues.emplace_back(std::make_unique<Poco::NotificationQueue>());
    threads.emplace_back(std::make_unique<Poco::Thread>("AsyncLog"));
    const size_t i = threads.size() - 1;
    runnables.emplace_back(new OwnRunnableForChannel(*this, i));
}

void OwnAsyncSplitChannel::addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority)
{
    if (is_open)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempted to register channel for text_log while the split channel is open");
    text_log = log_queue;
    text_log_max_priority.store(max_priority, std::memory_order_relaxed);
    text_log_thread = std::make_unique<Poco::Thread>("AsyncTextLog");
    text_log_runnable = std::make_unique<OwnRunnableForTextLog>(*this);
}

void OwnAsyncSplitChannel::setLevel(const std::string & name, int level)
{
    if (auto it = name_to_channels.find(name); it != name_to_channels.end())
    {
        if (auto * channel = dynamic_cast<DB::OwnFormattingChannel *>(it->second.first.get()))
            channel->setLevel(level);
    }
}
}
