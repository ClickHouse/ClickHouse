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

void OwnSplitChannel::log(const Poco::Message & msg)
{
    if (!isLoggingEnabled())
        return;

    const auto & logs_queue = CurrentThread::getInternalTextLogsQueue();
    if (channels.empty() && (logs_queue == nullptr && !logs_queue->isNeeded(msg.getPriority(), msg.getSource())))
        return;

    logSplit(ExtendedLogMessage::getFrom(msg), logs_queue, getThreadName());
}

void OwnSplitChannel::logSplit(
    const ExtendedLogMessage & msg_ext,
    const std::shared_ptr<InternalTextLogsQueue> & logs_queue,
    const std::string & thread_name,
    bool check_masker)
{
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    const Poco::Message & msg = msg_ext.base;

    try
    {
        if (check_masker)
        {
            if (const auto & masker = SensitiveDataMasker::getInstance())
            {
                auto message_text = msg.getText();
                auto matches = masker->wipeSensitiveData(message_text);
                if (matches > 0)
                {
                    const Message masked_message{
                        msg, message_text}; // we will continue with the copy of original message with text modified
                    logSplit(ExtendedLogMessage{masked_message, msg_ext}, logs_queue, thread_name, false);
                    return;
                }
            }
        }

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
        {
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

        auto text_log_locked = text_log.lock();
        if (!text_log_locked)
            return;

        /// Also log to system.text_log table, if message is not too noisy
        auto text_log_max_priority_loaded = text_log_max_priority.load(std::memory_order_relaxed);
        if (text_log_max_priority_loaded && msg.getPriority() <= text_log_max_priority_loaded)
        {
            TextLogElement elem;

            elem.event_time = msg_ext.time_seconds;
            elem.event_time_microseconds = msg_ext.time_in_microseconds;

            elem.thread_name = thread_name;
            elem.thread_id = msg_ext.thread_id;

            elem.query_id = msg_ext.query_id;

            elem.message = msg.getText();
            elem.logger_name = msg.getSource();
            elem.level = msg.getPriority();

            if (msg.getSourceFile() != nullptr)
                elem.source_file = msg.getSourceFile();

            elem.source_line = msg.getSourceLine();
            elem.message_format_string = msg.getFormatString();

#define SET_VALUE_IF_EXISTS(INDEX) if ((INDEX) <= msg.getFormatStringArgs().size()) (elem.value##INDEX) = msg.getFormatStringArgs()[(INDEX) - 1]

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

OwnAsyncSplitChannel::OwnAsyncSplitChannel()
    : thread("AsyncLogger")
{
    thread.start(*this);
}

OwnAsyncSplitChannel::~OwnAsyncSplitChannel()
{
    try
    {
        if (thread.isRunning())
        {
            while (!queue.empty())
                Poco::Thread::sleep(100);

            do
            {
                queue.wakeUpAll();
            } while (!thread.tryJoin(100));
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

class OwnMessageNotification : public Poco::Notification
{
public:
    OwnMessageNotification(const Message & msg, const std::shared_ptr<InternalTextLogsQueue> & logs_queue_)
        : msg_ext(ExtendedLogMessage::getFrom(msg))
        , logs_queue(logs_queue_)
        , thread_name(getThreadName())
    {
    }

    ExtendedLogMessage msg_ext;
    std::shared_ptr<InternalTextLogsQueue> logs_queue;
    std::string thread_name;
};

void OwnAsyncSplitChannel::log(const Poco::Message & msg)
{
    if (!isLoggingEnabled())
        return;

    const auto & logs_queue = CurrentThread::getInternalTextLogsQueue();
    if (sync_channel.channels.empty() && (logs_queue == nullptr && !logs_queue->isNeeded(msg.getPriority(), msg.getSource())))
        return;

    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);
    try
    {
        queue.enqueueNotification(new OwnMessageNotification(msg, logs_queue));
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

void OwnAsyncSplitChannel::run()
{
    Poco::AutoPtr<Poco::Notification> notification = queue.waitDequeueNotification();
    while (notification)
    {
        OwnMessageNotification * own_notification = dynamic_cast<OwnMessageNotification *>(notification.get());
        {
            if (own_notification)
                sync_channel.logSplit(own_notification->msg_ext, own_notification->logs_queue, own_notification->thread_name);
        }
        notification = queue.waitDequeueNotification();
    }
}

void OwnAsyncSplitChannel::setChannelProperty(const std::string & channel_name, const std::string & name, const std::string & value)
{
    sync_channel.setChannelProperty(channel_name, name, value);
}

void OwnAsyncSplitChannel::addChannel(Poco::AutoPtr<Poco::Channel> channel, const std::string & name)
{
    sync_channel.addChannel(channel, name);
}

void OwnAsyncSplitChannel::addTextLog(std::shared_ptr<DB::TextLogQueue> log_queue, int max_priority)
{
    sync_channel.addTextLog(log_queue, max_priority);
}

void OwnAsyncSplitChannel::setLevel(const std::string & name, int level)
{
    sync_channel.setLevel(name, level);
}
}
