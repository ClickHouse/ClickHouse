#include <Loggers/TextLogSink.h>

#include <Poco/Message.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/TextLog.h>
#include <Loggers/ExtendedLogChannel.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/setThreadName.h>
#include <Common/IO.h>

namespace DB
{

void TextLogSink::log(const ExtendedLogMessage & msg)
{
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

    try
    {
    #ifndef WITHOUT_TEXT_LOG
        auto logs_queue = CurrentThread::getInternalTextLogsQueue();

        /// Log to "TCP queue" if message is not too noisy
        if (logs_queue && logs_queue->isNeeded(msg.base.getPriority(), msg.base.getSource()))
        {
            MutableColumns columns = InternalTextLogsQueue::getSampleColumns();

            size_t i = 0;
            columns[i++]->insert(msg.time_seconds);
            columns[i++]->insert(msg.time_microseconds);
            columns[i++]->insert(DNSResolver::instance().getHostName());
            columns[i++]->insert(msg.query_id);
            columns[i++]->insert(msg.thread_id);
            columns[i++]->insert(static_cast<Int64>(msg.base.getPriority()));
            columns[i++]->insert(msg.base.getSource());
            columns[i++]->insert(msg.base.getText());

            [[maybe_unused]] bool push_result = logs_queue->emplace(std::move(columns));
        }

        auto text_log_locked = text_log.lock();
        if (!text_log_locked)
            return;

        /// Also log to system.text_log table, if message is not too noisy
        auto text_log_max_priority_loaded = text_log_max_priority.load(std::memory_order_relaxed);
        if (text_log_max_priority_loaded && msg.base.getPriority() <= text_log_max_priority_loaded)
        {
            TextLogElement elem;

            elem.event_time = msg.time_seconds;
            elem.event_time_microseconds = msg.time_in_microseconds;

            elem.thread_name = getThreadName();
            elem.thread_id = msg.thread_id;

            elem.query_id = msg.query_id;

            elem.message = msg.base.getText();
            elem.logger_name = msg.base.getSource();
            elem.level = msg.base.getPriority();

            if (msg.base.getSourceFile() != nullptr)
                elem.source_file = msg.base.getSourceFile();

            elem.source_line = msg.base.getSourceLine();
            elem.message_format_string = msg.base.getFormatString();

    #define SET_VALUE_IF_EXISTS(INDEX) if ((INDEX) <= msg.base.getFormatStringArgs().size()) (elem.value##INDEX) = msg.base.getFormatStringArgs()[(INDEX) - 1]

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
    #endif
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
        const std::string & message = msg.base.getText();

        /// NOTE: errors are ignored, since nothing can be done.
        writeRetry(STDERR_FILENO, "Cannot add message to the log: ");
        writeRetry(STDERR_FILENO, message.data(), message.size());
        writeRetry(STDERR_FILENO, "\n");
        writeRetry(STDERR_FILENO, exception_message.data(), exception_message.size());
        writeRetry(STDERR_FILENO, "\n");
    }
}

#ifndef WITHOUT_TEXT_LOG
void TextLogSink::addTextLog(std::shared_ptr<SystemLogQueue<TextLogElement>> log_queue, int max_priority)
{
    text_log = log_queue;
    text_log_max_priority.store(max_priority, std::memory_order_relaxed);
}
#endif

}
