#include "OwnSplitChannel.h"

#include <iostream>
#include <Core/Block.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/TextLog.h>
#include <sys/time.h>
#include <Poco/Message.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <common/getThreadId.h>
#include <Common/SensitiveDataMasker.h>

namespace DB
{
void OwnSplitChannel::log(const Poco::Message & msg)
{
    auto logs_queue = CurrentThread::getInternalTextLogsQueue();

    if (channels.empty() && (logs_queue == nullptr || msg.getPriority() > logs_queue->max_priority))
        return;

    if (auto * masker = SensitiveDataMasker::getInstance())
    {
        auto message_text = msg.getText();
        auto matches = masker->wipeSensitiveData(message_text);
        if (matches > 0)
        {
            logSplit({msg, message_text}); // we will continue with the copy of original message with text modified
            return;
        }

    }

    logSplit(msg);
}


void OwnSplitChannel::logSplit(const Poco::Message & msg)
{
    ExtendedLogMessage msg_ext = ExtendedLogMessage::getFrom(msg);

    /// Log data to child channels
    for (auto & channel : channels)
    {
        if (channel.second)
            channel.second->logExtended(msg_ext); // extended child
        else
            channel.first->log(msg); // ordinary child
    }

    auto logs_queue = CurrentThread::getInternalTextLogsQueue();

    /// Log to "TCP queue" if message is not too noisy
    if (logs_queue && msg.getPriority() <= logs_queue->max_priority)
    {
        MutableColumns columns = InternalTextLogsQueue::getSampleColumns();

        size_t i = 0;
        columns[i++]->insert(msg_ext.time_seconds);
        columns[i++]->insert(msg_ext.time_microseconds);
        columns[i++]->insert(DNSResolver::instance().getHostName());
        columns[i++]->insert(msg_ext.query_id);
        columns[i++]->insert(msg_ext.thread_id);
        columns[i++]->insert(Int64(msg.getPriority()));
        columns[i++]->insert(msg.getSource());
        columns[i++]->insert(msg.getText());

        logs_queue->emplace(std::move(columns));
    }

    /// Also log to system.text_log table, if message is not too noisy
    auto text_log_max_priority_loaded = text_log_max_priority.load(std::memory_order_relaxed);
    if (text_log_max_priority_loaded && msg.getPriority() <= text_log_max_priority_loaded)
    {
        TextLogElement elem;

        elem.event_time = msg_ext.time_seconds;
        elem.microseconds = msg_ext.time_microseconds;

        elem.thread_name = getThreadName();
        elem.thread_id = msg_ext.thread_id;

        elem.query_id = msg_ext.query_id;

        elem.message = msg.getText();
        elem.logger_name = msg.getSource();
        elem.level = msg.getPriority();

        if (msg.getSourceFile() != nullptr)
            elem.source_file = msg.getSourceFile();

        elem.source_line = msg.getSourceLine();

        std::lock_guard<std::mutex> lock(text_log_mutex);
        if (auto log = text_log.lock())
            log->add(elem);
    }
}


void OwnSplitChannel::addChannel(Poco::AutoPtr<Poco::Channel> channel)
{
    channels.emplace_back(std::move(channel), dynamic_cast<ExtendedLogChannel *>(channel.get()));
}

void OwnSplitChannel::addTextLog(std::shared_ptr<DB::TextLog> log, int max_priority)
{
    std::lock_guard<std::mutex> lock(text_log_mutex);
    text_log = log;
    text_log_max_priority.store(max_priority, std::memory_order_relaxed);
}

}
