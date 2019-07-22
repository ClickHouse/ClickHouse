#include "OwnSplitChannel.h"

#include <iostream>
#include <Core/Block.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/TextLog.h>
#include <sys/time.h>
#include <Poco/Message.h>
#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <common/getThreadNumber.h>


namespace DB
{
void OwnSplitChannel::log(const Poco::Message & msg)
{
    auto logs_queue = CurrentThread::getInternalTextLogsQueue();

    if (channels.empty() && (logs_queue == nullptr || msg.getPriority() > logs_queue->max_priority))
        return;

    ExtendedLogMessage msg_ext = ExtendedLogMessage::getFrom(msg);

    /// Log data to child channels
    for (auto & channel : channels)
    {
        if (channel.second)
            channel.second->logExtended(msg_ext); // extended child
        else
            channel.first->log(msg); // ordinary child
    }

    /// Log to "TCP queue" if message is not too noisy
    if (logs_queue && msg.getPriority() <= logs_queue->max_priority)
    {
        MutableColumns columns = InternalTextLogsQueue::getSampleColumns();

        size_t i = 0;
        columns[i++]->insert(msg_ext.time_seconds);
        columns[i++]->insert(msg_ext.time_microseconds);
        columns[i++]->insert(DNSResolver::instance().getHostName());
        columns[i++]->insert(msg_ext.query_id);
        columns[i++]->insert(msg_ext.thread_number);
        columns[i++]->insert(Int64(msg.getPriority()));
        columns[i++]->insert(msg.getSource());
        columns[i++]->insert(msg.getText());

        logs_queue->emplace(std::move(columns));
    }

    /// Also log to system.internal_text_log table

    if (CurrentThread::getGroup() != nullptr &&
        CurrentThread::getGroup()->global_context != nullptr &&
        CurrentThread::getGroup()->global_context->getTextLog() != nullptr) {

        const auto text_log = CurrentThread::getGroup()->global_context->getTextLog();
        TextLogElement elem;

        elem.event_time = msg_ext.time_seconds;
        elem.microseconds = msg_ext.time_microseconds;

        elem.thread_name = getThreadName();
        elem.thread_number = msg_ext.thread_number;
        elem.os_thread_id = msg.getOsTid();
        elem.message = msg.getText();

        elem.level = msg.getPriority();

        if (msg.getSourceFile() != nullptr) {
            elem.source_file = msg.getSourceFile();
        }

        elem.source_line = msg.getSourceLine();

        text_log->add(elem);
    }

}

void OwnSplitChannel::addChannel(Poco::AutoPtr<Poco::Channel> channel)
{
    channels.emplace_back(std::move(channel), dynamic_cast<ExtendedLogChannel *>(channel.get()));
}


}
