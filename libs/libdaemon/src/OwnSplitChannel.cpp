#include <daemon/OwnSplitChannel.h>

#include <Common/CurrentThread.h>
#include <Common/DNSResolver.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Core/Block.h>

#include <Poco/Message.h>
#include <Poco/Ext/ThreadNumber.h>

#include <sys/time.h>
#include <iostream>


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
        columns[i++]->insert(static_cast<UInt64>(msg_ext.time_seconds));
        columns[i++]->insert(static_cast<UInt64>(msg_ext.time_microseconds));
        columns[i++]->insert(DNSResolver::instance().getHostName());
        columns[i++]->insert(msg_ext.query_id);
        columns[i++]->insert(static_cast<UInt64>(msg_ext.thread_number));
        columns[i++]->insert(static_cast<Int64>(msg.getPriority()));
        columns[i++]->insert(msg.getSource());
        columns[i++]->insert(msg.getText());

        logs_queue->emplace(std::move(columns));
    }

    /// TODO: Also log to system.internal_text_log table
}

void OwnSplitChannel::addChannel(Poco::AutoPtr<Poco::Channel> channel)
{
    channels.emplace_back(std::move(channel), dynamic_cast<ExtendedLogChannel *>(channel.get()));
}


}
