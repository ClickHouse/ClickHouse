#include "OwnSplitChannel.h"

#include <iostream>
#include <Core/Block.h>
#include <Interpreters/InternalTextLogsQueue.h>
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

    if (sensitive_data_masker)
    {
        auto message_text = msg.getText();
        auto matches = sensitive_data_masker->wipeSensitiveData(message_text);
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
        columns[i++]->insert(msg_ext.thread_number);
        columns[i++]->insert(Int64(msg.getPriority()));
        columns[i++]->insert(msg.getSource());
        columns[i++]->insert(msg.getText());

        logs_queue->emplace(std::move(columns));
    }

    /// TODO: Also log to system.internal_text_log table
}


void OwnSplitChannel::setMasker(DB::SensitiveDataMasker * _sensitive_data_masker)
{
    std::lock_guard lock(mutex);
    sensitive_data_masker = _sensitive_data_masker;
}

void OwnSplitChannel::addChannel(Poco::AutoPtr<Poco::Channel> channel)
{
    channels.emplace_back(std::move(channel), dynamic_cast<ExtendedLogChannel *>(channel.get()));
}


}
