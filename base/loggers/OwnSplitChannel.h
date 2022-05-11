#pragma once
#include <atomic>
#include <vector>
#include <map>
#include <mutex>
#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include "ExtendedLogChannel.h"

namespace DB
{
    class TextLog;
}

namespace DB
{
/// Works as Poco::SplitterChannel, but performs additional work:
///  passes logs to Client via TCP interface
///  tries to use extended logging interface of child for more comprehensive logging
class OwnSplitChannel : public Poco::Channel
{
public:
    /// Makes an extended message from msg and passes it to the client logs queue and child (if possible)
    void log(const Poco::Message & msg) override;
    /// Adds a child channel
    void addChannel(Poco::AutoPtr<Poco::Channel> channel, const std::string & name);

    void addTextLog(std::shared_ptr<DB::TextLog> log, int max_priority);

    void setLevel(const std::string & name, int level);

private:
    void logSplit(const Poco::Message & msg);
    void tryLogSplit(const Poco::Message & msg);

    using ChannelPtr = Poco::AutoPtr<Poco::Channel>;
    /// Handler and its pointer casted to extended interface
    using ExtendedChannelPtrPair = std::pair<ChannelPtr, ExtendedLogChannel *>;
    std::map<std::string, ExtendedChannelPtrPair> channels;

    std::mutex text_log_mutex;

    std::weak_ptr<DB::TextLog> text_log;
    std::atomic<int> text_log_max_priority = -1;
};

}
