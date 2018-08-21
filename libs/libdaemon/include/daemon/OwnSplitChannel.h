#pragma once
#include <daemon/ExtendedLogChannel.h>
#include <Poco/Channel.h>
#include <Poco/AutoPtr.h>
#include <vector>


namespace DB
{

/// Works as Poco::SplitterChannel, but performs additional work:
///  passes logs to Client via TCP interface
///  tries to use extended logging interface of child for more comprehensive logging
class OwnSplitChannel : public Poco::Channel
{
public:
    OwnSplitChannel() = default;

    /// Makes an extended message from msg and passes it to the client logs queue and child (if possible)
    void log(const Poco::Message & msg) override;

    /// Adds a child channel
    void addChannel(Poco::AutoPtr<Poco::Channel> channel);

    ~OwnSplitChannel() = default;

private:

    using ChannelPtr = Poco::AutoPtr<Poco::Channel>;
    /// Handler and its pointer casted to extended interface
    using ExtendedChannelPtrPair = std::pair<ChannelPtr, ExtendedLogChannel *>;
    std::vector<ExtendedChannelPtrPair> channels;
};

}
