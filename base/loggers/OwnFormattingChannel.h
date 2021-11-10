#pragma once
#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/FormattingChannel.h>
#include "ExtendedLogChannel.h"
#include "OwnPatternFormatter.h"


namespace DB
{
// Like Poco::FormattingChannel but supports the extended logging interface and log level filter
class OwnFormattingChannel : public Poco::Channel, public ExtendedLogChannel
{
public:
    explicit OwnFormattingChannel(
        Poco::AutoPtr<OwnPatternFormatter> pFormatter_ = nullptr, Poco::AutoPtr<Poco::Channel> pChannel_ = nullptr)
        : pFormatter(std::move(pFormatter_)), pChannel(std::move(pChannel_))
    {
    }

    void setChannel(Poco::AutoPtr<Poco::Channel> pChannel_) { pChannel = std::move(pChannel_); }

    void setLevel(Poco::Message::Priority priority_) { priority = priority_; }

    // Poco::Logger::parseLevel returns ints
    void setLevel(int level) { priority = static_cast<Poco::Message::Priority>(level); }

    void open() override
    {
        if (pChannel)
            pChannel->open();
    }

    void close() override
    {
        if (pChannel)
            pChannel->close();
    }

    void log(const Poco::Message & msg) override;
    void logExtended(const ExtendedLogMessage & msg) override;

    ~OwnFormattingChannel() override;

private:
    Poco::AutoPtr<OwnPatternFormatter> pFormatter;
    Poco::AutoPtr<Poco::Channel> pChannel;
    Poco::Message::Priority priority = Poco::Message::PRIO_TRACE;
};

}
