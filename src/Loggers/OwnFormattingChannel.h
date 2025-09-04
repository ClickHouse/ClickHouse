#pragma once
#include <atomic>
#include <Loggers/ExtendedLogMessage.h>
#include <Loggers/OwnPatternFormatter.h>
#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>


namespace DB
{
// Like Poco::FormattingChannel but supports the extended logging interface and log level filter
class OwnFormattingChannel final : public Poco::Channel
{
public:
    explicit OwnFormattingChannel(Poco::AutoPtr<OwnPatternFormatter> pFormatter_, Poco::AutoPtr<Poco::Channel> pChannel_);

    void setChannel(Poco::AutoPtr<Poco::Channel> pChannel_);

    void setLevel(Poco::Message::Priority priority_) { priority = priority_; }
    void setLevel(int level) { priority = static_cast<Poco::Message::Priority>(level); }
    Poco::Message::Priority getPriority() const { return priority; }

    void open() override { pChannel->open(); }
    void close() override { pChannel->close(); }
    void setProperty(const std::string & name, const std::string & value) override { pChannel->setProperty(name, value); }

    void log(const Poco::Message &) override;
    void log(Poco::Message && msg) override;
    void logExtended(const ExtendedLogMessage & msg);

    ~OwnFormattingChannel() override;

private:
    Poco::AutoPtr<OwnPatternFormatter> pFormatter;
    Poco::AutoPtr<Poco::Channel> pChannel;
    std::atomic<Poco::Message::Priority> priority;
};

}
