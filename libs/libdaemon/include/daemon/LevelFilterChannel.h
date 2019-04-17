#pragma once

#include "Poco/Foundation.h"
#include "Poco/Channel.h"
#include "Poco/Message.h"
#include <vector>
#include <string>


namespace Poco {
    class Channel;
}

namespace DB
{

/// This channel sends messages only higher then specified level
class Foundation_API LevelFilterChannel : public Poco::Channel
{
public:
    /// Sends the given Message to all
    /// attaches channels.
    void log(const Poco::Message & msg);

    /// Sets or changes a configuration property.
    ///
    /// Only the "level" property is supported, which allows setting desired level
    void setProperty(const std::string & name, const std::string & value);

    /// Sets the destination channel to which the formatted
    /// messages are passed on.
    void setChannel(Poco::Channel * channel_);

    /// Returns the channel to which the formatted
    /// messages are passed on.
    Poco::Channel * getChannel() const;

    /// Opens the attached channel.
    void open();

    /// Closes the attached channel.
    void close();

    /// Sets the Logger's log level.
    void setLevel(Poco::Message::Priority);
    /// Sets the Logger's log level using a symbolic value.
    void setLevel(const std::string & value);

    /// Returns the Logger's log level.
    Poco::Message::Priority getLevel() const;

protected:
    ~LevelFilterChannel();

private:
    Poco::Channel * channel = nullptr;
    Poco::Message::Priority priority = Poco::Message::PRIO_ERROR;
};

}
