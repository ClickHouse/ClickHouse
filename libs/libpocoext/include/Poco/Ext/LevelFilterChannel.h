#pragma once

#include "Poco/Foundation.h"
#include "Poco/Channel.h"
#include "Poco/Message.h"
#include <vector>


namespace Poco
{

/// This channel sends messages only higher then specified level
class Foundation_API LevelFilterChannel : public Channel
{
public:
    /// Sends the given Message to all
    /// attaches channels.
    void log(const Message & msg);

    /// Sets or changes a configuration property.
    ///
    /// Only the "level" property is supported, which allows setting desired level
    void setProperty(const std::string & name, const std::string & value);

    /// Sets the destination channel to which the formatted
    /// messages are passed on.
    void setChannel(Channel * channel_);

    /// Returns the channel to which the formatted
    /// messages are passed on.
    Channel * getChannel() const;

    /// Opens the attached channel.
    void open();

    /// Closes the attached channel.
    void close();

    /// Sets the Logger's log level.
    void setLevel(Message::Priority);
    /// Sets the Logger's log level using a symbolic value.
    void setLevel(const std::string & value);

    /// Returns the Logger's log level.
    Message::Priority getLevel() const;

protected:
    ~LevelFilterChannel();

private:
    Channel * channel = nullptr;
    Message::Priority priority = Message::PRIO_ERROR;
};

}
