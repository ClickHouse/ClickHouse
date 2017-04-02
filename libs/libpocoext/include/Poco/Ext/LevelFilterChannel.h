#pragma once

#include "Poco/Foundation.h"
#include "Poco/Channel.h"
#include "Poco/Mutex.h"
#include "Poco/Message.h"
#include <vector>


namespace Poco {


class Foundation_API LevelFilterChannel: public Channel
    /// This channel sends messages only higher then specified level
{
public:
    void log(const Message& msg);
        /// Sends the given Message to all
        /// attaches channels.

    void setProperty(const std::string& name, const std::string& value);
        /// Sets or changes a configuration property.
        ///
        /// Only the "level" property is supported, which allows setting desired level

    void setChannel(Channel* pChannel);
        /// Sets the destination channel to which the formatted
        /// messages are passed on.

    Channel* getChannel() const;
        /// Returns the channel to which the formatted
        /// messages are passed on.

    void open();
        /// Opens the attached channel.

    void close();
        /// Closes the attached channel.

    void setLevel(Message::Priority);
        /// Sets the Logger's log level.
    void setLevel(const std::string& value);
        /// Sets the Logger's log level using a symbolic value.
    Message::Priority getLevel() const;
        /// Returns the Logger's log level.

protected:
    ~LevelFilterChannel();

private:
    Channel*          _channel = nullptr;
    Message::Priority _priority = Message::PRIO_ERROR;
};


}
