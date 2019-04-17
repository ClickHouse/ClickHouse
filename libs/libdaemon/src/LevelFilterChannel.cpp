// Copyright 2008, Yandex LLC.
// See the license of ClickHouse.

#include <daemon/LevelFilterChannel.h>
#include "Poco/LoggingRegistry.h"
#include "Poco/Channel.h"
#include "Poco/String.h"


namespace DB
{

LevelFilterChannel::~LevelFilterChannel()
{
    if (channel)
        channel->release();
}


void LevelFilterChannel::setChannel(Poco::Channel * channel_)
{
    if (channel)
        channel->release();
    channel = channel_;
    if (channel)
        channel->duplicate();
}


Poco::Channel * LevelFilterChannel::getChannel() const
{
    return channel;
}

void LevelFilterChannel::open()
{
    if (channel)
        channel->open();
}


void LevelFilterChannel::close()
{
    if (channel)
        channel->close();
}


void LevelFilterChannel::setLevel(Poco::Message::Priority priority_)
{
    priority = priority_;
}


void LevelFilterChannel::setLevel(const std::string & value)
{
    if (Poco::icompare(value, "fatal") == 0)
        setLevel(Poco::Message::PRIO_FATAL);
    else if (Poco::icompare(value, "critical") == 0)
        setLevel(Poco::Message::PRIO_CRITICAL);
    else if (Poco::icompare(value, "error") == 0)
        setLevel(Poco::Message::PRIO_ERROR);
    else if (Poco::icompare(value, "warning") == 0)
        setLevel(Poco::Message::PRIO_WARNING);
    else if (Poco::icompare(value, "notice") == 0)
        setLevel(Poco::Message::PRIO_NOTICE);
    else if (Poco::icompare(value, "information") == 0)
        setLevel(Poco::Message::PRIO_INFORMATION);
    else if (Poco::icompare(value, "debug") == 0)
        setLevel(Poco::Message::PRIO_DEBUG);
    else if (Poco::icompare(value, "trace") == 0)
        setLevel(Poco::Message::PRIO_TRACE);
    else
        throw Poco::InvalidArgumentException("Not a valid log value", value);
}


Poco::Message::Priority LevelFilterChannel::getLevel() const
{
    return priority;
}


void LevelFilterChannel::setProperty(const std::string & name, const std::string & value)
{
    if (Poco::icompare(name, "level") == 0)
        setLevel(value);
    else if (Poco::icompare(name, "channel") == 0)
        setChannel(Poco::LoggingRegistry::defaultRegistry().channelForName(value));
    else
        Poco::Channel::setProperty(name, value);
}


void LevelFilterChannel::log(const Poco::Message& msg)
{
    if ((priority >= msg.getPriority()) && channel)
        channel->log(msg);
}

}
