// Copyright 2008, Yandex LLC.
// See the license of ClickHouse.

#include "Poco/Ext/LevelFilterChannel.h"
#include "Poco/LoggingRegistry.h"
#include "Poco/String.h"


namespace Poco
{

LevelFilterChannel::~LevelFilterChannel()
{
    if (channel)
        channel->release();
}


void LevelFilterChannel::setChannel(Channel * channel_)
{
    if (channel)
        channel->release();
    channel = channel_;
    if (channel)
        channel->duplicate();
}


Channel * LevelFilterChannel::getChannel() const
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


void LevelFilterChannel::setLevel(Message::Priority priority_)
{
    priority = priority_;
}


void LevelFilterChannel::setLevel(const std::string & value)
{
    if (icompare(value, "fatal") == 0)
        setLevel(Message::PRIO_FATAL);
    else if (icompare(value, "critical") == 0)
        setLevel(Message::PRIO_CRITICAL);
    else if (icompare(value, "error") == 0)
        setLevel(Message::PRIO_ERROR);
    else if (icompare(value, "warning") == 0)
        setLevel(Message::PRIO_WARNING);
    else if (icompare(value, "notice") == 0)
        setLevel(Message::PRIO_NOTICE);
    else if (icompare(value, "information") == 0)
        setLevel(Message::PRIO_INFORMATION);
    else if (icompare(value, "debug") == 0)
        setLevel(Message::PRIO_DEBUG);
    else if (icompare(value, "trace") == 0)
        setLevel(Message::PRIO_TRACE);
    else
        throw InvalidArgumentException("Not a valid log value", value);
}


Message::Priority LevelFilterChannel::getLevel() const
{
    return priority;
}


void LevelFilterChannel::setProperty(const std::string & name, const std::string & value)
{
    if (icompare(name, "level") == 0)
        setLevel(value);
    else if (icompare(name, "channel") == 0)
        setChannel(LoggingRegistry::defaultRegistry().channelForName(value));
    else
        Channel::setProperty(name, value);
}


void LevelFilterChannel::log(const Message& msg)
{
    if ((priority >= msg.getPriority()) && channel)
        channel->log(msg);
}

}
