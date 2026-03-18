//
// FormattingChannel.cpp
//
// Library: Foundation
// Package: Logging
// Module:  Formatter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/FormattingChannel.h"
#include "Poco/Formatter.h"
#include "Poco/Message.h"
#include "Poco/LoggingRegistry.h"


namespace Poco {


FormattingChannel::FormattingChannel(): 
	_pFormatter(0), 
	_pChannel(0)
{
}


FormattingChannel::FormattingChannel(Formatter* pFormatter): 
	_pFormatter(pFormatter), 
	_pChannel(0)
{
	if (_pFormatter) _pFormatter->duplicate();
}


FormattingChannel::FormattingChannel(Formatter* pFormatter, Channel* pChannel): 
	_pFormatter(pFormatter), 
	_pChannel(pChannel)
{
	if (_pFormatter) _pFormatter->duplicate();
	if (_pChannel)   _pChannel->duplicate();
}


FormattingChannel::~FormattingChannel()
{
	if (_pChannel)   _pChannel->release();
	if (_pFormatter) _pFormatter->release();
}


void FormattingChannel::setFormatter(Formatter* pFormatter)
{
	if (_pFormatter) _pFormatter->release();
	_pFormatter = pFormatter;
	if (_pFormatter) _pFormatter->duplicate();
}


Formatter* FormattingChannel::getFormatter() const
{
	return _pFormatter;
}


void FormattingChannel::setChannel(Channel* pChannel)
{
	if (_pChannel) _pChannel->release();
	_pChannel = pChannel;
	if (_pChannel) _pChannel->duplicate();
}


Channel* FormattingChannel::getChannel() const
{
	return _pChannel;
}


void FormattingChannel::log(const Message& msg)
{
	if (_pChannel)
	{
		if (_pFormatter)
		{
			std::string text;
			_pFormatter->format(msg, text);
			_pChannel->log(Message(msg, text));
		}
		else
		{
			_pChannel->log(msg);
		}
	}
}


void FormattingChannel::setProperty(const std::string& name, const std::string& value)
{
	if (name == "channel")
		setChannel(LoggingRegistry::defaultRegistry().channelForName(value));
	else if (name == "formatter")
		setFormatter(LoggingRegistry::defaultRegistry().formatterForName(value));
	else if (_pChannel)
		_pChannel->setProperty(name, value);
}


void FormattingChannel::open()
{
	if (_pChannel)
		_pChannel->open();
}

	
void FormattingChannel::close()
{
	if (_pChannel)
		_pChannel->close();
}


} // namespace Poco
