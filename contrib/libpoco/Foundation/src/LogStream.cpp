//
// LogStream.cpp
//
// $Id: //poco/1.4/Foundation/src/LogStream.cpp#1 $
//
// Library: Foundation
// Package: Logging
// Module:  LogStream
//
// Copyright (c) 2006-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/LogStream.h"


namespace Poco {


//
// LogStreamBuf
//


LogStreamBuf::LogStreamBuf(Logger& logger, Message::Priority priority):
	_logger(logger),
	_priority(priority)
{
}


LogStreamBuf::~LogStreamBuf()
{
}


void LogStreamBuf::setPriority(Message::Priority priority)
{
	_priority = priority;
}


int LogStreamBuf::writeToDevice(char c)
{
	if (c == '\n' || c == '\r')
	{
		if (_message.find_first_not_of("\r\n") != std::string::npos)
		{
			Message msg(_logger.name(), _message, _priority);
			_message.clear();
			_logger.log(msg);
		}
	}
	else _message += c;
	return c;
}


//
// LogIOS
//


LogIOS::LogIOS(Logger& logger, Message::Priority priority):
	_buf(logger, priority)
{
	poco_ios_init(&_buf);
}


LogIOS::~LogIOS()
{
}


LogStreamBuf* LogIOS::rdbuf()
{
	return &_buf;
}


//
// LogStream
//


LogStream::LogStream(Logger& logger, Message::Priority priority):
	LogIOS(logger, priority),
	std::ostream(&_buf)
{
}


LogStream::LogStream(const std::string& loggerName, Message::Priority priority):
	LogIOS(Logger::get(loggerName), priority),
	std::ostream(&_buf)
{
}

	
LogStream::~LogStream()
{
}

	
LogStream& LogStream::fatal()
{
	return priority(Message::PRIO_FATAL);
}


LogStream& LogStream::fatal(const std::string& message)
{
	_buf.logger().fatal(message);
	return priority(Message::PRIO_FATAL);
}

	
LogStream& LogStream::critical()
{
	return priority(Message::PRIO_CRITICAL);
}


LogStream& LogStream::critical(const std::string& message)
{
	_buf.logger().critical(message);
	return priority(Message::PRIO_CRITICAL);
}


LogStream& LogStream::error()
{
	return priority(Message::PRIO_ERROR);
}


LogStream& LogStream::error(const std::string& message)
{
	_buf.logger().error(message);
	return priority(Message::PRIO_ERROR);
}


LogStream& LogStream::warning()
{
	return priority(Message::PRIO_WARNING);
}


LogStream& LogStream::warning(const std::string& message)
{
	_buf.logger().warning(message);
	return priority(Message::PRIO_WARNING);
}


LogStream& LogStream::notice()
{
	return priority(Message::PRIO_NOTICE);
}


LogStream& LogStream::notice(const std::string& message)
{
	_buf.logger().notice(message);
	return priority(Message::PRIO_NOTICE);
}


LogStream& LogStream::information()
{
	return priority(Message::PRIO_INFORMATION);
}


LogStream& LogStream::information(const std::string& message)
{
	_buf.logger().information(message);
	return priority(Message::PRIO_INFORMATION);
}


LogStream& LogStream::debug()
{
	return priority(Message::PRIO_DEBUG);
}


LogStream& LogStream::debug(const std::string& message)
{
	_buf.logger().debug(message);
	return priority(Message::PRIO_DEBUG);
}


LogStream& LogStream::trace()
{
	return priority(Message::PRIO_TRACE);
}


LogStream& LogStream::trace(const std::string& message)
{
	_buf.logger().trace(message);
	return priority(Message::PRIO_TRACE);
}


LogStream& LogStream::priority(Message::Priority priority)
{
	_buf.setPriority(priority);
	return *this;
}


} // namespace Poco
