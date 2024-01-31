//
// Logger.cpp
//
// Library: Foundation
// Package: Logging
// Module:  Logger
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Logger.h"
#include "Poco/Formatter.h"
#include "Poco/LoggingRegistry.h"
#include "Poco/Exception.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NumberParser.h"
#include "Poco/String.h"

#include <cassert>
#include <mutex>

namespace
{

std::mutex & getLoggerMutex()
{
	auto get_logger_mutex_placeholder_memory = []()
	{
		static char buffer[sizeof(std::mutex)]{};
		return buffer;
	};

	static std::mutex * logger_mutex = new (get_logger_mutex_placeholder_memory()) std::mutex();
	return *logger_mutex;
}

Poco::Logger::LoggerMap * _pLoggerMap = nullptr;

}

namespace Poco {


const std::string Logger::ROOT;


Logger::Logger(const std::string& name, Channel* pChannel, int level): _name(name), _pChannel(pChannel), _level(level)
{
	if (pChannel) pChannel->duplicate();
}


Logger::~Logger()
{
	if (_pChannel) _pChannel->release();
}


void Logger::setChannel(Channel* pChannel)
{
	if (_pChannel) _pChannel->release();
	_pChannel = pChannel;
	if (_pChannel) _pChannel->duplicate();
}


Channel* Logger::getChannel() const
{
	return _pChannel;
}


void Logger::setLevel(int level)
{
	_level = level;
}


void Logger::setLevel(const std::string& level)
{
	setLevel(parseLevel(level));
}


void Logger::setProperty(const std::string& name, const std::string& value)
{
	if (name == "channel")
		setChannel(LoggingRegistry::defaultRegistry().channelForName(value));
	else if (name == "level")
		setLevel(value);
	else
		Channel::setProperty(name, value);
}


void Logger::log(const Message& msg)
{
	if (_level >= msg.getPriority() && _pChannel)
	{
		_pChannel->log(msg);
	}
}


void Logger::log(const Exception& exc)
{
	error(exc.displayText());
}


void Logger::log(const Exception& exc, const char* file, int line)
{
	error(exc.displayText(), file, line);
}


void Logger::dump(const std::string& msg, const void* buffer, std::size_t length, Message::Priority prio)
{
	if (_level >= prio && _pChannel)
	{
		std::string text(msg);
		formatDump(text, buffer, length);
		_pChannel->log(Message(_name, text, prio));
	}
}


void Logger::setLevel(const std::string& name, int level)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	if (_pLoggerMap)
	{
		std::string::size_type len = name.length();
		for (auto & it : *_pLoggerMap)
		{
			if (len == 0 ||
				(it.first.compare(0, len, name) == 0 && (it.first.length() == len || it.first[len] == '.')))
			{
				it.second.logger->setLevel(level);
			}
		}
	}
}


void Logger::setChannel(const std::string& name, Channel* pChannel)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	if (_pLoggerMap)
	{
		std::string::size_type len = name.length();
		for (auto & it : *_pLoggerMap)
		{
			if (len == 0 ||
				(it.first.compare(0, len, name) == 0 && (it.first.length() == len || it.first[len] == '.')))
			{
				it.second.logger->setChannel(pChannel);
			}
		}
	}
}


void Logger::setProperty(const std::string& loggerName, const std::string& propertyName, const std::string& value)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	if (_pLoggerMap)
	{
		std::string::size_type len = loggerName.length();
		for (auto & it : *_pLoggerMap)
		{
			if (len == 0 ||
				(it.first.compare(0, len, loggerName) == 0 && (it.first.length() == len || it.first[len] == '.')))
			{
				it.second.logger->setProperty(propertyName, value);
			}
		}
	}
}


std::string Logger::format(const std::string& fmt, const std::string& arg)
{
	std::string args[] =
	{
		arg
	};
	return format(fmt, 1, args);
}


std::string Logger::format(const std::string& fmt, const std::string& arg0, const std::string& arg1)
{
	std::string args[] =
	{
		arg0,
		arg1
	};
	return format(fmt, 2, args);
}


std::string Logger::format(const std::string& fmt, const std::string& arg0, const std::string& arg1, const std::string& arg2)
{
	std::string args[] =
	{
		arg0,
		arg1,
		arg2
	};
	return format(fmt, 3, args);
}


std::string Logger::format(const std::string& fmt, const std::string& arg0, const std::string& arg1, const std::string& arg2, const std::string& arg3)
{
	std::string args[] =
	{
		arg0,
		arg1,
		arg2,
		arg3
	};
	return format(fmt, 4, args);
}


std::string Logger::format(const std::string& fmt, int argc, std::string argv[])
{
	std::string result;
	std::string::const_iterator it = fmt.begin();
	while (it != fmt.end())
	{
		if (*it == '$')
		{
			++it;
			if (*it == '$')
			{
				result += '$';
			}
			else if (*it >= '0' && *it <= '9')
			{
				int i = *it - '0';
				if (i < argc)
					result += argv[i];
			}
			else
			{
				result += '$';
				result += *it;
			}
		}
		else result += *it;
		++it;
	}
	return result;
}


void Logger::formatDump(std::string& message, const void* buffer, std::size_t length)
{
	const int BYTES_PER_LINE = 16;

	message.reserve(message.size() + length*6);
	if (!message.empty()) message.append("\n");
	unsigned char* base = (unsigned char*) buffer;
	int addr = 0;
	while (addr < length)
	{
		if (addr > 0) message.append("\n");
		message.append(NumberFormatter::formatHex(addr, 4));
		message.append("  ");
		int offset = 0;
		while (addr + offset < length && offset < BYTES_PER_LINE)
		{
			message.append(NumberFormatter::formatHex(base[addr + offset], 2));
			message.append(offset == 7 ? "  " : " ");
			++offset;
		}
		if (offset < 7) message.append(" ");
		while (offset < BYTES_PER_LINE) { message.append("   "); ++offset; }
		message.append(" ");
		offset = 0;
		while (addr + offset < length && offset < BYTES_PER_LINE)
		{
			unsigned char c = base[addr + offset];
			message += (c >= 32 && c < 127) ? (char) c : '.';
			++offset;
		}
		addr += BYTES_PER_LINE;
	}
}


namespace
{

inline LoggerPtr makeLoggerPtr(Logger & logger)
{
	return LoggerPtr(&logger, false /*add_ref*/);
}

}


Logger& Logger::get(const std::string& name)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	auto [it, inserted] = unsafeGet(name);

	/** If there are already shared pointer created for this logger
	  * we need to increment Logger reference count and now logger
	  * is owned by logger infrastructure.
	  */
	if (it->second.owned_by_shared_ptr)
	{
		it->second.logger->duplicate();
		it->second.owned_by_shared_ptr = false;
	}

	return *it->second.logger;
}


LoggerPtr Logger::getShared(const std::string & name, bool should_be_owned_by_shared_ptr_if_created)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());
	auto [it, inserted] = unsafeGet(name);

	/** If during `unsafeGet` logger was created, then this shared pointer owns it.
	  * If logger was already created, then this shared pointer does not own it.
	  */
	if (inserted && should_be_owned_by_shared_ptr_if_created)
	{
		it->second.owned_by_shared_ptr = true;
	}
	else
	{
		it->second.logger->duplicate();
	}

	return makeLoggerPtr(*it->second.logger);
}


std::pair<Logger::LoggerMapIterator, bool> Logger::unsafeGet(const std::string& name)
{
	std::optional<Logger::LoggerMapIterator> optional_logger_it = find(name);

	bool should_recreate_logger = false;

	/// Other thread already deleted this logger, but did not yet remove it from map
	if (optional_logger_it && (*optional_logger_it)->second.logger->referenceCount() == 0)
	{
		assert((*optional_logger_it)->second.owned_by_shared_ptr);
		should_recreate_logger = true;
	}

	if (!optional_logger_it || should_recreate_logger)
	{
		Logger * logger = nullptr;

		if (name == ROOT)
		{
			logger = new Logger(name, nullptr, Message::PRIO_INFORMATION);
		}
		else
		{
			Logger& par = parent(name);
			logger = new Logger(name, par.getChannel(), par.getLevel());
		}

		if (should_recreate_logger)
		{
			(*optional_logger_it)->second.logger = logger;
			return std::make_pair(*optional_logger_it, true);
		}

		return add(logger);
	}

	return std::make_pair(*optional_logger_it, false);
}


Logger * Logger::unsafeGetRawPtr(const std::string & name)
{
	return unsafeGet(name).first->second.logger;
}


Logger& Logger::create(const std::string& name, Channel* pChannel, int level)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	return *unsafeCreate(name, pChannel, level).first->second.logger;
}

LoggerPtr Logger::createShared(const std::string & name, Channel * pChannel, int level)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	auto [it, inserted] = unsafeCreate(name, pChannel, level);
	it->second.owned_by_shared_ptr = true;

	return makeLoggerPtr(*it->second.logger);
}

Logger& Logger::root()
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	return *unsafeGetRawPtr(ROOT);
}


Logger* Logger::has(const std::string& name)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	auto optional_it = find(name);
	if (!optional_it)
		return nullptr;

	return (*optional_it)->second.logger;
}


void Logger::shutdown()
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	if (_pLoggerMap)
	{
		for (auto & it : *_pLoggerMap)
		{
			if (it.second.owned_by_shared_ptr)
				continue;

			it.second.logger->release();
		}

		delete _pLoggerMap;
		_pLoggerMap = nullptr;
	}
}


std::optional<Logger::LoggerMapIterator> Logger::find(const std::string& name)
{
	if (_pLoggerMap)
	{
		LoggerMap::iterator it = _pLoggerMap->find(name);
		if (it != _pLoggerMap->end())
			return it;

		return {};
	}

	return {};
}

Logger * Logger::findRawPtr(const std::string & name)
{
	auto optional_it = find(name);
	if (!optional_it)
		return nullptr;

	return (*optional_it)->second.logger;
}


void intrusive_ptr_add_ref(Logger * ptr)
{
	ptr->duplicate();
}


void intrusive_ptr_release(Logger * ptr)
{
	size_t reference_count_before = ptr->_counter.fetch_sub(1, std::memory_order_acq_rel);
	if (reference_count_before != 1)
		return;

	{
		std::lock_guard<std::mutex> lock(getLoggerMutex());

		if (_pLoggerMap)
		{
			auto it = _pLoggerMap->find(ptr->name());
			assert(it != _pLoggerMap->end());

			/** It is possible that during release other thread created logger and
			  * updated iterator in map.
			  */
			if (it->second.logger == ptr)
			{
				/** If reference count is 0, this means this intrusive pointer owns logger
				  * and need destroy it.
				  */
				assert(it->second.owned_by_shared_ptr);
				_pLoggerMap->erase(it);
			}
		}
	}

	delete ptr;
}


void Logger::names(std::vector<std::string>& names)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	names.clear();
	if (_pLoggerMap)
	{
		for (LoggerMap::const_iterator it = _pLoggerMap->begin(); it != _pLoggerMap->end(); ++it)
		{
			names.push_back(it->first);
		}
	}
}


std::pair<Logger::LoggerMapIterator, bool> Logger::unsafeCreate(const std::string & name, Channel * pChannel, int level)
{
	if (find(name)) throw ExistsException();
	Logger* pLogger = new Logger(name, pChannel, level);
	return add(pLogger);
}


Logger& Logger::parent(const std::string& name)
{
	std::string::size_type pos = name.rfind('.');
	if (pos != std::string::npos)
	{
		std::string pname = name.substr(0, pos);
		Logger* pParent = findRawPtr(pname);
		if (pParent)
			return *pParent;
		else
			return parent(pname);
	}
	else return *unsafeGetRawPtr(ROOT);
}


int Logger::parseLevel(const std::string& level)
{
	if (icompare(level, "none") == 0)
		return 0;
	else if (icompare(level, "fatal") == 0)
		return Message::PRIO_FATAL;
	else if (icompare(level, "critical") == 0)
		return Message::PRIO_CRITICAL;
	else if (icompare(level, "error") == 0)
		return Message::PRIO_ERROR;
	else if (icompare(level, "warning") == 0)
		return Message::PRIO_WARNING;
	else if (icompare(level, "notice") == 0)
		return Message::PRIO_NOTICE;
	else if (icompare(level, "information") == 0)
		return Message::PRIO_INFORMATION;
	else if (icompare(level, "debug") == 0)
		return Message::PRIO_DEBUG;
	else if (icompare(level, "trace") == 0)
		return Message::PRIO_TRACE;
	else if (icompare(level, "test") == 0)
		return Message::PRIO_TEST;
	else
	{
		int numLevel;
		if (Poco::NumberParser::tryParse(level, numLevel))
		{
			if (numLevel > 0 && numLevel < 10)
				return numLevel;
			else
				throw InvalidArgumentException("Log level out of range ", level);
		}
		else
			throw InvalidArgumentException("Not a valid log level", level);
	}
}


class AutoLoggerShutdown
{
public:
	AutoLoggerShutdown()
	{
	}
	~AutoLoggerShutdown()
	{
		try
		{
			Logger::shutdown();
		}
		catch (...)
		{
			poco_unexpected();
		}
	}
};


namespace
{
	static AutoLoggerShutdown auto_logger_shutdown;
}


std::pair<Logger::LoggerMapIterator, bool> Logger::add(Logger* pLogger)
{
	if (!_pLoggerMap)
		_pLoggerMap = new Logger::LoggerMap;

	auto result = _pLoggerMap->emplace(pLogger->name(), LoggerEntry{pLogger, false /*owned_by_shared_ptr*/});
	assert(result.second);
	return result;
}


} // namespace Poco
