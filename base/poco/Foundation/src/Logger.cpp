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
#include "Poco/LoggingRegistry.h"
#include "Poco/Exception.h"
#include "Poco/NumberParser.h"
#include "Poco/String.h"

#include <cassert>
#include <mutex>
#include <optional>

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

std::optional<Poco::Logger::LoggerMapIterator> findEntry(const std::string & name)
{
	if (!_pLoggerMap)
		return {};
	auto it = _pLoggerMap->find(name);
	if (it == _pLoggerMap->end())
		return {};
	return it;
}

Poco::Logger * findEntryRawPtr(const std::string & name)
{
	auto it = findEntry(name);
	return it ? (*it)->second.logger : nullptr;
}

std::pair<Poco::Logger::LoggerMapIterator, bool> addEntry(Poco::Logger * pLogger)
{
	if (!_pLoggerMap)
		_pLoggerMap = new Poco::Logger::LoggerMap;

	auto result = _pLoggerMap->emplace(pLogger->name(), Poco::Logger::LoggerEntry{pLogger, false /*owned_by_shared_ptr*/});
	assert(result.second);
	return result;
}

std::pair<Poco::Logger::LoggerMapIterator, bool> unsafeGet(const std::string & name, bool get_shared);

Poco::Logger * unsafeGetRawPtr(const std::string & name)
{
	return unsafeGet(name, false /*get_shared*/).first->second.logger;
}

Poco::Logger & parentLogger(const std::string & name)
{
	std::string::size_type pos = name.rfind('.');
	if (pos != std::string::npos)
	{
		std::string pname = name.substr(0, pos);
		if (Poco::Logger * pParent = findEntryRawPtr(pname))
			return *pParent;
		return parentLogger(pname);
	}
	return *unsafeGetRawPtr(Poco::Logger::ROOT);
}

std::pair<Poco::Logger::LoggerMapIterator, bool> unsafeGet(const std::string & name, bool get_shared)
{
	auto optional_logger_it = findEntry(name);

	if (optional_logger_it)
	{
		auto & logger_it = *optional_logger_it;
		if (logger_it->second.owned_by_shared_ptr)
		{
			logger_it->second.logger->duplicate();
			if (!get_shared)
				logger_it->second.owned_by_shared_ptr = false;
		}
		return std::make_pair(logger_it, false);
	}

	Poco::Logger * logger = nullptr;
	if (name == Poco::Logger::ROOT)
		logger = new Poco::Logger(name, nullptr, Poco::Message::PRIO_INFORMATION);
	else
	{
		Poco::Logger & par = parentLogger(name);
		logger = new Poco::Logger(name, par.getChannel(), par.getLevel());
	}
	return addEntry(logger);
}

std::pair<Poco::Logger::LoggerMapIterator, bool> unsafeCreate(const std::string & name, Poco::Channel * pChannel, int level)
{
	if (findEntry(name)) throw Poco::ExistsException();
	return addEntry(new Poco::Logger(name, pChannel, level));
}

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

void Logger::log(Message && msg)
{
    if (_level >= msg.getPriority() && _pChannel)
    {
        _pChannel->log(std::move(msg));
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


namespace
{

struct LoggerDeleter
{
	void operator()(Poco::Logger * logger)
	{
		std::lock_guard<std::mutex> lock(getLoggerMutex());

		/// If logger infrastructure is destroyed just decrement logger reference count
		if (!_pLoggerMap)
		{
			logger->release();
			return;
		}

		auto it = _pLoggerMap->find(logger->name());
		assert(it != _pLoggerMap->end());

		/** If reference count is 1, this means this shared pointer owns logger
		  * and need destroy it.
		  */
		size_t reference_count_before_release = logger->release();
		if (reference_count_before_release == 1)
		{
			assert(it->second.owned_by_shared_ptr);
			_pLoggerMap->erase(it);
		}
	}
};

inline LoggerPtr makeLoggerPtr(Logger & logger, bool owned_by_shared_ptr)
{
	if (owned_by_shared_ptr)
		return LoggerPtr(&logger, LoggerDeleter());

	return LoggerPtr(std::shared_ptr<void>{}, &logger);
}

}


Logger& Logger::get(const std::string& name)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	auto [it, inserted] = unsafeGet(name, false /*get_shared*/);
	return *it->second.logger;
}


LoggerPtr Logger::getShared(const std::string & name, bool should_be_owned_by_shared_ptr_if_created)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());
	auto [it, inserted] = unsafeGet(name, true /*get_shared*/);

	/** If during `unsafeGet` logger was created, then this shared pointer owns it.
	  * If logger was already created, then this shared pointer does not own it.
	  */
	if (inserted && should_be_owned_by_shared_ptr_if_created)
		it->second.owned_by_shared_ptr = true;

	return makeLoggerPtr(*it->second.logger, it->second.owned_by_shared_ptr);
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

	return makeLoggerPtr(*it->second.logger, it->second.owned_by_shared_ptr);
}

Logger& Logger::root()
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	return *unsafeGetRawPtr(ROOT);
}


Logger* Logger::has(const std::string& name)
{
	std::lock_guard<std::mutex> lock(getLoggerMutex());

	return findEntryRawPtr(name);
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


} // namespace Poco
