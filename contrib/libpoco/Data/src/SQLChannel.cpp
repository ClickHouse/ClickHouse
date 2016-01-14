//
// SQLChannel.cpp
//
// $Id: //poco/Main/Data/src/SQLChannel.cpp#3 $
//
// Library: Net
// Package: Logging
// Module:  SQLChannel
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SQLChannel.h"
#include "Poco/Data/SessionFactory.h"
#include "Poco/DateTime.h"
#include "Poco/LoggingFactory.h"
#include "Poco/Instantiator.h"
#include "Poco/NumberParser.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Format.h"


namespace Poco {
namespace Data {


using namespace Keywords;


const std::string SQLChannel::PROP_CONNECTOR("connector");
const std::string SQLChannel::PROP_CONNECT("connect");
const std::string SQLChannel::PROP_NAME("name");
const std::string SQLChannel::PROP_TABLE("table");
const std::string SQLChannel::PROP_ARCHIVE_TABLE("archive");
const std::string SQLChannel::PROP_MAX_AGE("keep");
const std::string SQLChannel::PROP_ASYNC("async");
const std::string SQLChannel::PROP_TIMEOUT("timeout");
const std::string SQLChannel::PROP_THROW("throw");


SQLChannel::SQLChannel():
	_name("-"),
	_table("T_POCO_LOG"),
	_timeout(1000),
	_throw(true),
	_async(true)
{
}


SQLChannel::SQLChannel(const std::string& connector, 
	const std::string& connect,
	const std::string& name):
	_connector(connector),
	_connect(connect),
	_name(name),
	_table("T_POCO_LOG"),
	_timeout(1000),
	_throw(true),
	_async(true)
{
	open();
}


SQLChannel::~SQLChannel()
{
	try
	{
		close();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void SQLChannel::open()
{
	if (_connector.empty() || _connect.empty())
		throw IllegalStateException("Connector and connect string must be non-empty.");

	_pSession = new Session(_connector, _connect);
	initLogStatement();
}

	
void SQLChannel::close()
{
	wait();
}


void SQLChannel::log(const Message& msg)
{
	if (_async) logAsync(msg);
	else logSync(msg);
}


void SQLChannel::logAsync(const Message& msg)
{
	poco_check_ptr (_pLogStatement);
	if (0 == wait() && !_pLogStatement->done() && !_pLogStatement->initialized()) 
	{
		if (_throw) 
			throw TimeoutException("Timed out waiting for previous statement completion");
		else return;
	}

	if (!_pSession || !_pSession->isConnected()) open();
	logSync(msg);
}


void SQLChannel::logSync(const Message& msg)
{
	if (_pArchiveStrategy) _pArchiveStrategy->archive();

	_source = msg.getSource();
	_pid = msg.getPid();
	_thread = msg.getThread();
	_tid = msg.getTid();
	_priority = msg.getPriority();
	_text = msg.getText();
	_dateTime = msg.getTime();
	if (_source.empty()) _source = _name;

	try
	{
		_pLogStatement->execute();
	}
	catch (Exception&)
	{
		if (_throw) throw;
	}
}

	
void SQLChannel::setProperty(const std::string& name, const std::string& value)
{
	if (name == PROP_NAME)
	{
		_name = value;
		if (_name.empty()) _name = "-";
	}
	else if (name == PROP_CONNECTOR)
	{
		_connector = value;
		close(); open();
	}
	else if (name == PROP_CONNECT)
	{
		_connect = value;
		close(); open();
	}
	else if (name == PROP_TABLE)
	{
		_table = value;
		initLogStatement();
	}
	else if (name == PROP_ARCHIVE_TABLE)
	{
		if (value.empty())
		{
			_pArchiveStrategy = 0;
		}
		else if (_pArchiveStrategy)
		{
			_pArchiveStrategy->setDestination(value);
		}
		else
		{
			_pArchiveStrategy = new ArchiveByAgeStrategy(_connector, _connect, _table, value);
		}
	}
	else if (name == PROP_MAX_AGE)
	{
		if (value.empty() || "forever" == value)
		{
			_pArchiveStrategy = 0;
		}
		else if (_pArchiveStrategy)
		{
			_pArchiveStrategy->setThreshold(value);
		}
		else
		{
			ArchiveByAgeStrategy* p = new ArchiveByAgeStrategy(_connector, _connect, _table);
			p->setThreshold(value);
			_pArchiveStrategy = p;
		}
	}
	else if (name == PROP_ASYNC)
	{
		_async = isTrue(value);
		initLogStatement();
	}
	else if (name == PROP_TIMEOUT)
	{
		if (value.empty() || '0' == value[0])
			_timeout = Statement::WAIT_FOREVER;
		else
			_timeout = NumberParser::parse(value);
	}
	else if (name == PROP_THROW)
	{
		_throw = isTrue(value);
	}
	else
	{
		Channel::setProperty(name, value);
	}
}

	
std::string SQLChannel::getProperty(const std::string& name) const
{
	if (name == PROP_NAME)
	{
		if (_name != "-") return _name;
		else return "";
	}
	else if (name == PROP_CONNECTOR)
	{
		return _connector;
	}
	else if (name == PROP_CONNECT)
	{
		return _connect;
	}
	else if (name == PROP_TABLE)
	{
		return _table;
	}
	else if (name == PROP_ARCHIVE_TABLE)
	{
		return _pArchiveStrategy ? _pArchiveStrategy->getDestination() : "" ;
	}
	else if (name == PROP_MAX_AGE)
	{
		return _pArchiveStrategy ? _pArchiveStrategy->getThreshold() : "forever";
	}
	else if (name == PROP_TIMEOUT)
	{
		return NumberFormatter::format(_timeout);
	}
	else if (name == PROP_THROW)
	{
		if (_throw) return "true";
		else return "false";
	}
	else
	{
		return Channel::getProperty(name);
	}
}


void SQLChannel::initLogStatement()
{
	_pLogStatement = new Statement(*_pSession);

	std::string sql;
	Poco::format(sql, "INSERT INTO %s VALUES (?,?,?,?,?,?,?,?)", _table);
	*_pLogStatement << sql,
		use(_source),
		use(_name),
		use(_pid),
		use(_thread),
		use(_tid),
		use(_priority),
		use(_text),
		use(_dateTime);

	if (_async) _pLogStatement->setAsync();
}


void SQLChannel::registerChannel()
{
	Poco::LoggingFactory::defaultFactory().registerChannelClass("SQLChannel", 
		new Poco::Instantiator<SQLChannel, Poco::Channel>);
}


} } // namespace Poco::Data
