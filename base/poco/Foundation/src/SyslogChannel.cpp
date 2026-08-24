//
// SyslogChannel.cpp
//
// Library: Foundation
// Package: Logging
// Module:  SyslogChannel
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SyslogChannel.h"
#include "Poco/Message.h"
#include "Poco/StringTokenizer.h"
#include <syslog.h>


namespace Poco {


const std::string SyslogChannel::PROP_NAME     = "name";
const std::string SyslogChannel::PROP_FACILITY = "facility";
const std::string SyslogChannel::PROP_OPTIONS  = "options";


SyslogChannel::SyslogChannel(): 
	_options(SYSLOG_CONS), 
	_facility(SYSLOG_USER), 
	_open(false)
{
}


SyslogChannel::SyslogChannel(const std::string& name, int options, int facility): 
	_name(name), 
	_options(options), 
	_facility(facility), 
	_open(false)
{
}


SyslogChannel::~SyslogChannel()
{
	close();
}


void SyslogChannel::open()
{
	openlog(_name.c_str(), _options, _facility);
	_open = true;
}


void SyslogChannel::close()
{
	if (_open)
	{
		closelog();
		_open = false;
	}
}


void SyslogChannel::log(const Message& msg)
{
	if (!_open) open();
	syslog(getPrio(msg), "%s", msg.getText().c_str());
}


void SyslogChannel::setProperty(const std::string& name, const std::string& value)
{
	if (name == PROP_NAME)
	{
		_name = value;
	}
	else if (name == PROP_FACILITY)
	{
		if (value == "LOG_KERN")
			_facility = SYSLOG_KERN;
		else if (value == "LOG_USER")
			_facility = SYSLOG_USER;
		else if (value == "LOG_MAIL")
			_facility = SYSLOG_MAIL;
		else if (value == "LOG_DAEMON")
			_facility = SYSLOG_DAEMON;
		else if (value == "LOG_AUTH")
			_facility = SYSLOG_AUTH;
		else if (value == "LOG_AUTHPRIV")
			_facility = SYSLOG_AUTHPRIV;
		else if (value == "LOG_SYSLOG")
			_facility = SYSLOG_SYSLOG;
		else if (value == "LOG_LPR")
			_facility = SYSLOG_LPR;
		else if (value == "LOG_NEWS")
			_facility = SYSLOG_NEWS;
		else if (value == "LOG_UUCP")
			_facility = SYSLOG_UUCP;
		else if (value == "LOG_CRON")
			_facility = SYSLOG_CRON;
		else if (value == "LOG_FTP")
			_facility = SYSLOG_FTP;
		else if (value == "LOG_LOCAL0")
			_facility = SYSLOG_LOCAL0;
		else if (value == "LOG_LOCAL1")
			_facility = SYSLOG_LOCAL1;
		else if (value == "LOG_LOCAL2")
			_facility = SYSLOG_LOCAL2;
		else if (value == "LOG_LOCAL3")
			_facility = SYSLOG_LOCAL3;
		else if (value == "LOG_LOCAL4")
			_facility = SYSLOG_LOCAL4;
		else if (value == "LOG_LOCAL5")
			_facility = SYSLOG_LOCAL5;
		else if (value == "LOG_LOCAL6")
			_facility = SYSLOG_LOCAL6;
		else if (value == "LOG_LOCAL7")
			_facility = SYSLOG_LOCAL7;
	}
	else if (name == PROP_OPTIONS)
	{
		_options = 0;
		StringTokenizer tokenizer(value, "|+:;,", StringTokenizer::TOK_IGNORE_EMPTY | StringTokenizer::TOK_TRIM);
		for (StringTokenizer::Iterator it = tokenizer.begin(); it != tokenizer.end(); ++it)
		{
			if (*it == "LOG_CONS")
				_options |= SYSLOG_CONS;
			else if (*it == "LOG_NDELAY")
				_options |= SYSLOG_NDELAY;
			else if (*it == "LOG_PERROR")
				_options |= SYSLOG_PERROR;
			else if (*it == "LOG_PID")
				_options |= SYSLOG_PID;
		}
	}
	else
	{
		Channel::setProperty(name, value);
	}
}


std::string SyslogChannel::getProperty(const std::string& name) const
{
	if (name == PROP_NAME)
	{
		return _name;
	}
	else if (name == PROP_FACILITY)
	{
		if (_facility == SYSLOG_KERN)
			return "LOG_KERN";
		else if (_facility == SYSLOG_USER)
			return "LOG_USER";
		else if (_facility == SYSLOG_MAIL)
			return "LOG_MAIL";
		else if (_facility == SYSLOG_DAEMON)
			return "LOG_DAEMON";
		else if (_facility == SYSLOG_AUTH)
			return "LOG_AUTH";
		else if (_facility == SYSLOG_AUTHPRIV)
			return "LOG_AUTHPRIV";
		else if (_facility == SYSLOG_SYSLOG)
			return "LOG_SYSLOG";
		else if (_facility == SYSLOG_LPR)
			return "LOG_LPR";
		else if (_facility == SYSLOG_NEWS)
			return "LOG_NEWS";
		else if (_facility == SYSLOG_UUCP)
			return "LOG_UUCP";
		else if (_facility == SYSLOG_CRON)
			return "LOG_CRON";
		else if (_facility == SYSLOG_FTP)
			return "LOG_FTP";
		else if (_facility == SYSLOG_LOCAL0)
			return "LOG_LOCAL0";
		else if (_facility == SYSLOG_LOCAL1)
			return "LOG_LOCAL1";
		else if (_facility == SYSLOG_LOCAL2)
			return "LOG_LOCAL2";
		else if (_facility == SYSLOG_LOCAL3)
			return "LOG_LOCAL3";
		else if (_facility == SYSLOG_LOCAL4)
			return "LOG_LOCAL4";
		else if (_facility == SYSLOG_LOCAL5)
			return "LOG_LOCAL5";
		else if (_facility == SYSLOG_LOCAL6)
			return "LOG_LOCAL6";
		else if (_facility == SYSLOG_LOCAL7)
			return "LOG_LOCAL7";
		else
			return "";
	}
	else if (name == PROP_OPTIONS)
	{
		std::string result;
		if (_options & SYSLOG_CONS)
		{
			if (!result.empty()) result.append("|");
			result.append("LOG_CONS");
		}
		if (_options & SYSLOG_NDELAY)
		{
			if (!result.empty()) result.append("|");
			result.append("LOG_NDELAY");
		}
		if (_options & SYSLOG_PERROR)
		{
			if (!result.empty()) result.append("|");
			result.append("LOG_PERROR");
		}
		if (_options & SYSLOG_PID)
		{
			if (!result.empty()) result.append("|");
			result.append("LOG_PID");
		}
		return result;
	}
	else
	{
		return Channel::getProperty(name);
	}
}


int SyslogChannel::getPrio(const Message& msg)
{
	switch (msg.getPriority())
	{
	case Message::PRIO_TEST:
	case Message::PRIO_TRACE:
	case Message::PRIO_DEBUG:
		return LOG_DEBUG;
	case Message::PRIO_INFORMATION:
		return LOG_INFO;
	case Message::PRIO_NOTICE:
		return LOG_NOTICE;
	case Message::PRIO_WARNING:
		return LOG_WARNING;
	case Message::PRIO_ERROR:
		return LOG_ERR;
	case Message::PRIO_CRITICAL:
		return LOG_CRIT;
	case Message::PRIO_FATAL:
		return LOG_ALERT;
	default:
		return 0;
	}
}


} // namespace Poco
