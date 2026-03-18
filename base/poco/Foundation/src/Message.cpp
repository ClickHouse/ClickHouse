//
// Message.cpp
//
// Library: Foundation
// Package: Logging
// Module:  Message
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Message.h"
#include "Poco/Exception.h"
#include "Poco/Process.h"
#include "Poco/Thread.h"
#include <algorithm>


namespace Poco {


Message::Message(): 
	_prio(PRIO_FATAL), 
	_tid(0), 
	_file(0),
	_line(0),
	_pMap(0)
{
	init();
}


Message::Message(const std::string& source, const std::string& text, Priority prio): 
	_source(source), 
	_text(text), 
	_prio(prio), 
	_tid(0),
	_file(0),
	_line(0),
	_pMap(0)
{
	init();
}


Message::Message(
        const std::string& source, const std::string& text, Priority prio, const char* file, int line,
        std::string_view fmt_str, const std::vector<std::string>& fmt_str_args):
	_source(source), 
	_text(text), 
	_prio(prio), 
	_tid(0),
	_file(file),
	_line(line),
	_pMap(0),
	_fmt_str(fmt_str),
	_fmt_str_args(fmt_str_args)
{
	init();
}


Message::Message(
        std::string && source, std::string && text, Priority prio, const char * file, int line,
        std::string_view fmt_str, std::vector<std::string> && fmt_str_args):
    _source(std::move(source)),
    _text(std::move(text)),
    _prio(prio),
    _tid(0),
    _file(file),
    _line(line),
    _pMap(0),
    _fmt_str(fmt_str),
    _fmt_str_args(std::move(fmt_str_args))
{
    init();
}

Message::Message(const Message& msg):
	_source(msg._source),
	_text(msg._text),
	_prio(msg._prio),
	_time(msg._time),
	_tid(msg._tid),
	_thread(msg._thread),
	_pid(msg._pid),
	_file(msg._file),
	_line(msg._line),
	_fmt_str(msg._fmt_str),
	_fmt_str_args(msg._fmt_str_args)
{
	if (msg._pMap)
		_pMap = new StringMap(*msg._pMap);
	else
		_pMap = 0;
}


Message::Message(const Message& msg, const std::string& text):
	_source(msg._source),
	_text(text),
	_prio(msg._prio),
	_time(msg._time),
	_tid(msg._tid),
	_thread(msg._thread),
	_pid(msg._pid),
	_file(msg._file),
	_line(msg._line),
	_fmt_str(msg._fmt_str),
	_fmt_str_args(msg._fmt_str_args)
{
	if (msg._pMap)
		_pMap = new StringMap(*msg._pMap);
	else
		_pMap = 0;
}


Message::~Message()
{
	delete _pMap;
}


void Message::init()
{
	_pid = Process::id();
	Thread* pThread = Thread::current();
	if (pThread)
	{
		_tid    = pThread->id();
		_thread = pThread->name();
	}
}


Message& Message::operator = (const Message& msg)
{
	if (&msg != this)
	{
		Message tmp(msg);
		swap(tmp);
	}
	return *this;
}


void Message::swap(Message& msg)
{
	using std::swap;
	swap(_source, msg._source);
	swap(_text, msg._text);
	swap(_prio, msg._prio);
	swap(_time, msg._time);
	swap(_tid, msg._tid);
	swap(_thread, msg._thread);
	swap(_pid, msg._pid);
	swap(_file, msg._file);
	swap(_line, msg._line);
	swap(_pMap, msg._pMap);
	swap(_fmt_str, msg._fmt_str);
	swap(_fmt_str_args, msg._fmt_str_args);
}


void Message::setSource(const std::string& src)
{
	_source = src;
}


void Message::setText(const std::string& text)
{
	_text = text;
}


void Message::appendText(const std::string & text)
{
    _text.append(text);
}


void Message::setPriority(Priority prio)
{
	_prio = prio;
}


void Message::setTime(const Timestamp& t)
{
	_time = t;
}


void Message::setThread(const std::string& thread)
{
	_thread = thread;
}


void Message::setTid(long tid)
{
	_tid = tid;
}


void Message::setPid(long pid)
{
	_pid = pid;
}


void Message::setSourceFile(const char* file)
{
	_file = file;
}


void Message::setSourceLine(int line)
{
	_line = line;
}

std::string_view Message::getFormatString() const
{
    return _fmt_str;
}

void Message::setFormatString(std::string_view fmt_str)
{
    _fmt_str = fmt_str;
}


const std::vector<std::string>& Message::getFormatStringArgs() const
{
    return _fmt_str_args;
}

void Message::setFormatStringArgs(const std::vector<std::string>& fmt_str_args)
{
    _fmt_str_args = fmt_str_args;
}


bool Message::has(const std::string& param) const
{
	return _pMap && (_pMap->find(param) != _pMap->end());
}


const std::string& Message::get(const std::string& param) const
{
	if (_pMap)
	{
		StringMap::const_iterator it = _pMap->find(param);
		if (it != _pMap->end())
	 		return it->second;
	}

	throw NotFoundException();
}


const std::string& Message::get(const std::string& param, const std::string& defaultValue) const
{
	if (_pMap)
	{
		StringMap::const_iterator it = _pMap->find(param);
		if (it != _pMap->end())
	 		return it->second;
	}

	return defaultValue;
}


long Message::getPid() const
{
	if (_pid < 0)
		_pid = Process::id();
	return _pid;
}


void Message::set(const std::string& param, const std::string& value)
{
	if (!_pMap)
		_pMap = new StringMap;

	std::pair<StringMap::iterator, bool> result =
		_pMap->insert(std::make_pair(param, value));
	if (!result.second)
	{
		result.first->second = value;
	}
}


const std::string& Message::operator [] (const std::string& param) const
{
	if (_pMap)
		return (*_pMap)[param];
	else
		throw NotFoundException();
}


std::string& Message::operator [] (const std::string& param)
{
	if (!_pMap)
		_pMap = new StringMap;
	return (*_pMap)[param];
}


} // namespace Poco
