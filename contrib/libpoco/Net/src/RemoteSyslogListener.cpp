//
// RemoteSyslogListener.cpp
//
// $Id: //poco/1.4/Net/src/RemoteSyslogListener.cpp#4 $
//
// Library: Net
// Package: Logging
// Module:  RemoteSyslogListener
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/RemoteSyslogListener.h"
#include "Poco/Net/RemoteSyslogChannel.h"
#include "Poco/Net/DatagramSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Runnable.h"
#include "Poco/Notification.h"
#include "Poco/AutoPtr.h"
#include "Poco/NumberParser.h"
#include "Poco/NumberFormatter.h"
#include "Poco/DateTimeParser.h"
#include "Poco/Message.h"
#include "Poco/LoggingFactory.h"
#include "Poco/Buffer.h"
#include "Poco/Ascii.h"
#include <cstddef>


namespace Poco {
namespace Net {


//
// MessageNotification
//


class MessageNotification: public Poco::Notification
{
public:
	MessageNotification(const char* buffer, std::size_t length, const Poco::Net::SocketAddress& sourceAddress):
		_message(buffer, length),
		_sourceAddress(sourceAddress)
	{
	}

	MessageNotification(const std::string& message, const Poco::Net::SocketAddress& sourceAddress):
		_message(message),
		_sourceAddress(sourceAddress)
	{
	}
		
	~MessageNotification()
	{
	}
	
	const std::string& message() const
	{
		return _message;
	}
	
	const Poco::Net::SocketAddress& sourceAddress() const
	{
		return _sourceAddress;
	}
	
private:
	std::string _message;
	Poco::Net::SocketAddress _sourceAddress;
};


//
// RemoteUDPListener
//


class RemoteUDPListener: public Poco::Runnable
{
public:
	enum
	{
		WAITTIME_MILLISEC = 1000,
		BUFFER_SIZE = 65536
	};
	
	RemoteUDPListener(Poco::NotificationQueue& queue, Poco::UInt16 port);
	~RemoteUDPListener();

	void run();
	void safeStop();

private:
	Poco::NotificationQueue& _queue;
	DatagramSocket           _socket;
	bool                     _stopped;
};


RemoteUDPListener::RemoteUDPListener(Poco::NotificationQueue& queue, Poco::UInt16 port):
	_queue(queue),
	_socket(Poco::Net::SocketAddress(Poco::Net::IPAddress(), port)),
	_stopped(false)
{
}


RemoteUDPListener::~RemoteUDPListener()
{
}


void RemoteUDPListener::run()
{
	Poco::Buffer<char> buffer(BUFFER_SIZE);
	Poco::Timespan waitTime(WAITTIME_MILLISEC* 1000);
	while (!_stopped)
	{
		try
		{
			if (_socket.poll(waitTime, Socket::SELECT_READ))
			{
				Poco::Net::SocketAddress sourceAddress;
				int n = _socket.receiveFrom(buffer.begin(), BUFFER_SIZE, sourceAddress);
				if (n > 0)
				{
					_queue.enqueueNotification(new MessageNotification(buffer.begin(), n, sourceAddress));
				}
			}
		}
		catch (...)
		{
			// lazy exception catching
		}
	}
}


void RemoteUDPListener::safeStop()
{
	_stopped = true;
}


//
// SyslogParser
//


class SyslogParser: public Poco::Runnable
{
public:
	static const std::string NILVALUE;

	enum
	{
		WAITTIME_MILLISEC = 1000
	};

	SyslogParser(Poco::NotificationQueue& queue, RemoteSyslogListener* pListener);
	~SyslogParser();

	void parse(const std::string& line, Poco::Message& message);
	void run();
	void safeStop();

	static Poco::Message::Priority convert(RemoteSyslogChannel::Severity severity);

private:
	void parsePrio(const std::string& line, std::size_t& pos, RemoteSyslogChannel::Severity& severity, RemoteSyslogChannel::Facility& fac);
	void parseNew(const std::string& line, RemoteSyslogChannel::Severity severity, RemoteSyslogChannel::Facility fac, std::size_t& pos, Poco::Message& message);
	void parseBSD(const std::string& line, RemoteSyslogChannel::Severity severity, RemoteSyslogChannel::Facility fac, std::size_t& pos, Poco::Message& message);

	static std::string parseUntilSpace(const std::string& line, std::size_t& pos);
		/// Parses until it encounters the next space char, returns the string from pos, excluding space
		/// pos will point past the space char

private:
	Poco::NotificationQueue& _queue;
	bool                     _stopped;
	RemoteSyslogListener*    _pListener;
};


const std::string SyslogParser::NILVALUE("-");


SyslogParser::SyslogParser(Poco::NotificationQueue& queue, RemoteSyslogListener* pListener):
	_queue(queue),
	_stopped(false),
	_pListener(pListener)
{
	poco_check_ptr (_pListener);
}


SyslogParser::~SyslogParser()
{
}


void SyslogParser::run()
{
	while (!_stopped)
	{
		try
		{
			Poco::AutoPtr<Poco::Notification> pNf(_queue.waitDequeueNotification(WAITTIME_MILLISEC));
			if (pNf)
			{
				Poco::AutoPtr<MessageNotification> pMsgNf = pNf.cast<MessageNotification>();
				Poco::Message message;
				parse(pMsgNf->message(), message);
				message["addr"] =pMsgNf->sourceAddress().host().toString();
				_pListener->log(message);
			}
		}
		catch (Poco::Exception&)
		{
			// parsing exception, what should we do?
		}
		catch (...)
		{
		}
	}
}


void SyslogParser::safeStop()
{
	_stopped = true;
}


void SyslogParser::parse(const std::string& line, Poco::Message& message)
{
	// <int> -> int: lower 3 bits severity, upper bits: facility
	std::size_t pos = 0;
	RemoteSyslogChannel::Severity severity;
	RemoteSyslogChannel::Facility fac;
	parsePrio(line, pos, severity, fac);

	// the next field decide if we parse an old BSD message or a new syslog message
	// BSD: expects a month value in string form: Jan, Feb...
	// SYSLOG expects a version number: 1
	
	if (Poco::Ascii::isDigit(line[pos]))
	{
		parseNew(line, severity, fac, pos, message);
	}
	else
	{
		parseBSD(line, severity, fac, pos, message);
	}
	poco_assert (pos == line.size());
}


void SyslogParser::parsePrio(const std::string& line, std::size_t& pos, RemoteSyslogChannel::Severity& severity, RemoteSyslogChannel::Facility& fac)
{
	poco_assert (pos < line.size());
	poco_assert (line[pos] == '<');
	++pos;
	std::size_t start = pos;
	
	while (pos < line.size() && Poco::Ascii::isDigit(line[pos]))
		++pos;
	
	poco_assert (line[pos] == '>');
	poco_assert (pos - start > 0);
	std::string valStr = line.substr(start, pos - start);
	++pos; // skip the >

	int val = Poco::NumberParser::parse(valStr);
	poco_assert (val >= 0 && val <= (RemoteSyslogChannel::SYSLOG_LOCAL7 + RemoteSyslogChannel::SYSLOG_DEBUG));
	
	Poco::UInt16 pri = static_cast<Poco::UInt16>(val);
	// now get the lowest 3 bits
	severity = static_cast<RemoteSyslogChannel::Severity>(pri & 0x0007u);
	fac = static_cast<RemoteSyslogChannel::Facility>(pri & 0xfff8u);
}


void SyslogParser::parseNew(const std::string& line, RemoteSyslogChannel::Severity severity, RemoteSyslogChannel::Facility fac, std::size_t& pos, Poco::Message& message)
{
	Poco::Message::Priority prio = convert(severity);
	// rest of the unparsed header is:
	// VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID
	std::string versionStr(parseUntilSpace(line, pos));
	std::string timeStr(parseUntilSpace(line, pos)); // can be the nilvalue!
	std::string hostName(parseUntilSpace(line, pos));
	std::string appName(parseUntilSpace(line, pos));
	std::string procId(parseUntilSpace(line, pos));
	std::string msgId(parseUntilSpace(line, pos));
	std::string messageText(line.substr(pos));
	pos = line.size();
	Poco::DateTime date;
	int tzd = 0;
	bool hasDate = Poco::DateTimeParser::tryParse(RemoteSyslogChannel::SYSLOG_TIMEFORMAT, timeStr, date, tzd);
	Poco::Message logEntry(msgId, messageText, prio);
	logEntry[RemoteSyslogListener::LOG_PROP_HOST] = hostName;
	logEntry[RemoteSyslogListener::LOG_PROP_APP] = appName;
	
	if (hasDate)
		logEntry.setTime(date.timestamp());
	int lval(0);
	Poco::NumberParser::tryParse(procId, lval);
	logEntry.setPid(lval);
	message.swap(logEntry);
}


void SyslogParser::parseBSD(const std::string& line, RemoteSyslogChannel::Severity severity, RemoteSyslogChannel::Facility fac, std::size_t& pos, Poco::Message& message)
{
	Poco::Message::Priority prio = convert(severity);
	// rest of the unparsed header is:
	// "%b %f %H:%M:%S" SP hostname|ipaddress
	// detect three spaces
	int spaceCnt = 0;
	std::size_t start = pos;
	while (spaceCnt < 3 && pos < line.size())
	{
		if (line[pos] == ' ')
		{
			spaceCnt++;
			if (spaceCnt == 1)
			{
				// size must be 3 chars for month
				if (pos - start != 3)
				{
					// probably a shortened time value, or the hostname
					// assume hostName
					Poco::Message logEntry(line.substr(start, pos-start), line.substr(pos+1), prio);
					message.swap(logEntry);
					return;
				}
			}
			else if (spaceCnt == 2)
			{
				// a day value!
				if (!(Poco::Ascii::isDigit(line[pos-1]) && (Poco::Ascii::isDigit(line[pos-2]) || Poco::Ascii::isSpace(line[pos-2]))))
				{
					// assume the next field is a hostname
					spaceCnt = 3;
				}
			}
			if (pos + 1 < line.size() && line[pos+1] == ' ')
			{
				// we have two spaces when the day value is smaller than 10!
				++pos; // skip one
			}
		}
		++pos;
	}
	std::string timeStr(line.substr(start, pos-start-1));
	int tzd(0);
	Poco::DateTime date;
	int year = date.year(); // year is not included, use the current one
	bool hasDate = Poco::DateTimeParser::tryParse(RemoteSyslogChannel::BSD_TIMEFORMAT, timeStr, date, tzd);
	if (hasDate)
	{
		int m = date.month();
		int d = date.day();
		int h = date.hour();
		int min = date.minute();
		int sec = date.second();
		date = Poco::DateTime(year, m, d, h, min, sec);
	}
	// next entry is host SP
	std::string hostName(parseUntilSpace(line, pos));

	// TAG: at most 32 alphanumeric chars, ANY non alphannumeric indicates start of message content
	// ignore: treat everything as content
	std::string messageText(line.substr(pos));
	pos = line.size();
	Poco::Message logEntry(hostName, messageText, prio);
	logEntry.setTime(date.timestamp());
	message.swap(logEntry);
}


std::string SyslogParser::parseUntilSpace(const std::string& line, std::size_t& pos)
{
	std::size_t start = pos;
	while (pos < line.size() && !Poco::Ascii::isSpace(line[pos]))
		++pos;
	// skip space
	++pos;
	return line.substr(start, pos-start-1);
}


Poco::Message::Priority SyslogParser::convert(RemoteSyslogChannel::Severity severity)
{
	switch (severity)
	{
	case RemoteSyslogChannel::SYSLOG_EMERGENCY:
		return Poco::Message::PRIO_FATAL;
	case RemoteSyslogChannel::SYSLOG_ALERT:
		return Poco::Message::PRIO_FATAL;
	case RemoteSyslogChannel::SYSLOG_CRITICAL:
		return Poco::Message::PRIO_CRITICAL;
	case RemoteSyslogChannel::SYSLOG_ERROR:
		return Poco::Message::PRIO_ERROR;
	case RemoteSyslogChannel::SYSLOG_WARNING:
		return Poco::Message::PRIO_WARNING;
	case RemoteSyslogChannel::SYSLOG_NOTICE:
		return Poco::Message::PRIO_NOTICE;
	case RemoteSyslogChannel::SYSLOG_INFORMATIONAL:
		return Poco::Message::PRIO_INFORMATION;
	case RemoteSyslogChannel::SYSLOG_DEBUG:
		return Poco::Message::PRIO_DEBUG;
	}
	throw Poco::LogicException("Illegal severity value in message");
}


//
// RemoteSyslogListener
//


const std::string RemoteSyslogListener::PROP_PORT("port");
const std::string RemoteSyslogListener::PROP_THREADS("threads");

const std::string RemoteSyslogListener::LOG_PROP_APP("app");
const std::string RemoteSyslogListener::LOG_PROP_HOST("host");


RemoteSyslogListener::RemoteSyslogListener():
	_pListener(0),
	_pParser(0),
	_port(RemoteSyslogChannel::SYSLOG_PORT),
	_threads(1)
{
}


RemoteSyslogListener::RemoteSyslogListener(Poco::UInt16 port):
	_pListener(0),
	_pParser(0),
	_port(port),
	_threads(1)
{
}


RemoteSyslogListener::RemoteSyslogListener(Poco::UInt16 port, int threads):
	_pListener(0),
	_pParser(0),
	_port(port),
	_threads(threads)
{
}


RemoteSyslogListener::~RemoteSyslogListener()
{
}


void RemoteSyslogListener::processMessage(const std::string& messageText)
{
	Poco::Message message;
	_pParser->parse(messageText, message);
	log(message);
}


void RemoteSyslogListener::enqueueMessage(const std::string& messageText, const Poco::Net::SocketAddress& senderAddress)
{
	_queue.enqueueNotification(new MessageNotification(messageText, senderAddress));
}


void RemoteSyslogListener::setProperty(const std::string& name, const std::string& value)
{
	if (name == PROP_PORT)
	{
		int val = Poco::NumberParser::parse(value);
		if (val >= 0 && val < 65536)
			_port = static_cast<Poco::UInt16>(val);
		else
			throw Poco::InvalidArgumentException("Not a valid port number", value);
	}
	else if (name == PROP_THREADS)
	{
		int val = Poco::NumberParser::parse(value);
		if (val > 0 && val < 16)
			_threads = val;
		else
			throw Poco::InvalidArgumentException("Invalid number of threads", value);
	}
	else 
	{
		SplitterChannel::setProperty(name, value);
	}
}


std::string RemoteSyslogListener::getProperty(const std::string& name) const
{
	if (name == PROP_PORT)
		return Poco::NumberFormatter::format(_port);
	else if (name == PROP_THREADS)
		return Poco::NumberFormatter::format(_threads);
	else	
		return SplitterChannel::getProperty(name);
}


void RemoteSyslogListener::open()
{
	SplitterChannel::open();
	_pParser = new SyslogParser(_queue, this);
	if (_port > 0)
	{
		_pListener = new RemoteUDPListener(_queue, _port);
	}
	for (int i = 0; i < _threads; i++)
	{
		_threadPool.start(*_pParser);
	}
	if (_pListener)
	{
		_threadPool.start(*_pListener);
	}
}


void RemoteSyslogListener::close()
{
	if (_pListener)
	{
		_pListener->safeStop();
	}
	if (_pParser)
	{
		_pParser->safeStop();
	}
	_queue.clear();
	_threadPool.joinAll();
	delete _pListener;
	delete _pParser;
	_pListener = 0;
	_pParser = 0;
	SplitterChannel::close();
}


void RemoteSyslogListener::registerChannel()
{
	Poco::LoggingFactory::defaultFactory().registerChannelClass("RemoteSyslogListener", new Poco::Instantiator<RemoteSyslogListener, Poco::Channel>);
}


} } // namespace Poco::Net
