//
// POP3ClientSession.cpp
//
// $Id: //poco/1.4/Net/src/POP3ClientSession.cpp#1 $
//
// Library: Net
// Package: Mail
// Module:  POP3ClientSession
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/POP3ClientSession.h"
#include "Poco/Net/MailMessage.h"
#include "Poco/Net/MailStream.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetException.h"
#include "Poco/StreamCopier.h"
#include "Poco/NumberFormatter.h"
#include "Poco/UnbufferedStreamBuf.h"
#include "Poco/Ascii.h"
#include <istream>


using Poco::NumberFormatter;
using Poco::StreamCopier;


namespace Poco {
namespace Net {


class DialogStreamBuf: public Poco::UnbufferedStreamBuf
{
public:
	DialogStreamBuf(DialogSocket& socket):
		_socket(socket)
	{
	}
	
	~DialogStreamBuf()
	{
	}
		
private:
	int readFromDevice()
	{
		return _socket.get();
	}
	
	DialogSocket& _socket;
};


class DialogIOS: public virtual std::ios
{
public:
	DialogIOS(DialogSocket& socket):
		_buf(socket)
	{
		poco_ios_init(&_buf);
	}
	
	~DialogIOS()
	{
	}
	
	DialogStreamBuf* rdbuf()
	{
		return &_buf;
	}

protected:
	DialogStreamBuf _buf;
};


class DialogInputStream: public DialogIOS, public std::istream
{
public:
	DialogInputStream(DialogSocket& socket):
		DialogIOS(socket),
		std::istream(&_buf)
	{
	}
		
	~DialogInputStream()
	{
	}
};


POP3ClientSession::POP3ClientSession(const StreamSocket& socket):
	_socket(socket),
	_isOpen(true)
{
}


POP3ClientSession::POP3ClientSession(const std::string& host, Poco::UInt16 port):	
	_socket(SocketAddress(host, port)),
	_isOpen(true)
{
}


POP3ClientSession::~POP3ClientSession()
{
	try
	{
		close();
	}
	catch (...)
	{
	}
}


void POP3ClientSession::setTimeout(const Poco::Timespan& timeout)
{
	_socket.setReceiveTimeout(timeout);
}

	
Poco::Timespan POP3ClientSession::getTimeout() const
{
	return _socket.getReceiveTimeout();
}


void POP3ClientSession::login(const std::string& username, const std::string& password)
{
	std::string response;
	_socket.receiveMessage(response);
	if (!isPositive(response)) throw POP3Exception("The POP3 service is unavailable", response);
	sendCommand("USER", username, response);
	if (!isPositive(response)) throw POP3Exception("Login rejected for user", response);
	sendCommand("PASS", password, response);
	if (!isPositive(response)) throw POP3Exception("Password rejected for user", response);
}


void POP3ClientSession::close()
{
	if (_isOpen)
	{
		std::string response;
		sendCommand("QUIT", response);
		_socket.close();
		_isOpen = false;
	}
}


int POP3ClientSession::messageCount()
{
	std::string response;
	sendCommand("STAT", response);
	if (!isPositive(response)) throw POP3Exception("Cannot determine message count", response);
	std::string::const_iterator it  = response.begin();
	std::string::const_iterator end = response.end();
	int count = 0;
	while (it != end && !Poco::Ascii::isSpace(*it)) ++it;
	while (it != end && Poco::Ascii::isSpace(*it)) ++it;
	while (it != end && Poco::Ascii::isDigit(*it)) count = count*10 + *it++ - '0';
	return count;
}


void POP3ClientSession::listMessages(MessageInfoVec& messages)
{
	messages.clear();
	std::string response;
	sendCommand("LIST", response);
	if (!isPositive(response)) throw POP3Exception("Cannot get message list", response);
	_socket.receiveMessage(response);
	while (response != ".")
	{
		MessageInfo info = {0, 0};
		std::string::const_iterator it  = response.begin();
		std::string::const_iterator end = response.end();
		while (it != end && Poco::Ascii::isDigit(*it)) info.id = info.id*10 + *it++ - '0';
		while (it != end && Poco::Ascii::isSpace(*it)) ++it;
		while (it != end && Poco::Ascii::isDigit(*it)) info.size = info.size*10 + *it++ - '0';
		messages.push_back(info);
		_socket.receiveMessage(response);
	}
}


void POP3ClientSession::retrieveMessage(int id, MailMessage& message)
{
	std::string response;
	sendCommand("RETR", NumberFormatter::format(id), response);
	if (!isPositive(response)) throw POP3Exception("Cannot retrieve message", response);
	DialogInputStream sis(_socket);
	MailInputStream mis(sis);
	message.read(mis);
	while (mis.good()) mis.get(); // read any remaining junk
}


void POP3ClientSession::retrieveMessage(int id, MailMessage& message, PartHandler& handler)
{
	std::string response;
	sendCommand("RETR", NumberFormatter::format(id), response);
	if (!isPositive(response)) throw POP3Exception("Cannot retrieve message", response);
	DialogInputStream sis(_socket);
	MailInputStream mis(sis);
	message.read(mis, handler);
	while (mis.good()) mis.get(); // read any remaining junk
}


void POP3ClientSession::retrieveMessage(int id, std::ostream& ostr)
{
	std::string response;
	sendCommand("RETR", NumberFormatter::format(id), response);
	if (!isPositive(response)) throw POP3Exception("Cannot retrieve message", response);
	DialogInputStream sis(_socket);
	MailInputStream mis(sis);
	StreamCopier::copyStream(mis, ostr);
}


void POP3ClientSession::retrieveHeader(int id, MessageHeader& header)
{
	std::string response;
	sendCommand("TOP", NumberFormatter::format(id), "0", response);
	if (!isPositive(response)) throw POP3Exception("Cannot retrieve header", response);
	DialogInputStream sis(_socket);
	MailInputStream mis(sis);
	header.read(mis);
	// skip stuff following header
	mis.get(); // \r
	mis.get(); // \n
}


void POP3ClientSession::deleteMessage(int id)
{
	std::string response;
	sendCommand("DELE", NumberFormatter::format(id), response);
	if (!isPositive(response)) throw POP3Exception("Cannot mark message for deletion", response);
}


bool POP3ClientSession::sendCommand(const std::string& command, std::string& response)
{
	_socket.sendMessage(command);
	_socket.receiveMessage(response);
	return isPositive(response);
}


bool POP3ClientSession::sendCommand(const std::string& command, const std::string& arg, std::string& response)
{
	_socket.sendMessage(command, arg);
	_socket.receiveMessage(response);
	return isPositive(response);
}


bool POP3ClientSession::sendCommand(const std::string& command, const std::string& arg1, const std::string& arg2, std::string& response)
{
	_socket.sendMessage(command, arg1, arg2);
	_socket.receiveMessage(response);
	return isPositive(response);
}


bool POP3ClientSession::isPositive(const std::string& response)
{
	return response.length() > 0 && response[0] == '+';
}


} } // namespace Poco::Net
