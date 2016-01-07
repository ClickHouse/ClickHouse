//
// SMTPClientSession.cpp
//
// $Id: //poco/1.4/Net/src/SMTPClientSession.cpp#1 $
//
// Library: Net
// Package: Mail
// Module:  SMTPClientSession
//
// Copyright (c) 2005-2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SMTPClientSession.h"
#include "Poco/Net/MailMessage.h"
#include "Poco/Net/MailRecipient.h"
#include "Poco/Net/MailStream.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/SocketStream.h"
#include "Poco/Net/NetException.h"
#include "Poco/Environment.h"
#include "Poco/Net/NetworkInterface.h"
#include "Poco/HMACEngine.h"
#include "Poco/MD5Engine.h"
#include "Poco/SHA1Engine.h"
#include "Poco/DigestStream.h"
#include "Poco/StreamCopier.h"
#include "Poco/Base64Encoder.h"
#include "Poco/Base64Decoder.h"
#include "Poco/String.h"
#include <sstream>
#include <fstream>
#include <iostream>


using Poco::DigestEngine;
using Poco::HMACEngine;
using Poco::MD5Engine;
using Poco::SHA1Engine;
using Poco::DigestOutputStream;
using Poco::StreamCopier;
using Poco::Base64Encoder;
using Poco::Base64Decoder;
using Poco::Environment;


namespace Poco {
namespace Net {


SMTPClientSession::SMTPClientSession(const StreamSocket& socket):
	_socket(socket),
	_isOpen(false)
{
}


SMTPClientSession::SMTPClientSession(const std::string& host, Poco::UInt16 port):
	_socket(SocketAddress(host, port)),
	_isOpen(false)
{
}


SMTPClientSession::~SMTPClientSession()
{
	try
	{
		close();
	}
	catch (...)
	{
	}
}


void SMTPClientSession::setTimeout(const Poco::Timespan& timeout)
{
	_socket.setReceiveTimeout(timeout);
}

	
Poco::Timespan SMTPClientSession::getTimeout() const
{
	return _socket.getReceiveTimeout();
}


void SMTPClientSession::login(const std::string& hostname, std::string& response)
{
	open();
	int status = sendCommand("EHLO", hostname, response);
	if (isPermanentNegative(status))
		status = sendCommand("HELO", hostname, response);
	if (!isPositiveCompletion(status)) throw SMTPException("Login failed", response, status);
}


void SMTPClientSession::login(const std::string& hostname)
{
	std::string response;
	login(hostname, response);
}


void SMTPClientSession::login()
{
	login(Environment::nodeName());
}


void SMTPClientSession::loginUsingCRAMMD5(const std::string& username, const std::string& password)
{
	HMACEngine<MD5Engine> hmac(password);
	loginUsingCRAM(username, "CRAM-MD5", hmac);
}


void SMTPClientSession::loginUsingCRAMSHA1(const std::string& username, const std::string& password)
{
	HMACEngine<SHA1Engine> hmac(password);
	loginUsingCRAM(username, "CRAM-SHA1", hmac);
}


void SMTPClientSession::loginUsingCRAM(const std::string& username, const std::string& method, Poco::DigestEngine& hmac)
{
	std::string response;
	int status = sendCommand(std::string("AUTH ") + method, response);

	if (!isPositiveIntermediate(status)) throw SMTPException(std::string("Cannot authenticate using ") + method, response, status);
	std::string challengeBase64 = response.substr(4);
	
	std::istringstream istr(challengeBase64);
	Base64Decoder decoder(istr);
	std::string challenge;
	StreamCopier::copyToString(decoder, challenge);
	
	hmac.update(challenge);
	
	const DigestEngine::Digest& digest = hmac.digest();
	std::string digestString(DigestEngine::digestToHex(digest));
	
	std::string challengeResponse = username + " " + digestString;
	
	std::ostringstream challengeResponseBase64;
	Base64Encoder encoder(challengeResponseBase64);
	encoder << challengeResponse;
	encoder.close();
	
	status = sendCommand(challengeResponseBase64.str(), response);
  	if (!isPositiveCompletion(status)) throw SMTPException(std::string("Login using ") + method + " failed", response, status);  
}


void SMTPClientSession::loginUsingLogin(const std::string& username, const std::string& password)
{
	std::string response;
	int status = sendCommand("AUTH LOGIN", response);
	if (!isPositiveIntermediate(status)) throw SMTPException("Cannot authenticate using LOGIN", response, status);
	
	std::ostringstream usernameBase64;
	Base64Encoder usernameEncoder(usernameBase64);
	usernameEncoder << username;
	usernameEncoder.close();
	
	std::ostringstream passwordBase64;
	Base64Encoder passwordEncoder(passwordBase64);
	passwordEncoder << password;
	passwordEncoder.close();
	
	//Server request for username/password not defined could be either
	//S: login:
	//C: user_login
	//S: password:
	//C: user_password
	//or
	//S: password:
	//C: user_password
	//S: login:
	//C: user_login
	
	std::string decodedResponse;
	std::istringstream responseStream(response.substr(4));
	Base64Decoder responseDecoder(responseStream);
	StreamCopier::copyToString(responseDecoder, decodedResponse);
	
	if (Poco::icompare(decodedResponse, 0, 8, "username") == 0) // username first (md5("Username:"))
	{
		status = sendCommand(usernameBase64.str(), response);
		if (!isPositiveIntermediate(status)) throw SMTPException("Login using LOGIN username failed", response, status);
		
		status = sendCommand(passwordBase64.str(), response);
		if (!isPositiveCompletion(status)) throw SMTPException("Login using LOGIN password failed", response, status);  
	}
	else if  (Poco::icompare(decodedResponse, 0, 8, "password") == 0) // password first (md5("Password:"))
	{
		status = sendCommand(passwordBase64.str(), response);
		if (!isPositiveIntermediate(status)) throw SMTPException("Login using LOGIN password failed", response, status);  
		
		status = sendCommand(usernameBase64.str(), response);
		if (!isPositiveCompletion(status)) throw SMTPException("Login using LOGIN username failed", response, status);
	}
}

void SMTPClientSession::loginUsingPlain(const std::string& username, const std::string& password)
{
	std::ostringstream credentialsBase64;
	Base64Encoder credentialsEncoder(credentialsBase64);
	credentialsEncoder << '\0' << username << '\0' << password;
	credentialsEncoder.close();

	std::string response;
	int status = sendCommand("AUTH PLAIN", credentialsBase64.str(), response);
	if (!isPositiveCompletion(status)) throw SMTPException("Login using PLAIN failed", response, status);
}


void SMTPClientSession::login(LoginMethod loginMethod, const std::string& username, const std::string& password)
{
	login(Environment::nodeName(), loginMethod, username, password);
}


void SMTPClientSession::login(const std::string& hostname, LoginMethod loginMethod, const std::string& username, const std::string& password)
{
	std::string response;
	login(hostname, response);
	
	if (loginMethod == AUTH_CRAM_MD5)
	{
		if (response.find("CRAM-MD5", 0) != std::string::npos)
		{
			loginUsingCRAMMD5(username, password);
		}
		else throw SMTPException("The mail service does not support CRAM-MD5 authentication", response);
	}
	else if (loginMethod == AUTH_CRAM_SHA1)
	{
		if (response.find("CRAM-SHA1", 0) != std::string::npos)
		{
			loginUsingCRAMSHA1(username, password);
		}
		else throw SMTPException("The mail service does not support CRAM-SHA1 authentication", response);
	}
	else if (loginMethod == AUTH_LOGIN)
	{
		if (response.find("LOGIN", 0) != std::string::npos)
		{
			loginUsingLogin(username, password);
		}
		else throw SMTPException("The mail service does not support LOGIN authentication", response);
	}
	else if (loginMethod == AUTH_PLAIN)
	{
		if (response.find("PLAIN", 0) != std::string::npos)
		{
			loginUsingPlain(username, password);
		}
		else throw SMTPException("The mail service does not support PLAIN authentication", response);
	}
	else if (loginMethod != AUTH_NONE)
	{
		throw SMTPException("The autentication method is not supported");
	}
}


void SMTPClientSession::open()
{
	if (!_isOpen)
	{
		std::string response;
		int status = _socket.receiveStatusMessage(response);
		if (!isPositiveCompletion(status)) throw SMTPException("The mail service is unavailable", response, status);
		_isOpen = true;
	}
}


void SMTPClientSession::close()
{
	if (_isOpen)
	{
		std::string response;
		sendCommand("QUIT", response);
		_socket.close();
		_isOpen = false;
	}
}


void SMTPClientSession::sendCommands(const MailMessage& message, const Recipients* pRecipients)
{
	std::string response;
	int status = 0;
	const std::string& fromField = message.getSender();
	std::string::size_type emailPos = fromField.find('<');
	if (emailPos == std::string::npos)
	{
		std::string sender("<");
		sender.append(fromField);
		sender.append(">");
		status = sendCommand("MAIL FROM:", sender, response);
	}
	else
	{
		status = sendCommand("MAIL FROM:", fromField.substr(emailPos, fromField.size() - emailPos), response);
	}

	if (!isPositiveCompletion(status)) throw SMTPException("Cannot send message", response, status);
	
	std::ostringstream recipient;
	if (pRecipients)
	{
		for (Recipients::const_iterator it = pRecipients->begin(); it != pRecipients->end(); ++it)
		{
			recipient << '<' << *it << '>';
			int status = sendCommand("RCPT TO:", recipient.str(), response);
			if (!isPositiveCompletion(status)) throw SMTPException(std::string("Recipient rejected: ") + recipient.str(), response, status);
			recipient.str("");
		}
	}
	else
	{
		for (MailMessage::Recipients::const_iterator it = message.recipients().begin(); it != message.recipients().end(); ++it)
		{
			recipient << '<' << it->getAddress() << '>';
			int status = sendCommand("RCPT TO:", recipient.str(), response);
			if (!isPositiveCompletion(status)) throw SMTPException(std::string("Recipient rejected: ") + recipient.str(), response, status);
			recipient.str("");
		}
	}

	status = sendCommand("DATA", response);
	if (!isPositiveIntermediate(status)) throw SMTPException("Cannot send message data", response, status);
}


void SMTPClientSession::sendAddresses(const std::string& from, const Recipients& recipients)
{
	std::string response;
	int status = 0;

	std::string::size_type emailPos = from.find('<');
	if (emailPos == std::string::npos)
	{
		std::string sender("<");
		sender.append(from);
		sender.append(">");
		status = sendCommand("MAIL FROM:", sender, response);
	}
	else
	{
		status = sendCommand("MAIL FROM:", from.substr(emailPos, from.size() - emailPos), response);
	}

	if (!isPositiveCompletion(status)) throw SMTPException("Cannot send message", response, status);
	
	std::ostringstream recipient;

	for (Recipients::const_iterator it = recipients.begin(); it != recipients.end(); ++it)
	{

		recipient << '<' << *it << '>';
		int status = sendCommand("RCPT TO:", recipient.str(), response);
		if (!isPositiveCompletion(status)) throw SMTPException(std::string("Recipient rejected: ") + recipient.str(), response, status);
		recipient.str("");
	}
}


void SMTPClientSession::sendData()
{
	std::string response;
	int status = sendCommand("DATA", response);
	if (!isPositiveIntermediate(status)) throw SMTPException("Cannot send message data", response, status);
}


void SMTPClientSession::sendMessage(const MailMessage& message)
{
	sendCommands(message);
	transportMessage(message);
}


void SMTPClientSession::sendMessage(const MailMessage& message, const Recipients& recipients)
{
	sendCommands(message, &recipients);
	transportMessage(message);
}


void SMTPClientSession::transportMessage(const MailMessage& message)
{
	SocketOutputStream socketStream(_socket);
	MailOutputStream mailStream(socketStream);
	message.write(mailStream);
	mailStream.close();
	socketStream.flush();
	
	std::string response;
	int status = _socket.receiveStatusMessage(response);
	if (!isPositiveCompletion(status)) throw SMTPException("The server rejected the message", response, status);
}


int SMTPClientSession::sendCommand(const std::string& command, std::string& response)
{
	_socket.sendMessage(command);
	return _socket.receiveStatusMessage(response);
}


int SMTPClientSession::sendCommand(const std::string& command, const std::string& arg, std::string& response)
{
	_socket.sendMessage(command, arg);
	return _socket.receiveStatusMessage(response);
}


void SMTPClientSession::sendMessage(std::istream& istr)
{
	std::string response;
	int status = 0;
	
	SocketOutputStream socketStream(_socket);
	MailOutputStream mailStream(socketStream);
	StreamCopier::copyStream(istr, mailStream);
	mailStream.close();
	socketStream.flush();
	status = _socket.receiveStatusMessage(response);
	if (!isPositiveCompletion(status)) throw SMTPException("The server rejected the message", response, status);
}


} } // namespace Poco::Net
