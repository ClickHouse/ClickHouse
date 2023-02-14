//
// SMTPClientSession.h
//
// Library: Net
// Package: Mail
// Module:  SMTPClientSession
//
// Definition of the SMTPClientSession class.
//
// Copyright (c) 2005-2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SMTPClientSession_INCLUDED
#define Net_SMTPClientSession_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/DialogSocket.h"
#include "Poco/DigestEngine.h"
#include "Poco/Timespan.h"


namespace Poco {
namespace Net {


class MailMessage;


class Net_API SMTPClientSession
	/// This class implements an Simple Mail
	/// Transfer Protocol (SMTP, RFC 2821)
	/// client for sending e-mail messages.
{
public:
	typedef std::vector<std::string> Recipients;

	enum
	{
		SMTP_PORT = 25
	};

	enum LoginMethod
	{
		AUTH_NONE,
		AUTH_CRAM_MD5,
		AUTH_CRAM_SHA1,
		AUTH_LOGIN,
		AUTH_PLAIN,
		AUTH_XOAUTH2 
	};

	explicit SMTPClientSession(const StreamSocket& socket);
		/// Creates the SMTPClientSession using
		/// the given socket, which must be connected
		/// to a SMTP server.

	SMTPClientSession(const std::string& host, Poco::UInt16 port = SMTP_PORT);
		/// Creates the SMTPClientSession using a socket connected
		/// to the given host and port.

	virtual ~SMTPClientSession();
		/// Destroys the SMTPClientSession.

	void setTimeout(const Poco::Timespan& timeout);
		/// Sets the timeout for socket read operations.
		
	Poco::Timespan getTimeout() const;
		/// Returns the timeout for socket read operations.

	void login(const std::string& hostname);
		/// Greets the SMTP server by sending a EHLO command
		/// with the given hostname as argument.
		///
		/// If the server does not understand the EHLO command,
		/// a HELO command is sent instead.
		///
		/// Throws a SMTPException in case of a SMTP-specific error, or a
		/// NetException in case of a general network communication failure.

	void login();
		/// Calls login(hostname) with the current host name.

	void login(const std::string& hostname, LoginMethod loginMethod, const std::string& username, const std::string& password);
		/// Logs in to the SMTP server using the given authentication method and the given
		/// credentials.

	void login(LoginMethod loginMethod, const std::string& username, const std::string& password);
		/// Logs in to the SMTP server using the given authentication method and the given
		/// credentials.
		
	void open();
		/// Reads the initial response from the SMTP server.
		///
		/// Usually called implicitly through login(), but can
		/// also be called explicitly to implement different forms
		/// of SMTP authentication.
		///
		/// Does nothing if called more than once.

	void close();
		/// Sends a QUIT command and closes the connection to the server.	
		///
		/// Throws a SMTPException in case of a SMTP-specific error, or a
		/// NetException in case of a general network communication failure.

	void sendMessage(const MailMessage& message);
		/// Sends the given mail message by sending a MAIL FROM command,
		/// a RCPT TO command for every recipient, and a DATA command with
		/// the message headers and content. Using this function results in
		/// RCPT TO commands list generated from the recipient list supplied
		/// with the message itself.
		///
		/// Throws a SMTPException in case of a SMTP-specific error, or a
		/// NetException in case of a general network communication failure.

	void sendMessage(const MailMessage& message, const Recipients& recipients);
		/// Sends the given mail message by sending a MAIL FROM command,
		/// a RCPT TO command for every recipient, and a DATA command with
		/// the message headers and content. Using this function results in
		/// message header being generated from the supplied recipients list.
		///
		/// Throws a SMTPException in case of a SMTP-specific error, or a
		/// NetException in case of a general network communication failure.

	void sendMessage(std::istream& istr);
		/// Sends the mail message from the supplied stream. Content of the stream
		/// is copied without any checking. Only the completion status is checked and,
		/// if not valid, SMTPExcpetion is thrown.

	int sendCommand(const std::string& command, std::string& response);
		/// Sends the given command verbatim to the server
		/// and waits for a response.
		///
		/// Throws a SMTPException in case of a SMTP-specific error, or a
		/// NetException in case of a general network communication failure.

	int sendCommand(const std::string& command, const std::string& arg, std::string& response);
		/// Sends the given command verbatim to the server
		/// and waits for a response.
		///
		/// Throws a SMTPException in case of a SMTP-specific error, or a
		/// NetException in case of a general network communication failure.

	void sendAddresses(const std::string& from, const Recipients& recipients);
		/// Sends the message preamble by sending a MAIL FROM command,
		/// and a RCPT TO command for every recipient.
		///
		/// Throws a SMTPException in case of a SMTP-specific error, or a
		/// NetException in case of a general network communication failure.

	void sendData();
		/// Sends the message preamble by sending a DATA command.
		///
		/// Throws a SMTPException in case of a SMTP-specific error, or a
		/// NetException in case of a general network communication failure.

protected:
	enum StatusClass
	{
		SMTP_POSITIVE_COMPLETION   = 2,
		SMTP_POSITIVE_INTERMEDIATE = 3,
		SMTP_TRANSIENT_NEGATIVE    = 4,
		SMTP_PERMANENT_NEGATIVE    = 5
	};
	enum
	{
		DEFAULT_TIMEOUT = 30000000 // 30 seconds default timeout for socket operations	
	};

	static bool isPositiveCompletion(int status);
	static bool isPositiveIntermediate(int status);
	static bool isTransientNegative(int status);
	static bool isPermanentNegative(int status);

	void login(const std::string& hostname, std::string& response);
	void loginUsingCRAMMD5(const std::string& username, const std::string& password);
	void loginUsingCRAMSHA1(const std::string& username, const std::string& password);
	void loginUsingCRAM(const std::string& username, const std::string& method, Poco::DigestEngine& hmac);
	void loginUsingLogin(const std::string& username, const std::string& password);
	void loginUsingPlain(const std::string& username, const std::string& password);
	void loginUsingXOAUTH2(const std::string& username, const std::string& password);
	DialogSocket& socket();

private:
	void sendCommands(const MailMessage& message, const Recipients* pRecipients = 0);
	void transportMessage(const MailMessage& message);

	DialogSocket _socket;
	bool         _isOpen;
};


//
// inlines
//
inline bool SMTPClientSession::isPositiveCompletion(int status)
{
	return status/100 == SMTP_POSITIVE_COMPLETION;
}


inline bool SMTPClientSession::isPositiveIntermediate(int status)
{
	return status/100 == SMTP_POSITIVE_INTERMEDIATE;
}


inline bool SMTPClientSession::isTransientNegative(int status)
{
	return status/100 == SMTP_TRANSIENT_NEGATIVE;
}


inline bool SMTPClientSession::isPermanentNegative(int status)
{
	return status/100 == SMTP_PERMANENT_NEGATIVE;
}


inline DialogSocket& SMTPClientSession::socket()
{
	return _socket;
}


} } // namespace Poco::Net


#endif // Net_SMTPClientSession_INCLUDED
