//
// SecureSMTPClientSession.h
//
// Library: NetSSL_OpenSSL
// Package: Mail
// Module:  SecureSMTPClientSession
//
// Definition of the SecureSMTPClientSession class.
//
// Copyright (c) 2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SecureSMTPClientSession_INCLUDED
#define Net_SecureSMTPClientSession_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/SMTPClientSession.h"
#include "Poco/Net/Context.h"


namespace Poco {
namespace Net {


class NetSSL_API SecureSMTPClientSession: public SMTPClientSession
	/// This class implements an Simple Mail
	/// Transfer Protocol (SMTP, RFC 2821)
	/// client for sending e-mail messages that
	/// supports the STARTTLS command for secure
	/// connections.
	///
	/// Usage is as follows:
	///   1. Create a SecureSMTPClientSession object.
	///   2. Call login() or login(hostname).
	///   3. Call startTLS() to switch to a secure connection.
	///      Check the return value to see if a secure connection
	///      has actually been established (not all servers may
	///      support STARTTLS).
	///   4. Call any of the login() methods to securely authenticate
	///      with a username and password.
	///   5. Send the message(s).
{
public:
	explicit SecureSMTPClientSession(const StreamSocket& socket);
		/// Creates the SecureSMTPClientSession using
		/// the given socket, which must be connected
		/// to a SMTP server.

	SecureSMTPClientSession(const std::string& host, Poco::UInt16 port = SMTP_PORT);
		/// Creates the SecureSMTPClientSession using a socket connected
		/// to the given host and port.

	virtual ~SecureSMTPClientSession();
		/// Destroys the SMTPClientSession.

	bool startTLS();
		/// Sends a STARTTLS command and, if successful, 
		/// creates a secure SSL/TLS connection over the
		/// existing socket connection.
		///
		/// Must be called after login() or login(hostname).
		/// If successful, login() can be called again
		/// to authenticate the user.
		///
		/// Returns true if the STARTTLS command was successful,
		/// false otherwise.

	bool startTLS(Context::Ptr pContext);
		/// Sends a STARTTLS command and, if successful, 
		/// creates a secure SSL/TLS connection over the
		/// existing socket connection.
		///
		/// Uses the given Context object for creating
		/// the SSL/TLS connection.
		///
		/// Must be called after login() or login(hostname).
		/// If successful, login() can be called again
		/// to authenticate the user.
		///
		/// Returns true if the STARTTLS command was successful,
		/// false otherwise.
		
private:
	std::string _host;
};


} } // namespace Poco::Net


#endif // Net_SecureSMTPClientSession_INCLUDED
