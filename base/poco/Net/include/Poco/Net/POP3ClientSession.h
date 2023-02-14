//
// POP3ClientSession.h
//
// Library: Net
// Package: Mail
// Module:  POP3ClientSession
//
// Definition of the POP3ClientSession class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_POP3ClientSession_INCLUDED
#define Net_POP3ClientSession_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/DialogSocket.h"
#include "Poco/Timespan.h"
#include <ostream>
#include <vector>


namespace Poco {
namespace Net {


class MessageHeader;
class MailMessage;
class PartHandler;


class Net_API POP3ClientSession
	/// This class implements an Post Office Protocol
	/// Version 3 (POP3, RFC 1939)
	/// client for receiving e-mail messages.
{
public:
	enum
	{
		POP3_PORT = 110
	};
	
	struct MessageInfo
		/// Information returned by listMessages().
	{
		int id;
		int size;
	};
	
	typedef std::vector<MessageInfo> MessageInfoVec;

	explicit POP3ClientSession(const StreamSocket& socket);
		/// Creates the POP3ClientSession using
		/// the given socket, which must be connected
		/// to a POP3 server.

	POP3ClientSession(const std::string& host, Poco::UInt16 port = POP3_PORT);
		/// Creates the POP3ClientSession using a socket connected
		/// to the given host and port.

	virtual ~POP3ClientSession();
		/// Destroys the SMTPClientSession.

	void setTimeout(const Poco::Timespan& timeout);
		/// Sets the timeout for socket read operations.
		
	Poco::Timespan getTimeout() const;
		/// Returns the timeout for socket read operations.

	void login(const std::string& username, const std::string& password);
		/// Logs in to the POP3 server by sending a USER command
		/// followed by a PASS command.
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	void close();
		/// Sends a QUIT command and closes the connection to the server.	
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	int messageCount();
		/// Sends a STAT command to determine the number of messages
		/// available on the server and returns that number.
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	void listMessages(MessageInfoVec& messages);
		/// Fills the given vector with the ids and sizes of all
		/// messages available on the server.
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	void retrieveMessage(int id, MailMessage& message);
		/// Retrieves the message with the given id from the server and
		/// stores the raw message content in the message's
		/// content string, available with message.getContent().
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	void retrieveMessage(int id, MailMessage& message, PartHandler& handler);
		/// Retrieves the message with the given id from the server and
		/// stores it in message.
		///
		/// If the message has multiple parts, the parts
		/// are reported to the PartHandler. If the message
		/// is not a multi-part message, the content is stored
		/// in a string available by calling message.getContent().
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	void retrieveMessage(int id, std::ostream& ostr);
		/// Retrieves the raw message with the given id from the
		/// server and copies it to the given output stream.
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	void retrieveHeader(int id, MessageHeader& header);
		/// Retrieves the message header of the message with the
		/// given id and stores it in header.
		///
		/// For this to work, the server must support the TOP command.
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	void deleteMessage(int id);
		/// Marks the message with the given ID for deletion. The message
		/// will be deleted when the connection to the server is
		/// closed by calling close().
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	bool sendCommand(const std::string& command, std::string& response);
		/// Sends the given command verbatim to the server
		/// and waits for a response.
		///
		/// Returns true if the response is positive, false otherwise.
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	bool sendCommand(const std::string& command, const std::string& arg, std::string& response);
		/// Sends the given command verbatim to the server
		/// and waits for a response.
		///
		/// Returns true if the response is positive, false otherwise.
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

	bool sendCommand(const std::string& command, const std::string& arg1, const std::string& arg2, std::string& response);
		/// Sends the given command verbatim to the server
		/// and waits for a response.
		///
		/// Returns true if the response is positive, false otherwise.
		///
		/// Throws a POP3Exception in case of a POP3-specific error, or a
		/// NetException in case of a general network communication failure.

protected:
	static bool isPositive(const std::string& response);
	
private:
	DialogSocket _socket;
	bool         _isOpen;
};


} } // namespace Poco::Net


#endif // Net_POP3ClientSession_INCLUDED
