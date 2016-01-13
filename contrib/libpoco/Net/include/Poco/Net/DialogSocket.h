//
// DialogSocket.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/DialogSocket.h#2 $
//
// Library: Net
// Package: Sockets
// Module:  DialogSocket
//
// Definition of the DialogSocket class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_DialogSocket_INCLUDED
#define Net_DialogSocket_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/StreamSocket.h"


namespace Poco {
namespace Net {


class Net_API DialogSocket: public StreamSocket
	/// DialogSocket is a subclass of StreamSocket that
	/// can be used for implementing request-response
	/// based client server connections.
	///
	/// A request is always a single-line command terminated
	/// by CR-LF.
	///
	/// A response can either be a single line of text terminated
	/// by CR-LF, or multiple lines of text in the format used
	/// by the FTP and SMTP protocols.
	///
	/// Limited support for the TELNET protocol (RFC 854) is
	/// available.
	///
	/// Warning: Do not call receiveBytes() on a DialogSocket.
	/// Due to internal buffering in DialogSocket, receiveBytes()
	/// may return an unexpected result and interfere with
	/// DialogSocket's buffering. Use receiveRawBytes() instead.
{
public:
	DialogSocket();
		/// Creates an unconnected stream socket.
		///
		/// Before sending or receiving data, the socket
		/// must be connected with a call to connect().

	explicit DialogSocket(const SocketAddress& address);
		/// Creates a stream socket and connects it to
		/// the socket specified by address.

	DialogSocket(const Socket& socket);
		/// Creates the DialogSocket with the SocketImpl
		/// from another socket. The SocketImpl must be
		/// a StreamSocketImpl, otherwise an InvalidArgumentException
		/// will be thrown.

	DialogSocket(const DialogSocket& socket);
		/// Creates the DialogSocket as copy of another dialog socket.

	~DialogSocket();
		/// Destroys the DialogSocket.

	DialogSocket& operator = (const Socket& socket);
		/// Assignment operator.
		///
		/// Releases the socket's SocketImpl and
		/// attaches the SocketImpl from the other socket and
		/// increments the reference count of the SocketImpl.	

	DialogSocket& operator = (const DialogSocket& socket);
		/// Assignment operator.

	void sendByte(unsigned char ch);
		/// Sends a single byte over the socket connection.

	void sendString(const char* str);
		/// Sends the given null-terminated string over
		/// the socket connection.

	void sendString(const std::string& str);
		/// Sends the given string over the socket connection.

	void sendMessage(const std::string& message);
		/// Appends a CR-LF sequence to the message and sends it
		/// over the socket connection.

	void sendMessage(const std::string& message, const std::string& arg);
		/// Concatenates message and arg, separated by a space, appends a
		/// CR-LF sequence, and sends the result over the socket connection.

	void sendMessage(const std::string& message, const std::string& arg1, const std::string& arg2);
		/// Concatenates message and args, separated by a space, appends a
		/// CR-LF sequence, and sends the result over the socket connection.
		
	bool receiveMessage(std::string& message);
		/// Receives a single-line message, terminated by CR-LF,
		/// from the socket connection and appends it to response.
		/// 
		/// Returns true if a message has been read or false if
		/// the connection has been closed by the peer.
	
	int receiveStatusMessage(std::string& message);
		/// Receives a single-line or multi-line response from
		/// the socket connection. The format must be according to
		/// one of the response formats specified in the FTP (RFC 959) 
		/// or SMTP (RFC 2821) specifications.
		///
		/// The first line starts with a 3-digit status code.
		/// Following the status code is either a space character (' ' ) 
		/// (in case of a single-line response) or a minus character ('-')
		/// in case of a multi-line response. The following lines can have
		/// a three-digit status code followed by a minus-sign and some
		/// text, or some arbitrary text only. The last line again begins
		/// with a three-digit status code (which must be the same as the
		/// one in the first line), followed by a space and some arbitrary 
		/// text. All lines must be terminated by a CR-LF sequence.
		///
		/// The response contains all response lines, separated by a newline
		/// character, including the status code. The status code is returned.
		/// If the response line does not contain a status code, 0 is returned.
	
	int get();
		/// Reads one character from the connection.
		///
		/// Returns -1 (EOF_CHAR) if no more characters are available.

	int peek();
		/// Returns the character that would be returned by the next call
		/// to get(), without actually extracting the character from the
		/// buffer.
		///
		/// Returns -1 (EOF_CHAR) if no more characters are available.

	int receiveRawBytes(void* buffer, int length);
		/// Read up to length bytes from the connection and place
		/// them into buffer. If there are data bytes in the internal
		/// buffer, these bytes are returned first.
		///
		/// Use this member function instead of receiveBytes().
		///
		/// Returns the number of bytes read, which may be
		/// less than requested.

	void synch();
		/// Sends a TELNET SYNCH signal over the connection.
		///
		/// According to RFC 854, a TELNET_DM char is sent
		/// via sendUrgent().
		
	void sendTelnetCommand(unsigned char command);
		/// Sends a TELNET command sequence (TELNET_IAC followed
		/// by the given command) over the connection.

	void sendTelnetCommand(unsigned char command, unsigned char arg);
		/// Sends a TELNET command sequence (TELNET_IAC followed
		/// by the given command, followed by arg) over the connection.

	enum TelnetCodes
	{
		TELNET_SE   = 240,
		TELNET_NOP  = 241,
		TELNET_DM   = 242,
		TELNET_BRK  = 243,
		TELNET_IP   = 244,
		TELNET_AO   = 245,
		TELNET_AYT  = 246,
		TELNET_EC   = 247,
		TELNET_EL   = 248,
		TELNET_GA   = 249,
		TELNET_SB   = 250,
		TELNET_WILL = 251,
		TELNET_WONT = 252,
		TELNET_DO   = 253,
		TELNET_DONT = 254,
		TELNET_IAC  = 255
	};

protected:
	void allocBuffer();
	void refill();
	bool receiveLine(std::string& line);
	int receiveStatusLine(std::string& line);

private:
	enum
	{
		RECEIVE_BUFFER_SIZE = 1024,
		EOF_CHAR            = -1
	};
	
	char* _pBuffer;
	char* _pNext;
	char* _pEnd;
};


} } // namespace Poco::Net


#endif // Net_DialogSocket_INCLUDED
