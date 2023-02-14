//
// MailStream.h
//
// Library: Net
// Package: Mail
// Module:  MailStream
//
// Definition of the MailStream class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_MailStream_INCLUDED
#define Net_MailStream_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <istream>
#include <ostream>


namespace Poco {
namespace Net {


class Net_API MailStreamBuf: public Poco::UnbufferedStreamBuf
	/// The sole purpose of this stream buffer is to replace 
	/// a "\r\n.\r\n" character sequence with a "\r\n..\r\n" sequence for
	/// output streams and vice-versa for input streams.
	///
	/// This is used when sending mail messages to SMTP servers, or
	/// receiving mail messages from POP servers.
	///
	/// See RFC 2181 (Simple Mail Transfer Protocol) and RFC 1939
	/// (Post Office Protocol - Version 3) for more information.
{
public:
	MailStreamBuf(std::istream& istr);
		/// Creates the MailStreamBuf and connects it
		/// to the given input stream.

	MailStreamBuf(std::ostream& ostr);
		/// Creates the MailStreamBuf and connects it
		/// to the given output stream.

	~MailStreamBuf();
		/// Destroys the MailStreamBuf.
		
	void close();
		/// Writes the terminating period, followed by
		/// CR-LF.
		
protected:
	int readFromDevice();
	int writeToDevice(char c);
	int readOne();

private:
	enum State
	{
		ST_DATA,
		ST_CR,
		ST_CR_LF,
		ST_CR_LF_DOT,
		ST_CR_LF_DOT_DOT,
		ST_CR_LF_DOT_CR,
		ST_CR_LF_DOT_CR_LF
	};
	
	std::istream* _pIstr;
	std::ostream* _pOstr;
	std::string   _buffer;
	State         _state;
};


class Net_API MailIOS: public virtual std::ios
	/// The base class for MailInputStream and MailOutputStream.
	///
	/// This class provides common methods and is also needed to ensure 
	/// the correct initialization order of the stream buffer and base classes.
{
public:
	MailIOS(std::istream& istr);
		/// Creates the MailIOS and connects it
		/// to the given input stream.

	MailIOS(std::ostream& ostr);
		/// Creates the MailIOS and connects it
		/// to the given output stream.

	~MailIOS();
		/// Destroys the stream.

	void close();
		/// Writes the terminating period, followed by
		/// CR-LF.

	MailStreamBuf* rdbuf();
		/// Returns a pointer to the underlying streambuf.

protected:
	MailStreamBuf _buf;
};


class Net_API MailInputStream: public MailIOS, public std::istream
	/// This class is used for reading E-Mail messages from a
	/// POP3 server. All occurrences of "\r\n..\r\n" are replaced with
	/// "\r\n.\r\n". The first occurrence of "\r\n.\r\n" denotes the end
	/// of the stream.
{
public:
	MailInputStream(std::istream& istr);
		/// Creates the MailInputStream and connects it
		/// to the given input stream.

	~MailInputStream();
		/// Destroys the MailInputStream.
};


class Net_API MailOutputStream: public MailIOS, public std::ostream
	/// This class is used for writing E-Mail messages to a
	/// SMTP server. All occurrences of "\r\n.\r\n" are replaced with
	/// "\r\n..\r\n".
{
public:
	MailOutputStream(std::ostream& ostr);
		/// Creates the MailOutputStream and connects it
		/// to the given input stream.

	~MailOutputStream();
		/// Destroys the MailOutputStream.
};


} } // namespace Poco::Net


#endif // Net_MailStream_INCLUDED
