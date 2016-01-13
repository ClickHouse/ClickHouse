//
// Base64Encoder.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Base64Encoder.h#2 $
//
// Library: Foundation
// Package: Streams
// Module:  Base64
//
// Definition of class Base64Encoder.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Base64Encoder_INCLUDED
#define Foundation_Base64Encoder_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <ostream>


namespace Poco {


class Foundation_API Base64EncoderBuf: public UnbufferedStreamBuf
	/// This streambuf base64-encodes all data written
	/// to it and forwards it to a connected
	/// ostream.
	///
	/// Note: The characters are directly written
	/// to the ostream's streambuf, thus bypassing
	/// the ostream. The ostream's state is therefore
	/// not updated to match the buffer's state.
{
public:
	Base64EncoderBuf(std::ostream& ostr);
	~Base64EncoderBuf();
	
	int close();
		/// Closes the stream buffer.

	void setLineLength(int lineLength);
		/// Specify the line length.
		///
		/// After the given number of characters have been written, 
		/// a newline character will be written.
		///
		/// Specify 0 for an unlimited line length.

	int getLineLength() const;
		/// Returns the currently set line length.
	
private:
	int writeToDevice(char c);

	unsigned char   _group[3];
	int             _groupLength;
	int             _pos;
	int             _lineLength;
	std::streambuf& _buf;
	
	static const unsigned char OUT_ENCODING[64];
	
	friend class Base64DecoderBuf;

	Base64EncoderBuf(const Base64EncoderBuf&);
	Base64EncoderBuf& operator = (const Base64EncoderBuf&);
};


class Foundation_API Base64EncoderIOS: public virtual std::ios
	/// The base class for Base64Encoder.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	Base64EncoderIOS(std::ostream& ostr);
	~Base64EncoderIOS();
	int close();
	Base64EncoderBuf* rdbuf();

protected:
	Base64EncoderBuf _buf;

private:
	Base64EncoderIOS(const Base64EncoderIOS&);
	Base64EncoderIOS& operator = (const Base64EncoderIOS&);
};


class Foundation_API Base64Encoder: public Base64EncoderIOS, public std::ostream
	/// This ostream base64-encodes all data
	/// written to it and forwards it to
	/// a connected ostream.
	/// Always call close() when done
	/// writing data, to ensure proper
	/// completion of the encoding operation.
	///
	/// Note: The characters are directly written
	/// to the ostream's streambuf, thus bypassing
	/// the ostream. The ostream's state is therefore
	/// not updated to match the buffer's state.
{
public:
	Base64Encoder(std::ostream& ostr);
	~Base64Encoder();

private:
	Base64Encoder(const Base64Encoder&);
	Base64Encoder& operator = (const Base64Encoder&);
};


} // namespace Poco


#endif // Foundation_Base64Encoder_INCLUDED
