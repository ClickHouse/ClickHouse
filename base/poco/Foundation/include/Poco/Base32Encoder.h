//
// Base32Encoder.h
//
// Library: Foundation
// Package: Streams
// Module:  Base32
//
// Definition of class Base32Encoder.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Base32Encoder_INCLUDED
#define Foundation_Base32Encoder_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <ostream>


namespace Poco {


class Foundation_API Base32EncoderBuf: public UnbufferedStreamBuf
	/// This streambuf base32-encodes all data written
	/// to it and forwards it to a connected
	/// ostream.
	///
	/// Note: The characters are directly written
	/// to the ostream's streambuf, thus bypassing
	/// the ostream. The ostream's state is therefore
	/// not updated to match the buffer's state.
{
public:
	Base32EncoderBuf(std::ostream& ostr, bool padding = true);
	~Base32EncoderBuf();
	
	int close();
		/// Closes the stream buffer.

private:
	int writeToDevice(char c);

	unsigned char   _group[5];
	int             _groupLength;
	std::streambuf& _buf;
	bool		_doPadding;
	
	static const unsigned char OUT_ENCODING[32];
	
	friend class Base32DecoderBuf;

	Base32EncoderBuf(const Base32EncoderBuf&);
	Base32EncoderBuf& operator = (const Base32EncoderBuf&);
};


class Foundation_API Base32EncoderIOS: public virtual std::ios
	/// The base class for Base32Encoder.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	Base32EncoderIOS(std::ostream& ostr, bool padding = true);
	~Base32EncoderIOS();
	int close();
	Base32EncoderBuf* rdbuf();

protected:
	Base32EncoderBuf _buf;

private:
	Base32EncoderIOS(const Base32EncoderIOS&);
	Base32EncoderIOS& operator = (const Base32EncoderIOS&);
};


class Foundation_API Base32Encoder: public Base32EncoderIOS, public std::ostream
	/// This ostream base32-encodes all data
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
	Base32Encoder(std::ostream& ostr, bool padding = true);
	~Base32Encoder();

private:
	Base32Encoder(const Base32Encoder&);
	Base32Encoder& operator = (const Base32Encoder&);
};


} // namespace Poco


#endif // Foundation_Base32Encoder_INCLUDED
