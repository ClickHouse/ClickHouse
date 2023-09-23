//
// Base32Decoder.h
//
// Library: Foundation
// Package: Streams
// Module:  Base32
//
// Definition of class Base32Decoder.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Base32Decoder_INCLUDED
#define Foundation_Base32Decoder_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <istream>


namespace Poco {


class Foundation_API Base32DecoderBuf: public UnbufferedStreamBuf
	/// This streambuf base32-decodes all data read
	/// from the istream connected to it.
	///
	/// Note: For performance reasons, the characters
	/// are read directly from the given istream's
	/// underlying streambuf, so the state
	/// of the istream will not reflect that of
	/// its streambuf.
{
public:
	Base32DecoderBuf(std::istream& istr);
	~Base32DecoderBuf();

private:
	int readFromDevice();
	int readOne();

	unsigned char   _group[8];
	int             _groupLength;
	int             _groupIndex;
	std::streambuf& _buf;

	static unsigned char IN_ENCODING[256];
	static bool          IN_ENCODING_INIT;

private:
	Base32DecoderBuf(const Base32DecoderBuf&);
	Base32DecoderBuf& operator = (const Base32DecoderBuf&);
};


class Foundation_API Base32DecoderIOS: public virtual std::ios
	/// The base class for Base32Decoder.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	Base32DecoderIOS(std::istream& istr);
	~Base32DecoderIOS();
	Base32DecoderBuf* rdbuf();

protected:
	Base32DecoderBuf _buf;

private:
	Base32DecoderIOS(const Base32DecoderIOS&);
	Base32DecoderIOS& operator = (const Base32DecoderIOS&);
};


class Foundation_API Base32Decoder: public Base32DecoderIOS, public std::istream
	/// This istream base32-decodes all data
	/// read from the istream connected to it.
	///
	/// The class implements RFC 4648 - https://tools.ietf.org/html/rfc4648
	///
	/// Note: For performance reasons, the characters
	/// are read directly from the given istream's
	/// underlying streambuf, so the state
	/// of the istream will not reflect that of
	/// its streambuf.
{
public:
	Base32Decoder(std::istream& istr);
	~Base32Decoder();

private:
	Base32Decoder(const Base32Decoder&);
	Base32Decoder& operator = (const Base32Decoder&);
};


} // namespace Poco


#endif // Foundation_Base32Decoder_INCLUDED
