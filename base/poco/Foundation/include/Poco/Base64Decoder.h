//
// Base64Decoder.h
//
// Library: Foundation
// Package: Streams
// Module:  Base64
//
// Definition of class Base64Decoder.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Base64Decoder_INCLUDED
#define Foundation_Base64Decoder_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <istream>


namespace Poco {


class Foundation_API Base64DecoderBuf: public UnbufferedStreamBuf
	/// This streambuf base64-decodes all data read
	/// from the istream connected to it.
	///
	/// Note: For performance reasons, the characters 
	/// are read directly from the given istream's 
	/// underlying streambuf, so the state
	/// of the istream will not reflect that of
	/// its streambuf.
{
public:
	Base64DecoderBuf(std::istream& istr, int options = 0);
	~Base64DecoderBuf();

private:
	int readFromDevice();
	int readOne();

	int             _options;
	unsigned char   _group[3];
	int             _groupLength;
	int             _groupIndex;
	std::streambuf& _buf;
	const unsigned char* _pInEncoding;

	static unsigned char IN_ENCODING[256];
	static bool          IN_ENCODING_INIT;
	static unsigned char IN_ENCODING_URL[256];
	static bool          IN_ENCODING_URL_INIT;

private:
	Base64DecoderBuf(const Base64DecoderBuf&);
	Base64DecoderBuf& operator = (const Base64DecoderBuf&);
};


class Foundation_API Base64DecoderIOS: public virtual std::ios
	/// The base class for Base64Decoder.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	Base64DecoderIOS(std::istream& istr, int options = 0);
	~Base64DecoderIOS();
	Base64DecoderBuf* rdbuf();

protected:
	Base64DecoderBuf _buf;

private:
	Base64DecoderIOS(const Base64DecoderIOS&);
	Base64DecoderIOS& operator = (const Base64DecoderIOS&);
};


class Foundation_API Base64Decoder: public Base64DecoderIOS, public std::istream
	/// This istream base64-decodes all data
	/// read from the istream connected to it.
	///
	/// Note: For performance reasons, the characters 
	/// are read directly from the given istream's 
	/// underlying streambuf, so the state
	/// of the istream will not reflect that of
	/// its streambuf.
{
public:
	Base64Decoder(std::istream& istr, int options = 0);
	~Base64Decoder();

private:
	Base64Decoder(const Base64Decoder&);
	Base64Decoder& operator = (const Base64Decoder&);
};


} // namespace Poco


#endif // Foundation_Base64Decoder_INCLUDED
