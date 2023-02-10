//
// QuotedPrintableDecoder.h
//
// Library: Net
// Package: Messages
// Module:  QuotedPrintableDecoder
//
// Definition of the QuotedPrintableDecoder class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_QuotedPrintableDecoder_INCLUDED
#define Net_QuotedPrintableDecoder_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <istream>


namespace Poco {
namespace Net {


class Net_API QuotedPrintableDecoderBuf: public Poco::UnbufferedStreamBuf
	/// This streambuf decodes all quoted-printable (see RFC 2045) 
	/// encoded data read from the istream connected to it.
	///
	/// Note: For performance reasons, the characters 
	/// are read directly from the given istream's 
	/// underlying streambuf, so the state
	/// of the istream will not reflect that of
	/// its streambuf.
{
public:
	QuotedPrintableDecoderBuf(std::istream& istr);
	~QuotedPrintableDecoderBuf();
	
private:
	int readFromDevice();

	std::streambuf& _buf;
};


class Net_API QuotedPrintableDecoderIOS: public virtual std::ios
	/// The base class for QuotedPrintableDecoder.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	QuotedPrintableDecoderIOS(std::istream& istr);
	~QuotedPrintableDecoderIOS();
	QuotedPrintableDecoderBuf* rdbuf();

protected:
	QuotedPrintableDecoderBuf _buf;
};


class Net_API QuotedPrintableDecoder: public QuotedPrintableDecoderIOS, public std::istream
	/// This istream decodes all quoted-printable (see RFC 2045)
	/// encoded data read from the istream connected to it.
	///
	/// Note: For performance reasons, the characters 
	/// are read directly from the given istream's 
	/// underlying streambuf, so the state
	/// of the istream will not reflect that of
	/// its streambuf.
{
public:
	QuotedPrintableDecoder(std::istream& istr);
	~QuotedPrintableDecoder();
};


} } // namespace Poco::Net


#endif // Net_QuotedPrintableDecoder_INCLUDED
