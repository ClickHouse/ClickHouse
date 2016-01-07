//
// QuotedPrintableEncoder.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/QuotedPrintableEncoder.h#1 $
//
// Library: Net
// Package: Messages
// Module:  QuotedPrintableEncoder
//
// Definition of the QuotedPrintableEncoder class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_QuotedPrintableEncoder_INCLUDED
#define Net_QuotedPrintableEncoder_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <ostream>


namespace Poco {
namespace Net {


class Net_API QuotedPrintableEncoderBuf: public Poco::UnbufferedStreamBuf
	/// This streambuf encodes all data written
	/// to it in quoted-printable encoding (see RFC 2045)
	/// and forwards it to a connected ostream.
{
public:
	QuotedPrintableEncoderBuf(std::ostream& ostr);
	~QuotedPrintableEncoderBuf();
	int close();
	
private:
	int writeToDevice(char c);
	void writeEncoded(char c);
	void writeRaw(char c);

	int           _pending;
	int           _lineLength;
	std::ostream& _ostr;
};


class Net_API QuotedPrintableEncoderIOS: public virtual std::ios
	/// The base class for QuotedPrintableEncoder.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	QuotedPrintableEncoderIOS(std::ostream& ostr);
	~QuotedPrintableEncoderIOS();
	int close();
	QuotedPrintableEncoderBuf* rdbuf();

protected:
	QuotedPrintableEncoderBuf _buf;
};


class Net_API QuotedPrintableEncoder: public QuotedPrintableEncoderIOS, public std::ostream
	/// This ostream encodes all data
	/// written to it in quoted-printable encoding
	/// (see RFC 2045) and forwards it to a connected ostream.
	/// Always call close() when done
	/// writing data, to ensure proper
	/// completion of the encoding operation.
{
public:
	QuotedPrintableEncoder(std::ostream& ostr);
	~QuotedPrintableEncoder();
};


} } // namespace Poco::Net


#endif // Net_QuotedPrintableEncoder_INCLUDED
