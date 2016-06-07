//
// HexBinaryDecoder.h
//
// $Id: //poco/1.4/Foundation/include/Poco/HexBinaryDecoder.h#2 $
//
// Library: Foundation
// Package: Streams
// Module:  HexBinary
//
// Definition of the HexBinaryDecoder class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_HexBinaryDecoder_INCLUDED
#define Foundation_HexBinaryDecoder_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <istream>


namespace Poco {


class Foundation_API HexBinaryDecoderBuf: public UnbufferedStreamBuf
	/// This streambuf decodes all hexBinary-encoded data read
	/// from the istream connected to it.
	/// In hexBinary encoding, each binary octet is encoded as a character tuple,  
	/// consisting of two hexadecimal digits ([0-9a-fA-F]) representing the octet code.
	/// See also: XML Schema Part 2: Datatypes (http://www.w3.org/TR/xmlschema-2/),
	/// section 3.2.15.
	///
	/// Note: For performance reasons, the characters 
	/// are read directly from the given istream's 
	/// underlying streambuf, so the state
	/// of the istream will not reflect that of
	/// its streambuf.
{
public:
	HexBinaryDecoderBuf(std::istream& istr);
	~HexBinaryDecoderBuf();
	
private:
	int readFromDevice();
	int readOne();

	std::streambuf& _buf;
};


class Foundation_API HexBinaryDecoderIOS: public virtual std::ios
	/// The base class for HexBinaryDecoder.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	HexBinaryDecoderIOS(std::istream& istr);
	~HexBinaryDecoderIOS();
	HexBinaryDecoderBuf* rdbuf();

protected:
	HexBinaryDecoderBuf _buf;
};


class Foundation_API HexBinaryDecoder: public HexBinaryDecoderIOS, public std::istream
	/// This istream decodes all hexBinary-encoded data read
	/// from the istream connected to it.
	/// In hexBinary encoding, each binary octet is encoded as a character tuple,  
	/// consisting of two hexadecimal digits ([0-9a-fA-F]) representing the octet code.
	/// See also: XML Schema Part 2: Datatypes (http://www.w3.org/TR/xmlschema-2/),
	/// section 3.2.15.
	///
	/// Note: For performance reasons, the characters 
	/// are read directly from the given istream's 
	/// underlying streambuf, so the state
	/// of the istream will not reflect that of
	/// its streambuf.
{
public:
	HexBinaryDecoder(std::istream& istr);
	~HexBinaryDecoder();
};


} // namespace Poco


#endif // Foundation_HexBinaryDecoder_INCLUDED
