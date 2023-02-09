//
// HexBinaryEncoder.h
//
// Library: Foundation
// Package: Streams
// Module:  HexBinary
//
// Definition of the HexBinaryEncoder class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_HexBinaryEncoder_INCLUDED
#define Foundation_HexBinaryEncoder_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <ostream>


namespace Poco {


class Foundation_API HexBinaryEncoderBuf: public UnbufferedStreamBuf
	/// This streambuf encodes all data written
	/// to it in hexBinary encoding and forwards it to a connected
	/// ostream. 
	/// In hexBinary encoding, each binary octet is encoded as a character tuple,  
	/// consisting of two hexadecimal digits ([0-9a-fA-F]) representing the octet code.
	/// See also: XML Schema Part 2: Datatypes (http://www.w3.org/TR/xmlschema-2/),
	/// section 3.2.15.
	///
	/// Note: The characters are directly written
	/// to the ostream's streambuf, thus bypassing
	/// the ostream. The ostream's state is therefore
	/// not updated to match the buffer's state.
{
public:
	HexBinaryEncoderBuf(std::ostream& ostr);
	~HexBinaryEncoderBuf();
	
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
		
	void setUppercase(bool flag = true);
		/// Specify whether hex digits a-f are written in upper or lower case.
	
private:
	int writeToDevice(char c);

	int _pos;
	int _lineLength;
	int _uppercase;
	std::streambuf& _buf;
};


class Foundation_API HexBinaryEncoderIOS: public virtual std::ios
	/// The base class for HexBinaryEncoder.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	HexBinaryEncoderIOS(std::ostream& ostr);
	~HexBinaryEncoderIOS();
	int close();
	HexBinaryEncoderBuf* rdbuf();

protected:
	HexBinaryEncoderBuf _buf;
};


class Foundation_API HexBinaryEncoder: public HexBinaryEncoderIOS, public std::ostream
	/// This ostream encodes all data
	/// written to it in BinHex encoding and forwards it to
	/// a connected ostream.
	/// Always call close() when done
	/// writing data, to ensure proper
	/// completion of the encoding operation.
	/// In hexBinary encoding, each binary octet is encoded as a character tuple,  
	/// consisting of two hexadecimal digits ([0-9a-fA-F]) representing the octet code.
	/// See also: XML Schema Part 2: Datatypes (http://www.w3.org/TR/xmlschema-2/),
	/// section 3.2.15.
	///
	/// Note: The characters are directly written
	/// to the ostream's streambuf, thus bypassing
	/// the ostream. The ostream's state is therefore
	/// not updated to match the buffer's state.
{
public:
	HexBinaryEncoder(std::ostream& ostr);
	~HexBinaryEncoder();
};


} // namespace Poco


#endif // Foundation_HexBinaryEncoder_INCLUDED
