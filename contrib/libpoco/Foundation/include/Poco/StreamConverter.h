//
// StreamConverter.h
//
// $Id: //poco/1.4/Foundation/include/Poco/StreamConverter.h#1 $
//
// Library: Foundation
// Package: Text
// Module:  StreamConverter
//
// Definition of the StreamConverter class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_StreamConverter_INCLUDED
#define Foundation_StreamConverter_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/TextEncoding.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <istream>
#include <ostream>


namespace Poco {


class Foundation_API StreamConverterBuf: public UnbufferedStreamBuf
	/// A StreamConverter converts streams from one encoding (inEncoding)
	/// into another (outEncoding).
	/// If a character cannot be represented in outEncoding, defaultChar
	/// is used instead.
	/// If a byte sequence is not valid in inEncoding, defaultChar is used
	/// instead and the encoding error count is incremented.
{
public:
	StreamConverterBuf(std::istream& istr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar = '?');
		/// Creates the StreamConverterBuf and connects it
		/// to the given input stream.

	StreamConverterBuf(std::ostream& ostr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar = '?');
		/// Creates the StreamConverterBuf and connects it
		/// to the given output stream.

	~StreamConverterBuf();
		/// Destroys the StreamConverterBuf.

	int errors() const;
		/// Returns the number of encoding errors encountered.

protected:
	int readFromDevice();
	int writeToDevice(char c);

private:
	std::istream*       _pIstr;
	std::ostream*       _pOstr;
	const TextEncoding& _inEncoding;
	const TextEncoding& _outEncoding;
	int                 _defaultChar;
	unsigned char       _buffer[TextEncoding::MAX_SEQUENCE_LENGTH];
	int                 _sequenceLength;
	int                 _pos;
	int                 _errors;
};


class Foundation_API StreamConverterIOS: public virtual std::ios
	/// The base class for InputStreamConverter and OutputStreamConverter.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	StreamConverterIOS(std::istream& istr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar = '?');
	StreamConverterIOS(std::ostream& ostr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar = '?');
	~StreamConverterIOS();
	StreamConverterBuf* rdbuf();
	int errors() const;

protected:
	StreamConverterBuf _buf;
};


class Foundation_API InputStreamConverter: public StreamConverterIOS, public std::istream
	/// This stream converts all characters read from the
	/// underlying istream from one character encoding into another.
	/// If a character cannot be represented in outEncoding, defaultChar
	/// is used instead.
	/// If a byte sequence read from the underlying stream is not valid in inEncoding, 
	/// defaultChar is used instead and the encoding error count is incremented.
{
public:
	InputStreamConverter(std::istream& istr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar = '?');
		/// Creates the InputStreamConverter and connects it
		/// to the given input stream.

	~InputStreamConverter();
		/// Destroys the stream.
};


class Foundation_API OutputStreamConverter: public StreamConverterIOS, public std::ostream
	/// This stream converts all characters written to the
	/// underlying ostream from one character encoding into another.
	/// If a character cannot be represented in outEncoding, defaultChar
	/// is used instead.
	/// If a byte sequence written to the stream is not valid in inEncoding, 
	/// defaultChar is used instead and the encoding error count is incremented.
{
public:
	OutputStreamConverter(std::ostream& ostr, const TextEncoding& inEncoding, const TextEncoding& outEncoding, int defaultChar = '?');
		/// Creates the OutputStreamConverter and connects it
		/// to the given input stream.

	~OutputStreamConverter();
		/// Destroys the CountingOutputStream.
};


} // namespace Poco


#endif // Foundation_StreamConverter_INCLUDED
