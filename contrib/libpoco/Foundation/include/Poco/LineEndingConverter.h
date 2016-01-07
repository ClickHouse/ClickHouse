//
// LineEndingConverter.h
//
// $Id: //poco/1.4/Foundation/include/Poco/LineEndingConverter.h#1 $
//
// Library: Foundation
// Package: Streams
// Module:  LineEndingConverter
//
// Definition of the LineEndingConverter class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_LineEndingConverter_INCLUDED
#define Foundation_LineEndingConverter_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <istream>
#include <ostream>


namespace Poco {


class Foundation_API LineEnding
	/// This class defines valid line ending sequences
	/// for InputLineEndingConverter and OutputLineEndingConverter.
{
public:
	static const std::string NEWLINE_DEFAULT;
	static const std::string NEWLINE_CR;
	static const std::string NEWLINE_CRLF;
	static const std::string NEWLINE_LF;
};


class Foundation_API LineEndingConverterStreamBuf: public UnbufferedStreamBuf
	/// This stream buffer performs line ending conversion
	/// on text streams. The converter can convert from and to 
	/// the Unix (LF), Mac (CR) and DOS/Windows/Network (CF-LF) endings.
	///
	/// Any newline sequence in the source will be replaced by the
	/// target newline sequence.
{
public:
	LineEndingConverterStreamBuf(std::istream& istr);
		/// Creates the LineEndingConverterStreamBuf and connects it
		/// to the given input stream.

	LineEndingConverterStreamBuf(std::ostream& ostr);
		/// Creates the LineEndingConverterStreamBuf and connects it
		/// to the given output stream.

	~LineEndingConverterStreamBuf();
		/// Destroys the LineEndingConverterStream.
		
	void setNewLine(const std::string& newLineCharacters);
		/// Sets the target line ending for the converter.
		///
		/// Possible values are:
		///   * NEWLINE_DEFAULT (whatever is appropriate for the current platform)
		///   * NEWLINE_CRLF    (Windows),
		///   * NEWLINE_LF      (Unix),
		///   * NEWLINE_CR      (Macintosh)
		///
		/// In theory, any character sequence can be used as newline sequence.
		/// In practice, however, only the above three make sense.

	const std::string& getNewLine() const;
		/// Returns the line ending currently in use.

protected:
	int readFromDevice();
	int writeToDevice(char c);

private:
	std::istream*               _pIstr;
	std::ostream*               _pOstr;
	std::string                 _newLine;
	std::string::const_iterator _it;
	char                        _lastChar;
};


class Foundation_API LineEndingConverterIOS: public virtual std::ios
	/// The base class for InputLineEndingConverter and OutputLineEndingConverter.
	///
	/// This class provides common methods and is also needed to ensure 
	/// the correct initialization order of the stream buffer and base classes.
{
public:
	LineEndingConverterIOS(std::istream& istr);
		/// Creates the LineEndingConverterIOS and connects it
		/// to the given input stream.

	LineEndingConverterIOS(std::ostream& ostr);
		/// Creates the LineEndingConverterIOS and connects it
		/// to the given output stream.

	~LineEndingConverterIOS();
		/// Destroys the stream.

	void setNewLine(const std::string& newLineCharacters);
		/// Sets the target line ending for the converter.
		///
		/// Possible values are:
		///   * NEWLINE_DEFAULT (whatever is appropriate for the current platform)
		///   * NEWLINE_CRLF    (Windows),
		///   * NEWLINE_LF      (Unix),
		///   * NEWLINE_CR      (Macintosh)
		///
		/// In theory, any character sequence can be used as newline sequence.
		/// In practice, however, only the above three make sense.
		///
		/// If an empty string is given, all newline characters are removed from
		/// the stream.

	const std::string& getNewLine() const;
		/// Returns the line ending currently in use.

	LineEndingConverterStreamBuf* rdbuf();
		/// Returns a pointer to the underlying streambuf.

protected:
	LineEndingConverterStreamBuf _buf;
};


class Foundation_API InputLineEndingConverter: public LineEndingConverterIOS, public std::istream
	/// InputLineEndingConverter performs line ending conversion
	/// on text input streams. The converter can convert from and to 
	/// the Unix (LF), Mac (CR) and DOS/Windows/Network (CF-LF) endings.
	///
	/// Any newline sequence in the source will be replaced by the
	/// target newline sequence.
{
public:
	InputLineEndingConverter(std::istream& istr);
		/// Creates the LineEndingConverterInputStream and connects it
		/// to the given input stream.

	InputLineEndingConverter(std::istream& istr, const std::string& newLineCharacters);
		/// Creates the LineEndingConverterInputStream and connects it
		/// to the given input stream.

	~InputLineEndingConverter();
		/// Destroys the stream.
};


class Foundation_API OutputLineEndingConverter: public LineEndingConverterIOS, public std::ostream
	/// OutputLineEndingConverter performs line ending conversion
	/// on text output streams. The converter can convert from and to 
	/// the Unix (LF), Mac (CR) and DOS/Windows/Network (CF-LF) endings.
	///
	/// Any newline sequence in the source will be replaced by the
	/// target newline sequence.
{
public:
	OutputLineEndingConverter(std::ostream& ostr);
		/// Creates the LineEndingConverterOutputStream and connects it
		/// to the given input stream.

	OutputLineEndingConverter(std::ostream& ostr, const std::string& newLineCharacters);
		/// Creates the LineEndingConverterOutputStream and connects it
		/// to the given input stream.

	~OutputLineEndingConverter();
		/// Destroys the LineEndingConverterOutputStream.
};


} // namespace Poco


#endif // Foundation_LineEndingConverter_INCLUDED
