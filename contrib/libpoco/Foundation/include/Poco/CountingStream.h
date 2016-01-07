//
// CountingStream.h
//
// $Id: //poco/1.4/Foundation/include/Poco/CountingStream.h#1 $
//
// Library: Foundation
// Package: Streams
// Module:  CountingStream
//
// Definition of the CountingStreamBuf, CountingInputStream and CountingOutputStream classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_CountingStream_INCLUDED
#define Foundation_CountingStream_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <istream>
#include <ostream>


namespace Poco {


class Foundation_API CountingStreamBuf: public UnbufferedStreamBuf
	/// This stream buffer counts all characters and lines
	/// going through it.
{
public:
	CountingStreamBuf();
		/// Creates an unconnected CountingStreamBuf.
	
	CountingStreamBuf(std::istream& istr);
		/// Creates the CountingStreamBuf and connects it
		/// to the given input stream.

	CountingStreamBuf(std::ostream& ostr);
		/// Creates the CountingStreamBuf and connects it
		/// to the given output stream.

	~CountingStreamBuf();
		/// Destroys the CountingStream.
		
	int chars() const;
		/// Returns the total number of characters.
		
	int lines() const;
		/// Returns the total number of lines.
		
	int pos() const;
		/// Returns the number of characters on the current line.
		
	void reset();
		/// Resets all counters.
		
	void setCurrentLineNumber(int line);
		/// Sets the current line number.
		///
		/// This is mainly useful when parsing C/C++
		/// preprocessed source code containing #line directives.
		
	int getCurrentLineNumber() const;
		/// Returns the current line number (same as lines()).

	void addChars(int chars);
		/// Add to the total number of characters.
		
	void addLines(int lines);
		/// Add to the total number of lines.
		
	void addPos(int pos);
		/// Add to the number of characters on the current line.

protected:
	int readFromDevice();
	int writeToDevice(char c);

private:
	std::istream* _pIstr;
	std::ostream* _pOstr;
	int _chars;
	int _lines;
	int _pos;
};


class Foundation_API CountingIOS: public virtual std::ios
	/// The base class for CountingInputStream and CountingOutputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	CountingIOS();
		/// Creates the basic stream and leaves it unconnected.

	CountingIOS(std::istream& istr);
		/// Creates the basic stream and connects it
		/// to the given input stream.

	CountingIOS(std::ostream& ostr);
		/// Creates the basic stream and connects it
		/// to the given output stream.

	~CountingIOS();
		/// Destroys the stream.

	int chars() const;
		/// Returns the total number of characters.

	int lines() const;
		/// Returns the total number of lines.

	int pos() const;
		/// Returns the number of characters on the current line.

	void reset();
		/// Resets all counters.

	void setCurrentLineNumber(int line);
		/// Sets the current line number.
		///
		/// This is mainly useful when parsing C/C++
		/// preprocessed source code containing #line directives.
		
	int getCurrentLineNumber() const;
		/// Returns the current line number (same as lines()).

	void addChars(int chars);
		/// Add to the total number of characters.
		
	void addLines(int lines);
		/// Add to the total number of lines.
		
	void addPos(int pos);
		/// Add to the number of characters on the current line.

	CountingStreamBuf* rdbuf();
		/// Returns a pointer to the underlying streambuf.

protected:
	CountingStreamBuf _buf;
};


class Foundation_API CountingInputStream: public CountingIOS, public std::istream
	/// This stream counts all characters and lines
	/// going through it. This is useful for lexers and parsers
	/// that need to determine the current position in the stream.
{
public:
	CountingInputStream(std::istream& istr);
		/// Creates the CountingInputStream and connects it
		/// to the given input stream.

	~CountingInputStream();
		/// Destroys the stream.
};


class Foundation_API CountingOutputStream: public CountingIOS, public std::ostream
	/// This stream counts all characters and lines
	/// going through it.
{
public:
	CountingOutputStream();
		/// Creates an unconnected CountingOutputStream.
	
	CountingOutputStream(std::ostream& ostr);
		/// Creates the CountingOutputStream and connects it
		/// to the given output stream.

	~CountingOutputStream();
		/// Destroys the CountingOutputStream.
};


//
// inlines
//
inline int CountingStreamBuf::chars() const
{
	return _chars;
}


inline int CountingStreamBuf::lines() const
{
	return _lines;
}


inline int CountingStreamBuf::pos() const
{
	return _pos;
}


inline int CountingStreamBuf::getCurrentLineNumber() const
{
	return _lines;
}


inline int CountingIOS::chars() const
{
	return _buf.chars();
}


inline int CountingIOS::lines() const
{
	return _buf.lines();
}


inline int CountingIOS::pos() const
{
	return _buf.pos();
}


inline int CountingIOS::getCurrentLineNumber() const
{
	return _buf.getCurrentLineNumber();
}


} // namespace Poco


#endif // Foundation_CountingStream_INCLUDED
