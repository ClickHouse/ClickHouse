//
// TeeStream.h
//
// $Id: //poco/1.4/Foundation/include/Poco/TeeStream.h#1 $
//
// Library: Foundation
// Package: Streams
// Module:  TeeStream
//
// Definition of the TeeStream class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_TeeStream_INCLUDED
#define Foundation_TeeStream_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <vector>
#include <istream>
#include <ostream>


namespace Poco {


class Foundation_API TeeStreamBuf: public UnbufferedStreamBuf
	/// This stream buffer copies all data written to or
	/// read from it to one or multiple output streams.
{
public:
	TeeStreamBuf();
		/// Creates an unconnected CountingStreamBuf.
		/// Use addStream() to attach output streams.
	
	TeeStreamBuf(std::istream& istr);
		/// Creates the CountingStreamBuf and connects it
		/// to the given input stream.

	TeeStreamBuf(std::ostream& ostr);
		/// Creates the CountingStreamBuf and connects it
		/// to the given output stream.

	~TeeStreamBuf();
		/// Destroys the CountingStream.

	void addStream(std::ostream& ostr);
		/// Adds the given output stream.

protected:
	int readFromDevice();
	int writeToDevice(char c);

private:
	typedef std::vector<std::ostream*> StreamVec;
	
	std::istream* _pIstr;
	StreamVec     _streams;
};


class Foundation_API TeeIOS: public virtual std::ios
	/// The base class for TeeInputStream and TeeOutputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	TeeIOS();
		/// Creates the basic stream and leaves it unconnected.

	TeeIOS(std::istream& istr);
		/// Creates the basic stream and connects it
		/// to the given input stream.

	TeeIOS(std::ostream& ostr);
		/// Creates the basic stream and connects it
		/// to the given output stream.

	~TeeIOS();
		/// Destroys the stream.

	void addStream(std::ostream& ostr);
		/// Adds the given output stream.

	TeeStreamBuf* rdbuf();
		/// Returns a pointer to the underlying streambuf.

protected:
	TeeStreamBuf _buf;
};


class Foundation_API TeeInputStream: public TeeIOS, public std::istream
	/// This stream copies all characters read through it
	/// to one or multiple output streams.
{
public:
	TeeInputStream(std::istream& istr);
		/// Creates the TeeInputStream and connects it
		/// to the given input stream.

	~TeeInputStream();
		/// Destroys the TeeInputStream.
};


class Foundation_API TeeOutputStream: public TeeIOS, public std::ostream
	/// This stream copies all characters written to it
	/// to one or multiple output streams.
{
public:
	TeeOutputStream();
		/// Creates an unconnected TeeOutputStream.
	
	TeeOutputStream(std::ostream& ostr);
		/// Creates the TeeOutputStream and connects it
		/// to the given input stream.

	~TeeOutputStream();
		/// Destroys the TeeOutputStream.
};


} // namespace Poco


#endif // Foundation_TeeStream_INCLUDED
