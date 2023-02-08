//
// NullStream.h
//
// Library: Foundation
// Package: Streams
// Module:  NullStream
//
// Definition of the NullStreamBuf, NullInputStream and NullOutputStream classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NullStream_INCLUDED
#define Foundation_NullStream_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnbufferedStreamBuf.h"
#include <istream>
#include <ostream>


namespace Poco {


class Foundation_API NullStreamBuf: public UnbufferedStreamBuf
	/// This stream buffer discards all characters written to it.
	/// Any read operation immediately yields EOF.
{
public:
	NullStreamBuf();
		/// Creates a NullStreamBuf.
	
	~NullStreamBuf();
		/// Destroys the NullStreamBuf.
		
protected:
	int readFromDevice();
	int writeToDevice(char c);
};


class Foundation_API NullIOS: public virtual std::ios
	/// The base class for NullInputStream and NullOutputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	NullIOS();
	~NullIOS();

protected:
	NullStreamBuf _buf;
};


class Foundation_API NullInputStream: public NullIOS, public std::istream
	/// Any read operation from this stream immediately
	/// yields EOF.
{
public:
	NullInputStream();
		/// Creates the NullInputStream.

	~NullInputStream();
		/// Destroys the NullInputStream.
};


class Foundation_API NullOutputStream: public NullIOS, public std::ostream
	/// This stream discards all characters written to it.
{
public:
	NullOutputStream();
		/// Creates the NullOutputStream.
	
	~NullOutputStream();
		/// Destroys the NullOutputStream.
};


} // namespace Poco


#endif // Foundation_NullStream_INCLUDED
