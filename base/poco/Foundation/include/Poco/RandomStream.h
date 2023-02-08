//
// RandomStream.h
//
// Library: Foundation
// Package: Crypt
// Module:  RandomStream
//
// Definition of class RandomInputStream.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_RandomStream_INCLUDED
#define Foundation_RandomStream_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/BufferedStreamBuf.h"
#include <istream>


namespace Poco {


class Foundation_API RandomBuf: public BufferedStreamBuf
	/// This streambuf generates random data.
	/// On Windows NT, the cryptographic API is used.
	/// On Unix, /dev/random is used, if available.
	/// Otherwise, a random number generator, some
	/// more-or-less random data and a SHA-1 digest
	/// is used to generate random data.
{
public:
	RandomBuf();
	~RandomBuf();
	int readFromDevice(char* buffer, std::streamsize length);
};


class Foundation_API RandomIOS: public virtual std::ios
	/// The base class for RandomInputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	RandomIOS();
	~RandomIOS();
	RandomBuf* rdbuf();

protected:
	RandomBuf _buf;
};


class Foundation_API RandomInputStream: public RandomIOS, public std::istream
	/// This istream generates random data
	/// using the RandomBuf.
{
public:
	RandomInputStream();
	~RandomInputStream();
};


} // namespace Poco


#endif // Foundation_RandomStream_INCLUDED
