//
// DigestStream.h
//
// $Id: //poco/1.4/Foundation/include/Poco/DigestStream.h#1 $
//
// Library: Foundation
// Package: Crypt
// Module:  DigestStream
//
// Definition of classes DigestInputStream and DigestOutputStream.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_DigestStream_INCLUDED
#define Foundation_DigestStream_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/BufferedStreamBuf.h"
#include "Poco/DigestEngine.h"
#include <istream>
#include <ostream>


namespace Poco {


class Foundation_API DigestBuf: public BufferedStreamBuf
	/// This streambuf computes a digest of all data going
	/// through it.
{
public:
	DigestBuf(DigestEngine& eng);
	DigestBuf(DigestEngine& eng, std::istream& istr);
	DigestBuf(DigestEngine& eng, std::ostream& ostr);
	~DigestBuf();	
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);
	void close();

private:
	DigestEngine& _eng;
	std::istream* _pIstr;
	std::ostream* _pOstr;
	static const int BUFFER_SIZE;
};


class Foundation_API DigestIOS: public virtual std::ios
	/// The base class for DigestInputStream and DigestOutputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	DigestIOS(DigestEngine& eng);
	DigestIOS(DigestEngine& eng, std::istream& istr);
	DigestIOS(DigestEngine& eng, std::ostream& ostr);
	~DigestIOS();
	DigestBuf* rdbuf();

protected:
	DigestBuf _buf;
};


class Foundation_API DigestInputStream: public DigestIOS, public std::istream
	/// This istream computes a digest of
	/// all the data passing through it,
	/// using a DigestEngine.
{
public:
	DigestInputStream(DigestEngine& eng, std::istream& istr);
	~DigestInputStream();
};


class Foundation_API DigestOutputStream: public DigestIOS, public std::ostream
	/// This ostream computes a digest of
	/// all the data passing through it,
	/// using a DigestEngine.
	/// To ensure that all data has been incorporated
	/// into the digest, call close() or flush() before 
	/// you obtain the digest from the digest engine.
{
public:
	DigestOutputStream(DigestEngine& eng);
	DigestOutputStream(DigestEngine& eng, std::ostream& ostr);
	~DigestOutputStream();
	void close();
};


} // namespace Poco


#endif // Foundation_DigestStream_INCLUDED
