//
// HTTPHeaderStream.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPHeaderStream.h#1 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPHeaderStream
//
// Definition of the HTTPHeaderStream class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPHeaderStream_INCLUDED
#define Net_HTTPHeaderStream_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/HTTPBasicStreamBuf.h"
#include "Poco/MemoryPool.h"
#include <cstddef>
#include <istream>
#include <ostream>


namespace Poco {
namespace Net {


class HTTPSession;


class Net_API HTTPHeaderStreamBuf: public HTTPBasicStreamBuf
	/// This is the streambuf class used for reading from a HTTP header
	/// in a HTTPSession.
{
public:
	typedef HTTPBasicStreamBuf::openmode openmode;

	HTTPHeaderStreamBuf(HTTPSession& session, openmode mode);
	~HTTPHeaderStreamBuf();
	
protected:
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);

private:
	HTTPSession& _session;
	bool         _end;
};


class Net_API HTTPHeaderIOS: public virtual std::ios
	/// The base class for HTTPHeaderInputStream.
{
public:
	HTTPHeaderIOS(HTTPSession& session, HTTPHeaderStreamBuf::openmode mode);
	~HTTPHeaderIOS();
	HTTPHeaderStreamBuf* rdbuf();

protected:
	HTTPHeaderStreamBuf _buf;
};


class Net_API HTTPHeaderInputStream: public HTTPHeaderIOS, public std::istream
	/// This class is for internal use by HTTPSession only.
{
public:
	HTTPHeaderInputStream(HTTPSession& session);
	~HTTPHeaderInputStream();

	void* operator new(std::size_t size);
	void operator delete(void* ptr);
	
private:
	static Poco::MemoryPool _pool;
};


class Net_API HTTPHeaderOutputStream: public HTTPHeaderIOS, public std::ostream
	/// This class is for internal use by HTTPSession only.
{
public:
	HTTPHeaderOutputStream(HTTPSession& session);
	~HTTPHeaderOutputStream();

	void* operator new(std::size_t size);
	void operator delete(void* ptr);
	
private:
	static Poco::MemoryPool _pool;
};


} } // namespace Poco::Net


#endif // Net_HTTPHeaderStream_INCLUDED
