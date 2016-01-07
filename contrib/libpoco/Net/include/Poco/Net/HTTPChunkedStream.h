//
// HTTPChunkedStream.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPChunkedStream.h#1 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPChunkedStream
//
// Definition of the HTTPChunkedStream class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPChunkedStream_INCLUDED
#define Net_HTTPChunkedStream_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/HTTPBasicStreamBuf.h"
#include "Poco/MemoryPool.h"
#include <cstddef>
#include <istream>
#include <ostream>


namespace Poco {
namespace Net {


class HTTPSession;


class Net_API HTTPChunkedStreamBuf: public HTTPBasicStreamBuf
	/// This is the streambuf class used for reading and writing
	/// HTTP message bodies in chunked transfer coding.
{
public:
	typedef HTTPBasicStreamBuf::openmode openmode;

	HTTPChunkedStreamBuf(HTTPSession& session, openmode mode);
	~HTTPChunkedStreamBuf();
	void close();

protected:
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);

private:
	HTTPSession&    _session;
	openmode        _mode;
	std::streamsize _chunk;
	std::string     _chunkBuffer;
};


class Net_API HTTPChunkedIOS: public virtual std::ios
	/// The base class for HTTPInputStream.
{
public:
	HTTPChunkedIOS(HTTPSession& session, HTTPChunkedStreamBuf::openmode mode);
	~HTTPChunkedIOS();
	HTTPChunkedStreamBuf* rdbuf();

protected:
	HTTPChunkedStreamBuf _buf;
};


class Net_API HTTPChunkedInputStream: public HTTPChunkedIOS, public std::istream
	/// This class is for internal use by HTTPSession only.
{
public:
	HTTPChunkedInputStream(HTTPSession& session);
	~HTTPChunkedInputStream();
	
	void* operator new(std::size_t size);
	void operator delete(void* ptr);
	
private:
	static Poco::MemoryPool _pool;
};


class Net_API HTTPChunkedOutputStream: public HTTPChunkedIOS, public std::ostream
	/// This class is for internal use by HTTPSession only.
{
public:
	HTTPChunkedOutputStream(HTTPSession& session);
	~HTTPChunkedOutputStream();

	void* operator new(std::size_t size);
	void operator delete(void* ptr);
	
private:
	static Poco::MemoryPool _pool;
};


} } // namespace Poco::Net


#endif // Net_HTTPChunkedStream_INCLUDED
