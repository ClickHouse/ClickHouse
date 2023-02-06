//
// ApacheStream.h
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ApacheConnector_ApacheStream_INCLUDED
#define ApacheConnector_ApacheStream_INCLUDED


#include "ApacheConnector.h"
#include "Poco/BufferedStreamBuf.h"
#include <istream>
#include <ostream>


class ApacheStreamBuf: public Poco::BufferedStreamBuf
	/// This is the streambuf class used for reading from and writing to a socket.
{
public:
	ApacheStreamBuf(ApacheRequestRec* pApacheRequest, bool haveData = false);
		/// Creates a ApacheStreamBuf with the given socket.

	~ApacheStreamBuf();
		/// Destroys the SocketStreamBuf.

protected:
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);

private:
	enum 
	{
		STREAM_BUFFER_SIZE = 1024
	};

	ApacheRequestRec* _pApacheRequest;
	bool  _haveData;
};


class ApacheIOS: public virtual std::ios
	/// The base class for ApacheStream, ApacheInputStream and
	/// ApacheOutputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	ApacheIOS(ApacheRequestRec* pApacheRequest, bool haveData = false);
		/// Creates the ApacheIOS with the given socket.
		
	~ApacheIOS();
		/// Destroys the ApacheIOS.
		///
		/// Flushes the buffer, but does not close the socket.
		
	ApacheStreamBuf* rdbuf();
		/// Returns a pointer to the internal ApacheStreamBuf.
		
	void close();
		/// Flushes the stream.

protected:
	ApacheStreamBuf _buf;
};


class ApacheOutputStream: public ApacheIOS, public std::ostream
	/// An output stream for writing to an Apache response.
{
public:
	ApacheOutputStream(ApacheRequestRec* pApacheRequest);
		/// Creates the ApacheOutputStream with the given socket.

	~ApacheOutputStream();
		/// Destroys the ApacheOutputStream.
		///
		/// Flushes the buffer.
};


class ApacheInputStream: public ApacheIOS, public std::istream
	/// An input stream for reading from an Apache request.
	///
	/// Using formatted input from a ApacheInputStream
	/// is not recommended, due to the read-ahead behavior of
	/// istream with formatted reads.
{
public:
	ApacheInputStream(ApacheRequestRec* pApacheRequest);
		/// Creates the ApacheInputStream with the given socket.

	~ApacheInputStream();
		/// Destroys the ApacheInputStream.
};


#endif // ApacheConnector_ApacheStream_INCLUDED
