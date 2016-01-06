//
// SocketStream.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/SocketStream.h#1 $
//
// Library: Net
// Package: Sockets
// Module:  SocketStream
//
// Definition of the SocketStream class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SocketStream_INCLUDED
#define Net_SocketStream_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/BufferedBidirectionalStreamBuf.h"
#include <istream>
#include <ostream>


namespace Poco {
namespace Net {


class StreamSocketImpl;


class Net_API SocketStreamBuf: public Poco::BufferedBidirectionalStreamBuf
	/// This is the streambuf class used for reading from and writing to a socket.
{
public:
	SocketStreamBuf(const Socket& socket);
		/// Creates a SocketStreamBuf with the given socket.
		///
		/// The socket's SocketImpl must be a StreamSocketImpl,
		/// otherwise an InvalidArgumentException is thrown.

	~SocketStreamBuf();
		/// Destroys the SocketStreamBuf.
		
	StreamSocketImpl* socketImpl() const;
		/// Returns the internal SocketImpl.
	
protected:
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);

private:
	enum 
	{
		STREAM_BUFFER_SIZE = 1024
	};

	StreamSocketImpl* _pImpl;
};


class Net_API SocketIOS: public virtual std::ios
	/// The base class for SocketStream, SocketInputStream and
	/// SocketOutputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	SocketIOS(const Socket& socket);
		/// Creates the SocketIOS with the given socket.
		///
		/// The socket's SocketImpl must be a StreamSocketImpl,
		/// otherwise an InvalidArgumentException is thrown.
		
	~SocketIOS();
		/// Destroys the SocketIOS.
		///
		/// Flushes the buffer, but does not close the socket.
		
	SocketStreamBuf* rdbuf();
		/// Returns a pointer to the internal SocketStreamBuf.
		
	void close();
		/// Flushes the stream and closes the socket.
		
	StreamSocket socket() const;
		/// Returns the underlying socket.

protected:
	SocketStreamBuf _buf;
};


class Net_API SocketOutputStream: public SocketIOS, public std::ostream
	/// An output stream for writing to a socket.
{
public:
	explicit SocketOutputStream(const Socket& socket);
		/// Creates the SocketOutputStream with the given socket.
		///
		/// The socket's SocketImpl must be a StreamSocketImpl,
		/// otherwise an InvalidArgumentException is thrown.

	~SocketOutputStream();
		/// Destroys the SocketOutputStream.
		///
		/// Flushes the buffer, but does not close the socket.
};


class Net_API SocketInputStream: public SocketIOS, public std::istream
	/// An input stream for reading from a socket.
	///
	/// When using formatted input from a SocketInputStream,
	/// always ensure that a receive timeout is set for the
	/// socket. Otherwise your program might unexpectedly
	/// hang.
	///
	/// However, using formatted input from a SocketInputStream
	/// is not recommended, due to the read-ahead behavior of
	/// istream with formatted reads.
{
public:
	explicit SocketInputStream(const Socket& socket);
		/// Creates the SocketInputStream with the given socket.
		///
		/// The socket's SocketImpl must be a StreamSocketImpl,
		/// otherwise an InvalidArgumentException is thrown.

	~SocketInputStream();
		/// Destroys the SocketInputStream.
};


class Net_API SocketStream: public SocketIOS, public std::iostream
	/// An bidirectional stream for reading from and writing to a socket.
	///
	/// When using formatted input from a SocketStream,
	/// always ensure that a receive timeout is set for the
	/// socket. Otherwise your program might unexpectedly
	/// hang.
	///
	/// However, using formatted input from a SocketStream
	/// is not recommended, due to the read-ahead behavior of
	/// istream with formatted reads.
{
public:
	explicit SocketStream(const Socket& socket);
		/// Creates the SocketStream with the given socket.
		///
		/// The socket's SocketImpl must be a StreamSocketImpl,
		/// otherwise an InvalidArgumentException is thrown.

	~SocketStream();
		/// Destroys the SocketStream.
		///
		/// Flushes the buffer, but does not close the socket.
};


//
// inlines
//
inline StreamSocketImpl* SocketStreamBuf::socketImpl() const
{
	return _pImpl;
}


} } // namespace Poco::Net


#endif // Net_SocketStream_INCLUDED
