//
// FIFOBufferStream.h
//
// Library: Foundation
// Package: Streams
// Module:  FIFOBufferStream
//
// Definition of the FIFOBufferStream class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_FIFOBufferStream_INCLUDED
#define Foundation_FIFOBufferStream_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/FIFOBuffer.h"
#include "Poco/BufferedBidirectionalStreamBuf.h"
#include <istream>
#include <ostream>


namespace Poco {


class Foundation_API FIFOBufferStreamBuf: public BufferedBidirectionalStreamBuf
	/// This is the streambuf class used for reading from and writing to a FIFOBuffer.
	/// FIFOBuffer is enabled for emtpy/non-empty/full state transitions notifications.
{
public:
	
	FIFOBufferStreamBuf();
		/// Creates a FIFOBufferStreamBuf.

	explicit FIFOBufferStreamBuf(FIFOBuffer& fifoBuffer);
		/// Creates a FIFOBufferStreamBuf and assigns the given buffer to it.

	FIFOBufferStreamBuf(char* pBuffer, std::size_t length);
		/// Creates a FIFOBufferStreamBuf and assigns the given buffer to it.

	FIFOBufferStreamBuf(const char* pBuffer, std::size_t length);
		/// Creates a FIFOBufferStreamBuf and assigns the given buffer to it.

	explicit FIFOBufferStreamBuf(std::size_t length);
		/// Creates a FIFOBufferStreamBuf of the given length.

	~FIFOBufferStreamBuf();
		/// Destroys the FIFOBufferStreamBuf.

	FIFOBuffer& fifoBuffer();
		/// Returns the underlying FIFO buffer reference.

protected:
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);

private:
	enum 
	{
		STREAM_BUFFER_SIZE = 1024
	};

	FIFOBuffer* _pFIFOBuffer;
	FIFOBuffer& _fifoBuffer;
};


class Foundation_API FIFOIOS: public virtual std::ios
	/// The base class for FIFOBufferInputStream and
	/// FIFOBufferStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	explicit FIFOIOS(FIFOBuffer& buffer);
		/// Creates a FIFOIOS and assigns the given buffer to it.

	FIFOIOS(char* pBuffer, std::size_t length);
		/// Creates a FIFOIOS and assigns the given buffer to it.

	FIFOIOS(const char* pBuffer, std::size_t length);
		/// Creates a FIFOIOS and assigns the given buffer to it.

	explicit FIFOIOS(std::size_t length);
		/// Creates a FIFOIOS of the given length.
		
	~FIFOIOS();
		/// Destroys the FIFOIOS.
		///
		/// Flushes the buffer.
		
	FIFOBufferStreamBuf* rdbuf();
		/// Returns a pointer to the internal FIFOBufferStreamBuf.
		
	void close();
		/// Flushes the stream.

protected:
	FIFOBufferStreamBuf _buf;
};


class Foundation_API FIFOBufferStream: public FIFOIOS, public std::iostream
	/// An output stream for writing to a FIFO.
{
public:
	Poco::BasicEvent<bool>& readable;
	Poco::BasicEvent<bool>& writable;

	explicit FIFOBufferStream(FIFOBuffer& buffer);
		/// Creates the FIFOBufferStream with supplied buffer as initial value.

	FIFOBufferStream(char* pBuffer, std::size_t length);
		/// Creates a FIFOBufferStream and assigns the given buffer to it.

	FIFOBufferStream(const char* pBuffer, std::size_t length);
		/// Creates a FIFOBufferStream and assigns the given buffer to it.

	explicit FIFOBufferStream(std::size_t length);
		/// Creates a FIFOBufferStream of the given length.

	~FIFOBufferStream();
		/// Destroys the FIFOBufferStream.
		///
		/// Flushes the buffer.

private:
	FIFOBufferStream();
	FIFOBufferStream(const FIFOBufferStream& other);
	FIFOBufferStream& operator =(const FIFOBufferStream& other);
};


///
/// inlines
///


inline FIFOBuffer& FIFOBufferStreamBuf::fifoBuffer()
{
	return _fifoBuffer;
}


} // namespace Poco


#endif // Foundation_FIFOBufferStream_INCLUDED
