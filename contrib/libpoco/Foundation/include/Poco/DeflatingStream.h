//
// DeflatingStream.h
//
// $Id: //poco/1.4/Foundation/include/Poco/DeflatingStream.h#1 $
//
// Library: Foundation
// Package: Streams
// Module:  ZLibStream
//
// Definition of the DeflatingStream class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_DeflatingStream_INCLUDED
#define Foundation_DeflatingStream_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/BufferedStreamBuf.h"
#include <istream>
#include <ostream>
#if defined(POCO_UNBUNDLED)
#include <zlib.h>
#else
#include "Poco/zlib.h"
#endif


namespace Poco {


class Foundation_API DeflatingStreamBuf: public BufferedStreamBuf
	/// This is the streambuf class used by DeflatingInputStream and DeflatingOutputStream.
	/// The actual work is delegated to zlib (see http://zlib.net).
	/// Both zlib (deflate) streams and gzip streams are supported.
	/// Output streams should always call close() to ensure
	/// proper completion of compression.
	/// A compression level (0 to 9) can be specified in the constructor.
{
public:
	enum StreamType
	{
		STREAM_ZLIB, /// Create a zlib header, use Adler-32 checksum.
		STREAM_GZIP  /// Create a gzip header, use CRC-32 checksum.
	};

	DeflatingStreamBuf(std::istream& istr, StreamType type, int level);
		/// Creates a DeflatingStreamBuf for compressing data read
		/// from the given input stream.

	DeflatingStreamBuf(std::istream& istr, int windowBits, int level);
		/// Creates a DeflatingStreamBuf for compressing data read
		/// from the given input stream.
		///
		/// Please refer to the zlib documentation of deflateInit2() for a description
		/// of the windowBits parameter.
		
	DeflatingStreamBuf(std::ostream& ostr, StreamType type, int level);
		/// Creates a DeflatingStreamBuf for compressing data passed
		/// through and forwarding it to the given output stream.

	DeflatingStreamBuf(std::ostream& ostr, int windowBits, int level);
		/// Creates a DeflatingStreamBuf for compressing data passed
		/// through and forwarding it to the given output stream.
		///
		/// Please refer to the zlib documentation of deflateInit2() for a description
		/// of the windowBits parameter.
		
	~DeflatingStreamBuf();
		/// Destroys the DeflatingStreamBuf.
		
	int close();
		/// Finishes up the stream. 
		///
		/// Must be called when deflating to an output stream.

protected:
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);
	virtual int sync();

private:
	enum 
	{
		STREAM_BUFFER_SIZE  = 1024,
		DEFLATE_BUFFER_SIZE = 32768
	};

	std::istream* _pIstr;
	std::ostream* _pOstr;
	char*    _buffer;
	z_stream _zstr;
	bool     _eof;
};


class Foundation_API DeflatingIOS: public virtual std::ios
	/// The base class for DeflatingOutputStream and DeflatingInputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	DeflatingIOS(std::ostream& ostr, DeflatingStreamBuf::StreamType type = DeflatingStreamBuf::STREAM_ZLIB, int level = Z_DEFAULT_COMPRESSION);
		/// Creates a DeflatingIOS for compressing data passed
		/// through and forwarding it to the given output stream.

	DeflatingIOS(std::ostream& ostr, int windowBits, int level);
		/// Creates a DeflatingIOS for compressing data passed
		/// through and forwarding it to the given output stream.
		///
		/// Please refer to the zlib documentation of deflateInit2() for a description
		/// of the windowBits parameter.

	DeflatingIOS(std::istream& istr, DeflatingStreamBuf::StreamType type = DeflatingStreamBuf::STREAM_ZLIB, int level = Z_DEFAULT_COMPRESSION);
		/// Creates a DeflatingIOS for compressing data read
		/// from the given input stream.

	DeflatingIOS(std::istream& istr, int windowBits, int level);
		/// Creates a DeflatingIOS for compressing data read
		/// from the given input stream.
		///
		/// Please refer to the zlib documentation of deflateInit2() for a description
		/// of the windowBits parameter.

	~DeflatingIOS();
		/// Destroys the DeflatingIOS.
		
	DeflatingStreamBuf* rdbuf();
		/// Returns a pointer to the underlying stream buffer.
		
protected:
	DeflatingStreamBuf _buf;
};


class Foundation_API DeflatingOutputStream: public DeflatingIOS, public std::ostream
	/// This stream compresses all data passing through it
	/// using zlib's deflate algorithm.
	/// After all data has been written to the stream, close()
	/// must be called to ensure completion of compression.
	/// Example:
	///     std::ofstream ostr("data.gz", std::ios::binary);
	///     DeflatingOutputStream deflater(ostr, DeflatingStreamBuf::STREAM_GZIP);
	///     deflater << "Hello, world!" << std::endl;
	///     deflater.close();
	///     ostr.close();
{
public:
	DeflatingOutputStream(std::ostream& ostr, DeflatingStreamBuf::StreamType type = DeflatingStreamBuf::STREAM_ZLIB, int level = Z_DEFAULT_COMPRESSION);
		/// Creates a DeflatingOutputStream for compressing data passed
		/// through and forwarding it to the given output stream.

	DeflatingOutputStream(std::ostream& ostr, int windowBits, int level);
		/// Creates a DeflatingOutputStream for compressing data passed
		/// through and forwarding it to the given output stream.
		///
		/// Please refer to the zlib documentation of deflateInit2() for a description
		/// of the windowBits parameter.

	~DeflatingOutputStream();
		/// Destroys the DeflatingOutputStream.
		
	int close();
		/// Finishes up the stream. 
		///
		/// Must be called when deflating to an output stream.

protected:
	virtual int sync();
};


class Foundation_API DeflatingInputStream: public DeflatingIOS, public std::istream
	/// This stream compresses all data passing through it
	/// using zlib's deflate algorithm.
{
public:
	DeflatingInputStream(std::istream& istr, DeflatingStreamBuf::StreamType type = DeflatingStreamBuf::STREAM_ZLIB, int level = Z_DEFAULT_COMPRESSION);
		/// Creates a DeflatingIOS for compressing data read
		/// from the given input stream.

	DeflatingInputStream(std::istream& istr, int windowBits, int level);
		/// Creates a DeflatingIOS for compressing data read
		/// from the given input stream.
		///
		/// Please refer to the zlib documentation of deflateInit2() for a description
		/// of the windowBits parameter.

	~DeflatingInputStream();
		/// Destroys the DeflatingInputStream.
};


} // namespace Poco


#endif // Foundation_DeflatingStream_INCLUDED
