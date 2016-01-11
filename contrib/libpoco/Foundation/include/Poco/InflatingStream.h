//
// InflatingStream.h
//
// $Id: //poco/1.4/Foundation/include/Poco/InflatingStream.h#2 $
//
// Library: Foundation
// Package: Streams
// Module:  ZLibStream
//
// Definition of the InflatingInputStream and InflatingOutputStream classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_InflatingStream_INCLUDED
#define Foundation_InflatingStream_INCLUDED


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


class Foundation_API InflatingStreamBuf: public BufferedStreamBuf
	/// This is the streambuf class used by InflatingInputStream and InflatingOutputStream.
	/// The actual work is delegated to zlib (see http://zlib.net).
	/// Both zlib (deflate) streams and gzip streams are supported.
	/// Output streams should always call close() to ensure
	/// proper completion of decompression.
{
public:
	enum StreamType
	{
		STREAM_ZLIB, /// Expect a zlib header, use Adler-32 checksum.
		STREAM_GZIP, /// Expect a gzip header, use CRC-32 checksum.
		STREAM_ZIP   /// STREAM_ZIP is handled as STREAM_ZLIB, except that we do not check the ADLER32 value (must be checked by caller)
	};

	InflatingStreamBuf(std::istream& istr, StreamType type);
		/// Creates an InflatingStreamBuf for expanding the compressed data read from
		/// the give input stream.

	InflatingStreamBuf(std::istream& istr, int windowBits);
		/// Creates an InflatingStreamBuf for expanding the compressed data read from
		/// the given input stream.
		///
		/// Please refer to the zlib documentation of inflateInit2() for a description
		/// of the windowBits parameter.

	InflatingStreamBuf(std::ostream& ostr, StreamType type);
		/// Creates an InflatingStreamBuf for expanding the compressed data passed through
		/// and forwarding it to the given output stream.

	InflatingStreamBuf(std::ostream& ostr, int windowBits);
		/// Creates an InflatingStreamBuf for expanding the compressed data passed through
		/// and forwarding it to the given output stream.
		///
		/// Please refer to the zlib documentation of inflateInit2() for a description
		/// of the windowBits parameter.

	~InflatingStreamBuf();
		/// Destroys the InflatingStreamBuf.
		
	int close();
		/// Finishes up the stream. 
		///
		/// Must be called when inflating to an output stream.
		
	void reset();
		/// Resets the stream buffer.
		
protected:
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);
	int sync();

private:
	enum 
	{
		STREAM_BUFFER_SIZE  = 1024,
		INFLATE_BUFFER_SIZE = 32768
	};
	
	std::istream*  _pIstr;
	std::ostream*  _pOstr;
	char*    _buffer;
	z_stream _zstr;
	bool     _eof;
	bool     _check;
};


class Foundation_API InflatingIOS: public virtual std::ios
	/// The base class for InflatingOutputStream and InflatingInputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	InflatingIOS(std::ostream& ostr, InflatingStreamBuf::StreamType type = InflatingStreamBuf::STREAM_ZLIB);
		/// Creates an InflatingIOS for expanding the compressed data passed through
		/// and forwarding it to the given output stream.
		
	InflatingIOS(std::ostream& ostr, int windowBits);
		/// Creates an InflatingIOS for expanding the compressed data passed through
		/// and forwarding it to the given output stream.
		///
		/// Please refer to the zlib documentation of inflateInit2() for a description
		/// of the windowBits parameter.

	InflatingIOS(std::istream& istr, InflatingStreamBuf::StreamType type = InflatingStreamBuf::STREAM_ZLIB);
		/// Creates an InflatingIOS for expanding the compressed data read from 
		/// the given input stream.

	InflatingIOS(std::istream& istr, int windowBits);
		/// Creates an InflatingIOS for expanding the compressed data read from 
		/// the given input stream.
		///
		/// Please refer to the zlib documentation of inflateInit2() for a description
		/// of the windowBits parameter.

	~InflatingIOS();
		/// Destroys the InflatingIOS.
		
	InflatingStreamBuf* rdbuf();
		/// Returns a pointer to the underlying stream buffer.
		
protected:
	InflatingStreamBuf _buf;
};


class Foundation_API InflatingOutputStream: public InflatingIOS, public std::ostream
	/// This stream decompresses all data passing through it
	/// using zlib's inflate algorithm.
	///
	/// After all data has been written to the stream, close()
	/// must be called to ensure completion of decompression.
{
public:
	InflatingOutputStream(std::ostream& ostr, InflatingStreamBuf::StreamType type = InflatingStreamBuf::STREAM_ZLIB);
		/// Creates an InflatingOutputStream for expanding the compressed data passed through
		/// and forwarding it to the given output stream.
		
	InflatingOutputStream(std::ostream& ostr, int windowBits);
		/// Creates an InflatingOutputStream for expanding the compressed data passed through
		/// and forwarding it to the given output stream.
		///
		/// Please refer to the zlib documentation of inflateInit2() for a description
		/// of the windowBits parameter.

	~InflatingOutputStream();
		/// Destroys the InflatingOutputStream.
		
	int close();
		/// Finishes up the stream. 
		///
		/// Must be called to ensure all data is properly written to
		/// the target output stream.
};


class Foundation_API InflatingInputStream: public InflatingIOS, public std::istream
	/// This stream decompresses all data passing through it
	/// using zlib's inflate algorithm.
	/// Example:
	///     std::ifstream istr("data.gz", std::ios::binary);
	///     InflatingInputStream inflater(istr, InflatingStreamBuf::STREAM_GZIP);
	///     std::string data;
	///     inflater >> data;
	///
	/// The underlying input stream can contain more than one gzip/deflate stream.
	/// After a gzip/deflate stream has been processed, reset() can be called
	/// to inflate the next stream.
{
public:
	InflatingInputStream(std::istream& istr, InflatingStreamBuf::StreamType type = InflatingStreamBuf::STREAM_ZLIB);
		/// Creates an InflatingInputStream for expanding the compressed data read from 
		/// the given input stream.

	InflatingInputStream(std::istream& istr, int windowBits);
		/// Creates an InflatingInputStream for expanding the compressed data read from 
		/// the given input stream.
		///
		/// Please refer to the zlib documentation of inflateInit2() for a description
		/// of the windowBits parameter.

	~InflatingInputStream();
		/// Destroys the InflatingInputStream.
		
	void reset();
		/// Resets the zlib machinery so that another zlib stream can be read from
		/// the same underlying input stream.
};


} // namespace Poco


#endif // Foundation_InflatingStream_INCLUDED
