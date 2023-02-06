//
// ZipStream.h
//
// Library: Zip
// Package: Zip
// Module:  ZipStream
//
// Definition of the ZipStream class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_ZipStream_INCLUDED
#define Zip_ZipStream_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/PartialStream.h"
#include "Poco/SharedPtr.h"
#include "Poco/BufferedStreamBuf.h"
#include "Poco/Checksum.h"
#include <istream>
#include <ostream>


namespace Poco {
namespace Zip {


class ZipArchive;
class ZipLocalFileHeader;


class Zip_API ZipStreamBuf: public Poco::BufferedStreamBuf
	/// ZipStreamBuf is used to decompress single files from a Zip file.
{
public:
	ZipStreamBuf(std::istream& istr, const ZipLocalFileHeader& fileEntry, bool reposition);
		/// Creates the ZipStreamBuf. Set reposition to false, if you do on-the-fly decompression.
	
	ZipStreamBuf(std::ostream& ostr, ZipLocalFileHeader& fileEntry, bool reposition);
		/// Creates the ZipStreamBuf. Set reposition to false, if you do on-the-fly compression.

	virtual ~ZipStreamBuf();
		/// Destroys the ZipStreamBuf.

	void close(Poco::UInt64& extraDataSize);
		/// Informs a writing outputstream that writing is done for this stream

	bool crcValid() const;
		/// Call this method once all bytes were read from the input stream to determine if the CRC is valid

protected:
	int readFromDevice(char* buffer, std::streamsize length);

	int writeToDevice(const char* buffer, std::streamsize length);

private:
	enum
	{
		STREAM_BUFFER_SIZE = 1024
	};

	typedef Poco::SharedPtr<std::istream> PtrIStream;
	typedef Poco::SharedPtr<std::ostream> PtrOStream;
	std::istream*  _pIstr;
	std::ostream*  _pOstr;
	PtrIStream     _ptrBuf;
	PtrOStream     _ptrOBuf;
	PtrIStream     _ptrHelper;
	Poco::SharedPtr<PartialOutputStream> _ptrOHelper;
	Poco::Checksum _crc32;
	Poco::UInt32   _expectedCrc32;
	bool           _checkCRC;
		/// Note: we do not check crc if we decompress a streaming zip file and the crc is stored in the directory header
	Poco::UInt64   _bytesWritten;
	ZipLocalFileHeader* _pHeader;
};


class Zip_API ZipIOS: public virtual std::ios
	/// The base class for ZipInputStream and ZipOutputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	ZipIOS(std::istream& istr, const ZipLocalFileHeader& fileEntry, bool reposition);
		/// Creates the basic stream and connects it
		/// to the given input stream.

	ZipIOS(std::ostream& ostr, ZipLocalFileHeader& fileEntry, bool reposition);
		/// Creates the basic stream and connects it
		/// to the given output stream.

	~ZipIOS();
		/// Destroys the stream.

	ZipStreamBuf* rdbuf();
		/// Returns a pointer to the underlying streambuf.

protected:
	ZipStreamBuf _buf;
};


class Zip_API ZipInputStream: public ZipIOS, public std::istream
	/// This stream copies all characters read through it
	/// to one or multiple output streams.
{
public:
	ZipInputStream(std::istream& istr, const ZipLocalFileHeader& fileEntry, bool reposition = true);
		/// Creates the ZipInputStream and connects it
		/// to the given input stream.

	~ZipInputStream();
		/// Destroys the ZipInputStream.

	bool crcValid() const;
		/// Call this method once all bytes were read from the input stream to determine if the CRC is valid
};



class Zip_API ZipOutputStream: public ZipIOS, public std::ostream
	/// This stream compresses all characters written through it
	/// to one output stream.
{
public:
	ZipOutputStream(std::ostream& ostr, ZipLocalFileHeader& fileEntry, bool seekableOutput);
		/// Creates the ZipOutputStream and connects it
		/// to the given output stream.

	~ZipOutputStream();
		/// Destroys the ZipOutputStream.

	void close(Poco::UInt64& extraDataSize);
		/// Must be called for ZipOutputStreams!
};


} } // namespace Poco::Zip


#endif // Zip_ZipStream_INCLUDED
