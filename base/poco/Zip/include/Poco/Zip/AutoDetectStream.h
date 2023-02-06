//
// AutoDetectStream.h
//
// Library: Zip
// Package: Zip
// Module:  AutoDetectStream
//
// Definition of the AutoDetectStream class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_AutoDetectStream_INCLUDED
#define Zip_AutoDetectStream_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/BufferedStreamBuf.h"
#include <istream>


namespace Poco {
namespace Zip {


class Zip_API AutoDetectStreamBuf: public Poco::BufferedStreamBuf
	/// AutoDetectStreamBuf automatically detects the end of a stream using the 
	/// Data Descriptor signature.
{
public:
	AutoDetectStreamBuf(std::istream& in, const std::string& prefix, const std::string& postfix, bool reposition, Poco::UInt32 start, bool needsZip64);
		/// Creates the AutoDetectStream. 
		
	~AutoDetectStreamBuf();
		/// Destroys the AutoDetectStream.

protected:
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);

private:
	enum 
	{
		STREAM_BUFFER_SIZE  = 1024
	};

	std::istream*   _pIstr;
	bool            _eofDetected;
	int             _matchCnt;
	std::string     _prefix;
	std::string     _postfix;
	bool            _reposition;
	Poco::UInt32    _start;
	bool            _needsZip64;
	Poco::UInt64    _length;
};


class Zip_API AutoDetectIOS: public virtual std::ios
	/// The base class for AutoDetectInputStream and AutoDetectOutputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	AutoDetectIOS(std::istream& istr, const std::string& prefix, const std::string& postfix, bool reposition, Poco::UInt32 start, bool needsZip64);
		/// Creates the basic stream and connects it
		/// to the given input stream.

	~AutoDetectIOS();
		/// Destroys the stream.

	AutoDetectStreamBuf* rdbuf();
		/// Returns a pointer to the underlying streambuf.

protected:
	AutoDetectStreamBuf _buf;
};


class Zip_API AutoDetectInputStream: public AutoDetectIOS, public std::istream
	/// AutoDetectInputStream automatically detects the end of a stream using the 
	/// Data Descriptor signature.
{
public:
	AutoDetectInputStream(std::istream& istr, const std::string& prefix = std::string(), const std::string& postfix = std::string(), bool reposition = false, Poco::UInt32 start = 0, bool needsZip64 = false);
		/// Creates the AutoDetectInputStream and connects it to the underlying stream.

	~AutoDetectInputStream();
		/// Destroys the AutoDetectInputStream.
};


} } // namespace Poco::Zip


#endif // Zip_AutoDetectStream_INCLUDED
