//
// AutoDetectStream.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/AutoDetectStream.h#1 $
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
#include <ostream>


namespace Poco {
namespace Zip {


class Zip_API AutoDetectStreamBuf: public Poco::BufferedStreamBuf
	/// A AutoDetectStreamBuf is a class that limits one view on an inputstream to a selected view range
{
public:
	AutoDetectStreamBuf(std::istream& in, const std::string& prefix, const std::string& postfix, bool reposition, Poco::UInt32 start);
		/// Creates the AutoDetectStream. 
		

	AutoDetectStreamBuf(std::ostream& out);
		/// Creates the AutoDetectStream. 
		/// If initStream is true the status of the stream will be cleared on the first access, and the stream will be repositioned
		/// to position start

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

	std::istream*  _pIstr;
	std::ostream*  _pOstr;
	bool           _eofDetected;
	int            _matchCnt;
	std::string    _prefix;
	std::string    _postfix;
	bool           _reposition;
	Poco::UInt32   _start;
};


class Zip_API AutoDetectIOS: public virtual std::ios
	/// The base class for AutoDetectInputStream and AutoDetectOutputStream.
	///
	/// This class is needed to ensure the correct initialization
	/// order of the stream buffer and base classes.
{
public:
	AutoDetectIOS(std::istream& istr, const std::string& prefix, const std::string& postfix, bool reposition, Poco::UInt32 start);
		/// Creates the basic stream and connects it
		/// to the given input stream.

	AutoDetectIOS(std::ostream& ostr);
		/// Creates the basic stream and connects it
		/// to the given output stream.

	~AutoDetectIOS();
		/// Destroys the stream.

	AutoDetectStreamBuf* rdbuf();
		/// Returns a pointer to the underlying streambuf.

protected:
	AutoDetectStreamBuf _buf;
};


class Zip_API AutoDetectInputStream: public AutoDetectIOS, public std::istream
	/// This stream copies all characters read through it
	/// to one or multiple output streams.
{
public:
	AutoDetectInputStream(std::istream& istr, const std::string& prefix = std::string(), const std::string& postfix = std::string(), bool reposition = false, Poco::UInt32 start = 0);
		/// Creates the AutoDetectInputStream and connects it
		/// to the given input stream. Bytes read are guaranteed to be in the range [start, end-1]
		/// If initStream is true the status of the stream will be cleared on the first access, and the stream will be repositioned
		/// to position start

	~AutoDetectInputStream();
		/// Destroys the AutoDetectInputStream.
};


class Zip_API AutoDetectOutputStream: public AutoDetectIOS, public std::ostream
	/// This stream copies all characters written to it
	/// to one or multiple output streams.
{
public:
	AutoDetectOutputStream(std::ostream& ostr);
		/// Creates the AutoDetectOutputStream and connects it
		/// to the given input stream. Bytes written are guaranteed to be in the range [start, end-1]
		/// If initStream is true the status of the stream will be cleared on the first access, and the stream will be repositioned
		/// to position start

	~AutoDetectOutputStream();
		/// Destroys the AutoDetectOutputStream.
};


} } // namespace Poco::Zip


#endif // Zip_AutoDetectStream_INCLUDED
