//
// StreamCopier.h
//
// Library: Foundation
// Package: Streams
// Module:  StreamCopier
//
// Definition of class StreamCopier.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_StreamCopier_INCLUDED
#define Foundation_StreamCopier_INCLUDED


#include "Poco/Foundation.h"
#include <istream>
#include <ostream>
#include <cstddef>


namespace Poco {


class Foundation_API StreamCopier
	/// This class provides static methods to copy the contents from one stream
	/// into another.
{
public:
	static std::streamsize copyStream(std::istream& istr, std::ostream& ostr, std::size_t bufferSize = 8192);
		/// Writes all bytes readable from istr to ostr, using an internal buffer.
		///
		/// Returns the number of bytes copied.

#if defined(POCO_HAVE_INT64)
	static Poco::UInt64 copyStream64(std::istream& istr, std::ostream& ostr, std::size_t bufferSize = 8192);
		/// Writes all bytes readable from istr to ostr, using an internal buffer.
		///
		/// Returns the number of bytes copied as a 64-bit unsigned integer.
		///
		/// Note: the only difference to copyStream() is that a 64-bit unsigned
		/// integer is used to count the number of bytes copied.
#endif

	static std::streamsize copyStreamUnbuffered(std::istream& istr, std::ostream& ostr);
		/// Writes all bytes readable from istr to ostr.
		///
		/// Returns the number of bytes copied.

#if defined(POCO_HAVE_INT64)
	static Poco::UInt64 copyStreamUnbuffered64(std::istream& istr, std::ostream& ostr);
		/// Writes all bytes readable from istr to ostr.
		///
		/// Returns the number of bytes copied as a 64-bit unsigned integer.
		///
		/// Note: the only difference to copyStreamUnbuffered() is that a 64-bit unsigned
		/// integer is used to count the number of bytes copied.
#endif

	static std::streamsize copyToString(std::istream& istr, std::string& str, std::size_t bufferSize = 8192);
		/// Appends all bytes readable from istr to the given string, using an internal buffer.
		///
		/// Returns the number of bytes copied.

#if defined(POCO_HAVE_INT64)
	static Poco::UInt64 copyToString64(std::istream& istr, std::string& str, std::size_t bufferSize = 8192);
		/// Appends all bytes readable from istr to the given string, using an internal buffer.
		///
		/// Returns the number of bytes copied as a 64-bit unsigned integer.
		///
		/// Note: the only difference to copyToString() is that a 64-bit unsigned
		/// integer is used to count the number of bytes copied.
#endif
};


} // namespace Poco


#endif // Foundation_StreamCopier_INCLUDED
