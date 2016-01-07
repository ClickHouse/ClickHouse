//
// StringPartSource.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/StringPartSource.h#1 $
//
// Library: Net
// Package: Messages
// Module:  StringPartSource
//
// Definition of the StringPartSource class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_StringPartSource_INCLUDED
#define Net_StringPartSource_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/PartSource.h"
#include <sstream>


namespace Poco {
namespace Net {


class Net_API StringPartSource: public PartSource
	/// An implementation of PartSource for strings.
{
public:
	StringPartSource(const std::string& str);
		/// Creates the StringPartSource for the given string.
		///
		/// The MIME type is set to text/plain.
	
	StringPartSource(const std::string& str, const std::string& mediaType);
		/// Creates the StringPartSource for the given
		/// string and MIME type.

	StringPartSource(const std::string& str, const std::string& mediaType, const std::string& filename);
		/// Creates the StringPartSource for the given
		/// string, MIME type and filename.

	~StringPartSource();
		/// Destroys the StringPartSource.

	std::istream& stream();
		/// Returns a string input stream for the string.
		
	const std::string& filename() const;
		/// Returns the filename portion of the path.

	std::streamsize getContentLength() const;
		/// Returns the string size.

private:
	std::istringstream _istr;
	std::string        _filename;
	
	StringPartSource(const StringPartSource&);
	StringPartSource& operator = (const StringPartSource&);
};


} } // namespace Poco::Net


#endif // Net_StringPartSource_INCLUDED
