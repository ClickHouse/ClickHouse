//
// StringPartSource.cpp
//
// $Id: //poco/1.4/Net/src/StringPartSource.cpp#1 $
//
// Library: Net
// Package: Messages
// Module:  StringPartSource
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/StringPartSource.h"


namespace Poco {
namespace Net {


StringPartSource::StringPartSource(const std::string& str):
	PartSource("text/plain"),
	_istr(str)
{
}


StringPartSource::StringPartSource(const std::string& str, const std::string& mediaType):
	PartSource(mediaType),
	_istr(str)
{
}


StringPartSource::StringPartSource(const std::string& str, const std::string& mediaType, const std::string& filename):
	PartSource(mediaType),
	_istr(str),
	_filename(filename)
{
}


StringPartSource::~StringPartSource()
{
}


std::istream& StringPartSource::stream()
{
	return _istr;
}

	
const std::string& StringPartSource::filename() const
{
	return _filename;
}


std::streamsize StringPartSource::getContentLength() const
{
	return _istr.str().length();
}


} } // namespace Poco::Net
