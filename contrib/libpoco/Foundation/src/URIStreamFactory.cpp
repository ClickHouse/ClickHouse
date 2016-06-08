//
// URIStreamFactory.cpp
//
// $Id: //poco/1.4/Foundation/src/URIStreamFactory.cpp#1 $
//
// Library: Foundation
// Package: URI
// Module:  URIStreamFactory
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/URIStreamFactory.h"
#include <algorithm>


namespace Poco {


URIStreamFactory::URIStreamFactory()
{
}


URIStreamFactory::~URIStreamFactory()
{
}


URIRedirection::URIRedirection(const std::string& uri):
	_uri(uri)
{
}


URIRedirection::URIRedirection(const URIRedirection& redir):
	_uri(redir._uri)
{
}


URIRedirection& URIRedirection::operator = (const URIRedirection& redir)
{
	URIRedirection tmp(redir);
	swap(tmp);
	return *this;
}


void URIRedirection::swap(URIRedirection& redir)
{
	std::swap(_uri, redir._uri);
}


} // namespace Poco
