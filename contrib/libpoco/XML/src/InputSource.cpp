//
// InputSource.cpp
//
// $Id: //poco/1.4/XML/src/InputSource.cpp#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SAX/InputSource.h"


namespace Poco {
namespace XML {


InputSource::InputSource():
	_bistr(0),
	_cistr(0)
{
}


InputSource::InputSource(const XMLString& systemId):
	_systemId(systemId),
	_bistr(0),
	_cistr(0)
{
}


InputSource::InputSource(XMLByteInputStream& bistr):
	_bistr(&bistr),
	_cistr(0)
{
}


InputSource::~InputSource()
{
}


void InputSource::setPublicId(const XMLString& publicId)
{
	_publicId = publicId;
}


void InputSource::setSystemId(const XMLString& systemId)
{
	_systemId = systemId;
}


void InputSource::setEncoding(const XMLString& encoding)
{
	_encoding = encoding;
}


void InputSource::setByteStream(XMLByteInputStream& bistr)
{
	_bistr = &bistr;
}


void InputSource::setCharacterStream(XMLCharInputStream& cistr)
{
	_cistr = &cistr;
}


} } // namespace Poco::XML

