//
// LocatorImpl.cpp
//
// $Id: //poco/1.4/XML/src/LocatorImpl.cpp#1 $
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


#include "Poco/SAX/LocatorImpl.h"


namespace Poco {
namespace XML {


LocatorImpl::LocatorImpl()
{
	_lineNumber   = 0;
	_columnNumber = 0;
}


LocatorImpl::LocatorImpl(const Locator& loc)
{
	_publicId     = loc.getPublicId();
	_systemId     = loc.getSystemId();
	_lineNumber   = loc.getLineNumber();
	_columnNumber = loc.getColumnNumber();
}


LocatorImpl::~LocatorImpl()
{
}


LocatorImpl& LocatorImpl::operator = (const Locator& loc)
{
	if (&loc != this)
	{
		_publicId     = loc.getPublicId();
		_systemId     = loc.getSystemId();
		_lineNumber   = loc.getLineNumber();
		_columnNumber = loc.getColumnNumber();
	}
	return *this;
}


XMLString LocatorImpl::getPublicId() const
{
	return _publicId;
}


XMLString LocatorImpl::getSystemId() const
{
	return _systemId;
}


int LocatorImpl::getLineNumber() const
{
	return _lineNumber;
}


int LocatorImpl::getColumnNumber() const
{
	return _columnNumber;
}


void LocatorImpl::setPublicId(const XMLString& publicId)
{
	_publicId = publicId;
}


void LocatorImpl::setSystemId(const XMLString& systemId)
{
	_systemId = systemId;
}


void LocatorImpl::setLineNumber(int lineNumber)
{
	_lineNumber = lineNumber;
}


void LocatorImpl::setColumnNumber(int columnNumber)
{
	_columnNumber = columnNumber;
}


} } // namespace Poco::XML
