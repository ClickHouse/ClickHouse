//
// EventException.cpp
//
// Library: XML
// Package: DOM
// Module:  DOMEvents
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DOM/EventException.h"
#include <typeinfo>


namespace Poco {
namespace XML {


EventException::EventException(int code):
	XMLException("Unspecified event type")
{
}


EventException::EventException(const EventException& exc):
	XMLException(exc)
{
}


EventException::~EventException() noexcept
{
}


EventException& EventException::operator = (const EventException& exc)
{
	XMLException::operator = (exc);
	return *this;
}


const char* EventException::name() const noexcept
{
	return "EventException";
}


const char* EventException::className() const noexcept
{
	return typeid(*this).name();
}


Poco::Exception* EventException::clone() const
{
	return new EventException(*this);
}


} } // namespace Poco::XML
