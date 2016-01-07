//
// Channel.cpp
//
// $Id: //poco/1.4/Foundation/src/Channel.cpp#1 $
//
// Library: Foundation
// Package: Logging
// Module:  Channel
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Channel.h"


namespace Poco {


Channel::Channel()
{
}


Channel::~Channel()
{
}


void Channel::open()
{
}


void Channel::close()
{
}


void Channel::setProperty(const std::string& name, const std::string& value)
{
	throw PropertyNotSupportedException(name);
}


std::string Channel::getProperty(const std::string& name) const
{
	throw PropertyNotSupportedException(name);
}


} // namespace Poco
