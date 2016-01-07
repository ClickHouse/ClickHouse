//
// Formatter.cpp
//
// $Id: //poco/1.4/Foundation/src/Formatter.cpp#1 $
//
// Library: Foundation
// Package: Logging
// Module:  Formatter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Formatter.h"
#include "Poco/Exception.h"


namespace Poco {


Formatter::Formatter()
{
}


Formatter::~Formatter()
{
}


void Formatter::setProperty(const std::string& name, const std::string& value)
{
	throw PropertyNotSupportedException();
}


std::string Formatter::getProperty(const std::string& name) const
{
	throw PropertyNotSupportedException();
}


} // namespace Poco
