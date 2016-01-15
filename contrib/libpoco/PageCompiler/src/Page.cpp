//
// Page.cpp
//
// $Id: //poco/1.4/PageCompiler/src/Page.cpp#2 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Page.h"
#include "Poco/String.h"
#include "Poco/NumberParser.h"


Page::Page()
{
}


Page::~Page()
{
}


bool Page::getBool(const std::string& property, bool deflt) const
{
	if (has(property))
	{
		const std::string& value = get(property);
		return Poco::icompare(value, "true") == 0
		    || Poco::icompare(value, "yes") == 0 
		    || Poco::icompare(value, "on") == 0;
	}
	else return deflt;
}


int Page::getInt(const std::string& property, int deflt) const
{
	if (has(property))
	{
		return Poco::NumberParser::parse(get(property));
	}
	else return deflt;
}
