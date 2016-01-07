//
// RegularExpression.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  RegularExpression
//
// Implementation of the RegularExpression class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/RegularExpression.h"
#include <sstream>


namespace Poco {
namespace MongoDB {


RegularExpression::RegularExpression()
{
}


RegularExpression::RegularExpression(const std::string& pattern, const std::string& options) : _pattern(pattern), _options(options)
{
}


RegularExpression::~RegularExpression()
{
}


SharedPtr<Poco::RegularExpression> RegularExpression::createRE() const
{
	int options = 0;
	for(std::string::const_iterator optIt = _options.begin(); optIt != _options.end(); ++optIt)
	{
		switch(*optIt)
		{
		case 'i': // Case Insensitive
			options |= Poco::RegularExpression::RE_CASELESS;
			break;
		case 'm': // Multiline matching
			options |= Poco::RegularExpression::RE_MULTILINE;
			break;
		case 'x': // Verbose mode
			//No equivalent in Poco
			break;
		case 'l': // \w \W Locale dependent
			//No equivalent in Poco
			break;
		case 's': // Dotall mode
			options |= Poco::RegularExpression::RE_DOTALL;
			break;
		case 'u': // \w \W Unicode
			//No equivalent in Poco
			break;
		}
	}
	return new Poco::RegularExpression(_pattern, options);
}


} } // namespace Poco::MongoDB
