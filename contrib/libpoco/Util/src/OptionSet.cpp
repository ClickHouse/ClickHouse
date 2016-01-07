//
// OptionSet.cpp
//
// $Id: //poco/1.4/Util/src/OptionSet.cpp#1 $
//
// Library: Util
// Package: Options
// Module:  OptionSet
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/OptionSet.h"
#include "Poco/Util/OptionException.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Util {


OptionSet::OptionSet()
{
}


OptionSet::OptionSet(const OptionSet& options):
	_options(options._options)
{
}


OptionSet::~OptionSet()
{
}


OptionSet& OptionSet::operator = (const OptionSet& options)
{
	if (&options != this)
		_options = options._options;
	return *this;
}


void OptionSet::addOption(const Option& option)
{
	poco_assert (!option.fullName().empty());
	OptionVec::const_iterator it = _options.begin();
	OptionVec::const_iterator itEnd = _options.end();
	for (; it != itEnd; ++it)
	{
		if (it->fullName() == option.fullName())
		{
			throw DuplicateOptionException(it->fullName());
		}
	}

	_options.push_back(option);
}


bool OptionSet::hasOption(const std::string& name, bool matchShort) const
{
	bool found = false;
	for (Iterator it = _options.begin(); it != _options.end(); ++it)
	{
		if ((matchShort && it->matchesShort(name)) || (!matchShort && it->matchesFull(name)))
		{
			if (!found)
				found = true;
			else
				return false;
		}
	}
	return found;
}

	
const Option& OptionSet::getOption(const std::string& name, bool matchShort) const
{
	const Option* pOption = 0;
	for (Iterator it = _options.begin(); it != _options.end(); ++it)
	{
		if ((matchShort && it->matchesShort(name)) || (!matchShort && it->matchesPartial(name)))
		{
			if (!pOption)
			{
				pOption = &*it;
				if (!matchShort && it->matchesFull(name))
					break;
			}
			else if (!matchShort && it->matchesFull(name))
			{
				pOption = &*it;
				break;
			}
			else throw AmbiguousOptionException(name);
		}
	}
	if (pOption)
		return *pOption;
	else
		throw UnknownOptionException(name);
}


OptionSet::Iterator OptionSet::begin() const
{
	return _options.begin();
}


OptionSet::Iterator OptionSet::end() const
{
	return _options.end();
}


} } // namespace Poco::Util
