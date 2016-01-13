//
// OptionProcessor.cpp
//
// $Id: //poco/1.4/Util/src/OptionProcessor.cpp#2 $
//
// Library: Util
// Package: Options
// Module:  OptionProcessor
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/OptionProcessor.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionException.h"


namespace Poco {
namespace Util {


OptionProcessor::OptionProcessor(const OptionSet& options): 
	_options(options),
	_unixStyle(true),
	_ignore(false)
{
}


OptionProcessor::~OptionProcessor()
{
}


void OptionProcessor::setUnixStyle(bool flag)
{
	_unixStyle = flag;
}


bool OptionProcessor::process(const std::string& argument, std::string& optionName, std::string& optionArg)
{
	optionName.clear();
	optionArg.clear();
	if (!_ignore)
	{
		if (!_deferredOption.empty())
			return processCommon(argument, false, optionName, optionArg);
		else if (_unixStyle)
			return processUnix(argument, optionName, optionArg);
		else
			return processDefault(argument, optionName, optionArg);
	}
	return false;
}


void OptionProcessor::checkRequired() const
{
	for (OptionSet::Iterator it = _options.begin(); it != _options.end(); ++it)
	{
		if (it->required() && _specifiedOptions.find(it->fullName()) == _specifiedOptions.end())
			throw MissingOptionException(it->fullName());
	}
	if (!_deferredOption.empty())
	{
		std::string optionArg;
		const Option& option = _options.getOption(_deferredOption, false);
		option.process(_deferredOption, optionArg); // will throw MissingArgumentException
	}
}


bool OptionProcessor::processUnix(const std::string& argument, std::string& optionName, std::string& optionArg)
{
	std::string::const_iterator it  = argument.begin();
	std::string::const_iterator end = argument.end();
	if (it != end)
	{
		if (*it == '-')
		{
			++it;
			if (it != end)
			{
				if (*it == '-')
				{
					++it;
					if (it == end)
					{
						_ignore = true;
						return true;
					}
					else return processCommon(std::string(it, end), false, optionName, optionArg);
				}
				else return processCommon(std::string(it, end), true, optionName, optionArg);
			}
		}
	}
	return false;
}


bool OptionProcessor::processDefault(const std::string& argument, std::string& optionName, std::string& optionArg)
{
	std::string::const_iterator it  = argument.begin();
	std::string::const_iterator end = argument.end();
	if (it != end)
	{
		if (*it == '/')
		{
			++it;
			return processCommon(std::string(it, end), false, optionName, optionArg);
		}
	}
	return false;
}


bool OptionProcessor::processCommon(const std::string& optionStr, bool isShort, std::string& optionName, std::string& optionArg)
{
	if (!_deferredOption.empty())
	{
		const Option& option = _options.getOption(_deferredOption, false);
		std::string optionWithArg(_deferredOption);
		_deferredOption.clear();
		optionWithArg += '=';
		optionWithArg += optionStr;
		option.process(optionWithArg, optionArg);
		optionName = option.fullName();
		return true;
	}
	if (optionStr.empty()) throw EmptyOptionException();
	const Option& option = _options.getOption(optionStr, isShort);
	const std::string& group = option.group();
	if (!group.empty())
	{
		if (_groups.find(group) != _groups.end())
			throw IncompatibleOptionsException(option.fullName());
		else
			_groups.insert(group);
	}
	if (_specifiedOptions.find(option.fullName()) != _specifiedOptions.end() && !option.repeatable())
		throw DuplicateOptionException(option.fullName());
	_specifiedOptions.insert(option.fullName());
	if (option.argumentRequired() && ((!isShort && optionStr.find_first_of(":=") == std::string::npos) || (isShort && optionStr.length() == option.shortName().length())))
	{
		_deferredOption = option.fullName();
		return true;
	}
	option.process(optionStr, optionArg);
	optionName = option.fullName();
	return true;
}


} } // namespace Poco::Util
