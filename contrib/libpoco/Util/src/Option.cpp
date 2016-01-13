//
// Option.cpp
//
// $Id: //poco/1.4/Util/src/Option.cpp#1 $
//
// Library: Util
// Package: Options
// Module:  Option
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/Option.h"
#include "Poco/Util/OptionException.h"
#include "Poco/Util/Validator.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/String.h"
#include <algorithm>


using Poco::icompare;


namespace Poco {
namespace Util {


Option::Option(): 
	_required(false), 
	_repeatable(false), 
	_argRequired(false),
	_pValidator(0),
	_pCallback(0),
	_pConfig(0)
{
}


Option::Option(const Option& option):
	_shortName(option._shortName),
	_fullName(option._fullName),
	_description(option._description),
	_required(option._required),
	_repeatable(option._repeatable),
	_argName(option._argName),
	_argRequired(option._argRequired),
	_group(option._group),
	_binding(option._binding),
	_pValidator(option._pValidator),
	_pCallback(option._pCallback),
	_pConfig(option._pConfig)
{
	if (_pValidator) _pValidator->duplicate();
	if (_pCallback) _pCallback = _pCallback->clone();
	if (_pConfig) _pConfig->duplicate();
}


Option::Option(const std::string& fullName, const std::string& shortName):
	_shortName(shortName),
	_fullName(fullName),
	_required(false),
	_repeatable(false),
	_argRequired(false),
	_pValidator(0),
	_pCallback(0),
	_pConfig(0)
{
}


Option::Option(const std::string& fullName, const std::string& shortName, const std::string& description, bool required):
	_shortName(shortName),
	_fullName(fullName),
	_description(description),
	_required(required),
	_repeatable(false),
	_argRequired(false),
	_pValidator(0),
	_pCallback(0),
	_pConfig(0)
{
}


Option::Option(const std::string& fullName, const std::string& shortName, const std::string& description, bool required, const std::string& argName, bool argRequired):
	_shortName(shortName),
	_fullName(fullName),
	_description(description),
	_required(required),
	_repeatable(false),
	_argName(argName),
	_argRequired(argRequired),
	_pValidator(0),
	_pCallback(0),
	_pConfig(0)
{
}


Option::~Option()
{
	if (_pValidator) _pValidator->release();
	if (_pConfig) _pConfig->release();
	delete _pCallback;
}


Option& Option::operator = (const Option& option)
{
	if (&option != this)
	{
		Option tmp(option);
		swap(tmp);
	}
	return *this;
}


void Option::swap(Option& option)
{
	std::swap(_shortName, option._shortName);
	std::swap(_fullName, option._fullName);
	std::swap(_description, option._description);
	std::swap(_required, option._required);
	std::swap(_repeatable, option._repeatable);
	std::swap(_argName, option._argName);
	std::swap(_argRequired, option._argRequired);
	std::swap(_group, option._group);
	std::swap(_binding, option._binding);
	std::swap(_pValidator, option._pValidator);
	std::swap(_pCallback, option._pCallback);
	std::swap(_pConfig, option._pConfig);
}

	
Option& Option::shortName(const std::string& name)
{
	_shortName = name;
	return *this;
}


Option& Option::fullName(const std::string& name)
{
	_fullName = name;
	return *this;
}

	
Option& Option::description(const std::string& text)
{
	_description = text;
	return *this;
}

	
Option& Option::required(bool flag)
{
	_required = flag;
	return *this;
}


Option& Option::repeatable(bool flag)
{
	_repeatable = flag;
	return *this;
}

	
Option& Option::argument(const std::string& name, bool required)
{
	_argName     = name;
	_argRequired = required;
	return *this;
}

	
Option& Option::noArgument()
{
	_argName.clear();
	_argRequired = false;
	return *this;
}


Option& Option::group(const std::string& group)
{
	_group = group;
	return *this;
}


Option& Option::binding(const std::string& propertyName)
{
	return binding(propertyName, 0);
}


Option& Option::binding(const std::string& propertyName, AbstractConfiguration* pConfig)
{
	_binding = propertyName;
	if (_pConfig) _pConfig->release();
	_pConfig = pConfig;
	if (_pConfig) _pConfig->duplicate();
	return *this;
}


Option& Option::callback(const AbstractOptionCallback& cb)
{
	_pCallback = cb.clone();
	return *this;
}


Option& Option::validator(Validator* pValidator)
{
	if (_pValidator) _pValidator->release();
	_pValidator = pValidator;
	return *this;
}


bool Option::matchesShort(const std::string& option) const
{
	return option.length() > 0 
		&& !_shortName.empty() && option.compare(0, _shortName.length(), _shortName) == 0;
}


bool Option::matchesFull(const std::string& option) const
{
	std::string::size_type pos = option.find_first_of(":=");
	std::string::size_type len = pos == std::string::npos ? option.length() : pos;
	return len == _fullName.length()
		&& icompare(option, 0, len, _fullName, 0, len) == 0;
}


bool Option::matchesPartial(const std::string& option) const
{
	std::string::size_type pos = option.find_first_of(":=");
	std::string::size_type len = pos == std::string::npos ? option.length() : pos;
	return option.length() > 0 
		&& icompare(option, 0, len, _fullName, 0, len) == 0;
}


void Option::process(const std::string& option, std::string& arg) const
{
	std::string::size_type pos = option.find_first_of(":=");
	std::string::size_type len = pos == std::string::npos ? option.length() : pos;
	if (icompare(option, 0, len, _fullName, 0, len) == 0)
	{
		if (takesArgument())
		{
			if (argumentRequired() && pos == std::string::npos)
				throw MissingArgumentException(_fullName + " requires " + argumentName());
			if (pos != std::string::npos)
				arg.assign(option, pos + 1, option.length() - pos - 1);
			else
				arg.clear();
		}
		else if (pos != std::string::npos)
		{
			throw UnexpectedArgumentException(option);
		}
		else arg.clear();
	}
	else if (!_shortName.empty() && option.compare(0, _shortName.length(), _shortName) == 0)
	{
		if (takesArgument())
		{
			if (argumentRequired() && option.length() == _shortName.length())
				throw MissingArgumentException(_shortName + " requires " + argumentName());
			arg.assign(option, _shortName.length(), option.length() - _shortName.length());
		}
		else if (option.length() != _shortName.length())
		{
			throw UnexpectedArgumentException(option);
		}
		else arg.clear();
	}
	else throw UnknownOptionException(option);
}


} } // namespace Poco::Util
