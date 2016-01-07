//
// VarHolder.cpp
//
// $Id: //poco/svn/Foundation/src/VarHolder.cpp#3 $
//
// Library: Foundation
// Package: Core
// Module:  VarHolder
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Dynamic/VarHolder.h"
#include "Poco/Dynamic/Var.h"


namespace Poco {
namespace Dynamic {


VarHolder::VarHolder()
{
}


VarHolder::~VarHolder()
{
}


namespace Impl {


void escape(std::string& target, const std::string& source)
{
	std::string::const_iterator it(source.begin());
	std::string::const_iterator end(source.end());
	for (; it != end; ++it)
	{
		switch (*it)
		{
		case '"':
			target += "\\\"";
			break;
		case '\\':
			target += "\\\\";
			break;
		case '\b':
			target += "\\b";
			break;
		case '\f':
			target += "\\f";
			break;
		case '\n':
			target += "\\n";
			break;
		case '\r':
			target += "\\r";
			break;
		case '\t':
			target += "\\t";
			break;
		default:
			target += *it;
			break;
		}
	}
}


bool isJSONString(const Var& any)
{
	return any.type() == typeid(std::string) || 
		any.type() == typeid(char) || 
		any.type() == typeid(char*) || 
		any.type() == typeid(Poco::DateTime) || 
		any.type() == typeid(Poco::LocalDateTime) || 
		any.type() == typeid(Poco::Timestamp);
}


void appendJSONString(std::string& val, const Var& any)
{
	val += '"';
	escape(val, any.convert<std::string>());
	val += '"';
}


void appendJSONKey(std::string& val, const Var& any)
{
	return appendJSONString(val, any);
}


void appendJSONValue(std::string& val, const Var& any)
{
	if (any.isEmpty()) 
	{
		val.append("null");
	}
	else 
	{
		bool isStr = isJSONString(any);
		if (isStr) 
		{
			appendJSONString(val, any.convert<std::string>());
		}
		else
		{
			val.append(any.convert<std::string>());
		}
	}
}


} // namespace Impl


} } // namespace Poco::Dynamic
