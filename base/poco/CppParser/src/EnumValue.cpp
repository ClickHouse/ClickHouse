//
// EnumValue.cpp
//
// Library: CppParser
// Package: SymbolTable
// Module:  EnumValue
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/EnumValue.h"
#include "Poco/CppParser/Enum.h"


namespace Poco {
namespace CppParser {


EnumValue::EnumValue(const std::string& name, const std::string& value, Enum* pEnum):
	Symbol(name, pEnum->nameSpace()),
	_value(value)
{
	pEnum->addValue(this);
}


EnumValue::~EnumValue()
{
}


Symbol::Kind EnumValue::kind() const
{
	return Symbol::SYM_ENUM_VALUE;
}


std::string EnumValue::toString() const
{
	std::string result(name());
	if (!_value.empty())
	{
		result.append(" = ");
		result.append(_value);
	}
	return result;
}


} } // namespace Poco::CppParser
