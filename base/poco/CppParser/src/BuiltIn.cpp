//
// BuiltIn.cpp
//
// Library: CppParser
// Package: SymbolTable
// Module:  BuiltIn
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/BuiltIn.h"
#include "Poco/String.h"
#include <cctype>


namespace Poco {
namespace CppParser {


BuiltIn::BuiltIn(const std::string& name, NameSpace* pNameSpace):
	Symbol(name, pNameSpace)
{
}


BuiltIn::~BuiltIn()
{
}


Symbol::Kind BuiltIn::kind() const
{
	return Symbol::SYM_BUILTIN;
}


std::string BuiltIn::toString() const
{
	return fullName();
}


} } // namespace Poco::CppParser
