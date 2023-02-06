//
// TypeDef.cpp
//
// Library: CppParser
// Package: SymbolTable
// Module:  TypeDef
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/TypeDef.h"
#include "Poco/String.h"
#include <cctype>


namespace Poco {
namespace CppParser {


TypeDef::TypeDef(const std::string& decl, NameSpace* pNameSpace):
	Decl(decl, pNameSpace)
{
}


TypeDef::~TypeDef()
{
}


Symbol::Kind TypeDef::kind() const
{
	return Symbol::SYM_TYPEDEF;
}


std::string TypeDef::baseType() const
{
	std::string decl = declaration();
	if (decl.compare(0, 7, "typedef") == 0)
	{
		decl.erase(0, 7);
		std::string::size_type pos = decl.size() - 1;
		while (pos > 0 && std::isspace(decl[pos])) pos--;
		while (pos > 0 && !std::isspace(decl[pos])) pos--;
		decl.resize(pos + 1);
		Poco::trimInPlace(decl);
	}
	return decl;
}


TypeAlias::TypeAlias(const std::string& decl, NameSpace* pNameSpace):
	Decl(decl, pNameSpace)
{
}


TypeAlias::~TypeAlias()
{
}


Symbol::Kind TypeAlias::kind() const
{
	return Symbol::SYM_TYPEALIAS;
}


std::string TypeAlias::baseType() const
{
	std::string decl = declaration();
	if (decl.compare(0, 5, "using") == 0)
	{
		decl.erase(0, 5);
		std::string::size_type pos = 0;
		while (pos < decl.size() && std::isspace(decl[pos])) pos++;
		while (pos < decl.size() && decl[pos] != '=') pos++;
		decl.erase(0, pos);
		Poco::trimInPlace(decl);
	}
	return decl;
}


} } // namespace Poco::CppParser
