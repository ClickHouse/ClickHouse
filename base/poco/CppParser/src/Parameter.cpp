//
// Parameter.cpp
//
// Library: CppParser
// Package: SymbolTable
// Module:  Parameter
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/Parameter.h"
#include "Poco/CppParser/NameSpace.h"
#include "Poco/CppParser/TypeDef.h"
#include "Poco/CppParser/Utility.h"
#include "Poco/String.h"
#include "Poco/NumberFormatter.h"
#include <cstddef>


namespace Poco {
namespace CppParser {


int Parameter::_count(0);


Parameter::Parameter(const std::string& decl, Function* pFunction):
	Decl(handleDecl(decl), 0), // handle init values
	_type(),
	_isRef(false),
	_isPointer(false),
	_isConst(false)
{
	std::size_t pos = declaration().rfind(name());
	std::string tmp;
	if (pos == 0 && name().size() == declaration().size())
		tmp = declaration();
	else
		tmp = declaration().substr(0, pos);
	_type = Poco::trim(tmp);
	std::size_t rightCut = _type.size();
	while (rightCut > 0 && (_type[rightCut-1] == '&' || _type[rightCut-1] == '*' || _type[rightCut-1] == '\t' || _type[rightCut-1] == ' '))
	{
		if (_type[rightCut-1] == '&')
			_isRef = true;
		if (_type[rightCut-1] == '*')
			_isPointer = true;
		--rightCut;
	}
	_type = Poco::trim(_type.substr(0, rightCut));
	if (_type.find("const ") == 0)
	{
		_isConst = true;
		_type = _type.substr(6);
	}
	if (_type.find("const\t") == 0)
	{
		_type = _type.substr(6);
		_isConst = true;
	}

	Poco::trimInPlace(_type);
	pos = decl.find("=");
	_hasDefaultValue = (pos != std::string::npos);
	if (_hasDefaultValue)
	{
		_defaultDecl = decl.substr(pos + 1);
		Poco::trimInPlace(_defaultDecl);
		std::size_t posStart = _defaultDecl.find("(");
		std::size_t posEnd = _defaultDecl.rfind(")");
		if (posStart != std::string::npos && posEnd != std::string::npos)
		{
			_defaultValue = _defaultDecl.substr(posStart + 1, posEnd-posStart - 1);
		}
		else
		{
			poco_assert (posStart == std::string::npos && posEnd == std::string::npos);
			_defaultValue = _defaultDecl;
		}
		Poco::trimInPlace(_defaultValue);
	}
}


Parameter::~Parameter()
{
}


Symbol::Kind Parameter::kind() const
{
	return Symbol::SYM_PARAMETER;
}


bool Parameter::vectorType(const std::string& type, NameSpace* pNS)
{
	bool ret = type.find("vector") != std::string::npos;
	if (!ret)
	{
		Symbol* pSym = pNS->lookup(type);
		if (pSym)
		{
			if (pSym->kind() == Symbol::SYM_TYPEDEF)
			{
				TypeDef* pType = static_cast<TypeDef*>(pSym);
				ret = pType->baseType().find("vector") != std::string::npos;
			}
			else if (pSym->kind() == Symbol::SYM_TYPEALIAS)
			{
				TypeAlias* pType = static_cast<TypeAlias*>(pSym);
				ret = pType->baseType().find("vector") != std::string::npos;
			}
		}
	}
	return ret;
}


std::string Parameter::handleDecl(const std::string& decl)
{
	std::size_t pos = decl.find('=');
	std::string result(decl.substr(0, pos));
	// now check if we have to add a paramName
	Poco::trimInPlace(result);
	std::size_t posSpace = result.rfind(' ');
	bool mustAdd = false;
	if (posSpace>0)
	{
		std::string tmp(result.substr(posSpace+1));
		mustAdd = (tmp.find('<') != -1 || tmp.find('>') != -1 || tmp.find('*') != -1 || tmp.find('&') != -1);
	}
	else
		mustAdd = true;
	if (mustAdd)
	{
		result.append(" ");
		result.append("param");
		result.append(Poco::NumberFormatter::format(++_count));
		//never add the default val it breaks the upper class parser
	}
	return result;
}


} } // namespace Poco::CppParser
