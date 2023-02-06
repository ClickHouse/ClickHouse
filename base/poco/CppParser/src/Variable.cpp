//
// Variable.cpp
//
// Library: CppParser
// Package: SymbolTable
// Module:  Variable
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/Variable.h"
#include "Poco/String.h"
#include <cstddef>


namespace Poco {
namespace CppParser {


Variable::Variable(const std::string& decl, NameSpace* pNameSpace):
	Decl(decl, pNameSpace),
	_flags(0),
	_isPointer(false),
	_type()
{
	if (hasAttr(decl, "static"))
		_flags |= VAR_STATIC;
	if (hasAttr(decl, "mutable"))
		_flags |= VAR_MUTABLE;
	if (hasAttr(decl, "volatile"))
		_flags |= VAR_VOLATILE;
	if (hasAttr(decl, "const"))
		_flags |= VAR_CONST;

	std::size_t pos = decl.rfind(name());
	std::string tmp = decl.substr(0, pos);
	tmp = Poco::trim(tmp);
	
	pos = tmp.rfind("*");
	_isPointer = (pos == (tmp.size()-1));
	
	Poco::replaceInPlace(tmp, "static ", "");
	Poco::replaceInPlace(tmp, "mutable ", "");
	Poco::replaceInPlace(tmp, "volatile ", "");
	Poco::replaceInPlace(tmp, "static\t", "");
	Poco::replaceInPlace(tmp, "mutable\t", "");
	Poco::replaceInPlace(tmp, "volatile\t", "");
	if (tmp.find("const ") == 0)
		tmp = tmp.substr(6);
	if (tmp.find("const\t") == 0)
		tmp = tmp.substr(6);
	
	std::size_t rightCut = tmp.size();
	while (rightCut > 0 && (tmp[rightCut-1] == '&' || tmp[rightCut-1] == '*' || tmp[rightCut-1] == '\t' || tmp[rightCut-1] == ' '))
		--rightCut;
	_type = Poco::trim(tmp.substr(0, rightCut));

}


Variable::~Variable()
{
}


Symbol::Kind Variable::kind() const
{
	return Symbol::SYM_VARIABLE;
}


} } // namespace Poco::CppParser
