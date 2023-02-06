//
// Symbol.cpp
//
// Library: CppParser
// Package: SymbolTable
// Module:  Symbol
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/Symbol.h"
#include "Poco/CppParser/NameSpace.h"
#include "Poco/CppParser/Utility.h"
#include "Poco/String.h"
#include <cctype>
#include <cstddef>


namespace Poco {
namespace CppParser {


int Symbol::_nextId = 0;


Symbol::Symbol():
	_id(_nextId++),
	_pNameSpace(0),
	_access(ACC_PUBLIC),
	_line(-1)
{
}


Symbol::Symbol(const std::string& name, NameSpace* pNameSpace):
	_id(_nextId++),
	_name(name),
	_pNameSpace(pNameSpace),
	_access(ACC_PUBLIC),
	_line(-1)
{
	if (_pNameSpace)
		_pNameSpace->addSymbol(this);
}


Symbol::~Symbol()
{
}


void Symbol::setAccess(Access access)
{
	_access = access;
}


void Symbol::setDocumentation(const std::string& text)
{
	_documentation = text;
}


void Symbol::addDocumentation(const std::string& text)
{
	if (!_documentation.empty())
		_documentation.append("\n");
	_documentation.append(text);
}


void Symbol::setFile(const std::string& path)
{
	_file = path;
}


void Symbol::setLineNumber(int line)
{
	_line = line;
}


void Symbol::setPackage(const std::string& package)
{
	_package = package;
}


void Symbol::setLibrary(const std::string& library)
{
	_library = library;
}


std::string Symbol::fullName() const
{
	std::string fullName;
	if (_pNameSpace)
	{
		fullName = _pNameSpace->fullName();
		if (!fullName.empty()) fullName.append("::");
	}
	fullName.append(_name);
	return fullName;
}


std::string Symbol::extractName(const std::string& decl)
{
	poco_assert (!decl.empty());

	// special cases: operator () and operator []
	if (decl.find("operator ()") != std::string::npos)
		return "operator ()";
	else if (decl.find("operator[]") != std::string::npos)
		return "operator []";
		
	std::string::size_type pos = decl.find('(');
	// another special case: function pointer
	if (pos != std::string::npos && pos < decl.size() - 1)
	{
		std::string::size_type i = pos + 1;
		while (i < decl.size() && std::isspace(decl[i])) i++;
		if (i < decl.size() && decl[i] == '*')
		{
			i++;
			std::string name;
			while (i < decl.size() && std::isspace(decl[i])) i++;
			while (i < decl.size() && !std::isspace(decl[i]) && decl[i] != ')') name += decl[i++];
			return name;
		}
	}
	if (pos == std::string::npos || (pos > 0 && decl[pos - 1] == '('))
		pos = decl.size();
	--pos;
	// check for constant; start searching after template
	std::string::size_type eqStart = 0;
	if (decl.compare(0, 8, "template") == 0)
	{
		eqStart = 8;
		while (std::isspace(decl[eqStart]) && eqStart < decl.size()) ++eqStart;
		if (eqStart < decl.size() && decl[eqStart] == '<')
		{
			++eqStart;
			int tc = 1;
			while (tc > 0 && eqStart < decl.size())
			{
				if (decl[eqStart] == '<')
					++tc;
				else if (decl[eqStart] == '>')
					--tc;
				++eqStart;
			}
		}
	}
	std::string::size_type eqPos = decl.find('=', eqStart);
	if (eqPos != std::string::npos)
	{
		// special case: default template parameter
		std::string::size_type gtPos = decl.find('>', eqPos);
		std::string::size_type ltPos = decl.find('<', eqPos);
		if ((gtPos == std::string::npos || gtPos > pos || (ltPos != std::string::npos && gtPos > ltPos)) && eqPos < pos && eqPos > 0 && decl[eqPos + 1] != '=')
			pos = eqPos - 1;
	}
	while (pos > 0 && std::isspace(decl[pos])) --pos;
	while (pos > 0 && decl[pos] == ']')
	{
		--pos;
		while (pos > 0 && decl[pos] != '[') --pos;
		if (pos > 0) --pos;
		while (pos > 0 && std::isspace(decl[pos])) --pos;
	}
	// iterate over template (specialization)
	int nestedTemplateCount = 0;
	if (pos > 1 && decl[pos] == '>' && decl[pos-1] != '-' && decl[pos-1] != '>') // the operators ->, >>
	{
		--pos;
		++nestedTemplateCount;
		while (pos > 0 && nestedTemplateCount != 0)
		{
			if (decl[pos] == '<')
				--nestedTemplateCount;
			if (decl[pos] == '>')
				++nestedTemplateCount;
			--pos;
		}
		while (pos > 0 && std::isspace(decl[pos])) --pos;
	}
	std::string::size_type end = pos;
	std::string::size_type op = decl.find("operator ");
	if (op != std::string::npos && (op == 0 || std::isspace(decl[op - 1]) || decl[op - 1] == ':'))
	{
		pos = op;
	}
	else
	{
		while (pos > 0 && !isIdent(decl[pos])) --pos;
		while (pos > 0 && std::isspace(decl[pos])) --pos;
		while (pos > 0 && isIdent(decl[pos - 1])) --pos;
		if (pos > 0 && decl[pos - 1] == '~') --pos;
	}
	
	while (pos > 2 && decl[pos - 1] == ':')
	{
		pos -= 3;
		while (pos > 0 && isIdent(decl[pos - 1])) --pos;
	}
	return decl.substr(pos, end - pos + 1);
}


bool Symbol::isIdent(char c)
{
	return std::isalnum(c) || c == '_';
}


bool Symbol::hasAttr(const std::string& decl, const std::string& attr)
{
	std::string attrS(attr);
	attrS += ' ';
	std::string::size_type pos = decl.find(attrS);
	return pos == 0 || (pos != std::string::npos && decl[pos - 1] == ' ');
}


void Symbol::setAttributes(const Attributes& attrs)
{
	_attrs = attrs;
}


const Attributes& Symbol::getAttributes() const
{
	return _attrs;
}


} } // namespace Poco::CppParser
