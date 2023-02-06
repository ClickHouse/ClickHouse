//
// Namespace.cpp
//
// Library: CppParser
// Package: SymbolTable
// Module:  Namespace
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/NameSpace.h"
#include "Poco/Exception.h"
#include <sstream>
#include <cstddef>


using Poco::ExistsException;
using Poco::NotFoundException;


namespace Poco {
namespace CppParser {


NameSpace::NameSpace()
{
}


NameSpace::NameSpace(const std::string& name, NameSpace* pNameSpace):
	Symbol(name, pNameSpace)
{
}


NameSpace::~NameSpace()
{
	for (SymbolTable::iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		delete it->second;
	}
}


void NameSpace::addSymbol(Symbol* pSymbol)
{
	poco_check_ptr (pSymbol);

	_symbols.insert(SymbolTable::value_type(pSymbol->name(), pSymbol));
}


void NameSpace::importSymbol(const std::string& fullName)
{
	std::string localName;
	std::string::size_type pos = fullName.find_last_of(':');
	if (pos != std::string::npos && pos < fullName.size() - 1)
	{
		localName.assign(fullName, pos + 1, fullName.size() - pos - 1);
		_importedSymbols[localName] = fullName;
	}
}


void NameSpace::importNameSpace(const std::string& nameSpace)
{
	_importedNameSpaces.push_back(nameSpace);
}


NameSpace::Iterator NameSpace::begin() const
{
	return _symbols.begin();
}


NameSpace::Iterator NameSpace::end() const
{
	return _symbols.end();
}


Symbol* NameSpace::lookup(const std::string& name) const
{
	std::set<const NameSpace*> alreadyVisited;
	return lookup(name, alreadyVisited);
}


Symbol* NameSpace::lookup(const std::string& name, std::set<const NameSpace*>& alreadyVisited) const
{
	Symbol* pSymbol = 0;

	if (name.empty())
		return pSymbol;

	if (alreadyVisited.find(this) != alreadyVisited.end())
			return pSymbol;
	std::string head;
	std::string tail;
	splitName(name, head, tail);

	alreadyVisited.insert(this);
	bool currentNSInserted = true;


	if (head.empty())
	{
		alreadyVisited.insert(this);
		return root()->lookup(tail, alreadyVisited);
	}
	SymbolTable::const_iterator it = _symbols.find(head);
	if (it != _symbols.end())
	{
		pSymbol = it->second;
		if (!tail.empty())
		{
			alreadyVisited.insert(this);
			NameSpace* pNS = dynamic_cast<NameSpace*>(pSymbol);
			if (pNS)
				pSymbol = static_cast<NameSpace*>(pSymbol)->lookup(tail, alreadyVisited);
			else
				pSymbol = 0;
		}
	}
	else if (tail.empty())
	{
		AliasMap::const_iterator itAlias = _importedSymbols.find(head);
		if (itAlias != _importedSymbols.end())
			pSymbol = lookup(itAlias->second, alreadyVisited);
		else
		{
			for (NameSpaceVec::const_iterator it = _importedNameSpaces.begin(); !pSymbol && it != _importedNameSpaces.end(); ++it)
			{
				Symbol* pNS = lookup(*it, alreadyVisited);
				if (pNS && pNS->kind() == Symbol::SYM_NAMESPACE)
				{
					pSymbol = static_cast<NameSpace*>(pNS)->lookup(name, alreadyVisited);
				}
			}
		}
	}
	NameSpace* pNS = nameSpace();
	if (!pSymbol && pNS && (alreadyVisited.find(pNS) == alreadyVisited.end()))
	{
		// if we have to go up, never push the NS!
		if (currentNSInserted)
			alreadyVisited.erase(this);
		pSymbol = nameSpace()->lookup(name, alreadyVisited);
	}
	return pSymbol;
}


void NameSpace::nameSpaces(SymbolTable& table) const
{
	extract(Symbol::SYM_NAMESPACE, table);
}


void NameSpace::typeDefs(SymbolTable& table) const
{
	extract(Symbol::SYM_TYPEDEF, table);
}


void NameSpace::typeAliases(SymbolTable& table) const
{
	extract(Symbol::SYM_TYPEALIAS, table);
}


void NameSpace::enums(SymbolTable& table) const
{
	extract(Symbol::SYM_ENUM, table);
}


void NameSpace::classes(SymbolTable& table) const
{
	extract(Symbol::SYM_STRUCT, table);
}


void NameSpace::functions(SymbolTable& table) const
{
	extract(Symbol::SYM_FUNCTION, table);
}


void NameSpace::variables(SymbolTable& table) const
{
	extract(Symbol::SYM_VARIABLE, table);
}


Symbol::Kind NameSpace::kind() const
{
	return Symbol::SYM_NAMESPACE;
}


std::string NameSpace::toString() const
{
	std::ostringstream ostr;
	ostr << "namespace " << name() << "\n{\n";
	for (Iterator it = begin(); it != end(); ++it)
	{
		ostr << it->second->fullName() << "\n";
		ostr << it->second->toString() << "\n";
	}
	ostr << "}\n";
	return ostr.str();
}


void NameSpace::splitName(const std::string& name, std::string& head, std::string& tail)
{
	std::string::size_type pos = name.find(':');
	if (pos != std::string::npos)
	{
		head.assign(name, 0, pos);
		pos += 2;
		poco_assert (pos < name.length());
		tail.assign(name, pos, name.length() - pos);
	}
	else head = name;
}


NameSpace* NameSpace::root()
{
	static NameSpace root;
	return &root;
}


void NameSpace::extract(Symbol::Kind kind, SymbolTable& table) const
{
	for (SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		if (it->second->kind() == kind)
			table.insert(*it);
	}
}


} } // namespace Poco::CppParser
