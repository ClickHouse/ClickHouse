//
// NameSpace.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  NameSpace
//
// Definition of the NameSpace class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_NameSpace_INCLUDED
#define CppParser_NameSpace_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Symbol.h"
#include <map>
#include <vector>
#include <set>


namespace Poco {
namespace CppParser {


class CppParser_API NameSpace: public Symbol
	/// This class represents a namespace.
{
public:
	typedef std::multimap<std::string, Symbol*> SymbolTable;
	typedef SymbolTable::const_iterator Iterator;
	typedef std::map<std::string, std::string> AliasMap;
	typedef std::vector<std::string> NameSpaceVec;

	NameSpace();
		/// Creates the NameSpace.

	NameSpace(const std::string& name, NameSpace* pNameSpace = 0);
		/// Creates the NameSpace.

	~NameSpace();
		/// Destroys the NameSpace.

	void addSymbol(Symbol* pSymbol);
		/// Adds a symbol to the namespace.

	void importSymbol(const std::string& fullName);
		/// Imports a symbol from another namespace (using <symbol>).

	void importNameSpace(const std::string& nameSpace);
		/// Imports a namespace (using namespace <namespace>).

	Iterator begin() const;
		/// Returns an iterator for iterating over the NameSpace's Symbol's.

	Iterator end() const;
		/// Returns an iterator for iterating over the NameSpace's Symbol's.

	Symbol* lookup(const std::string& name) const;
		/// Looks up the given name in the symbol table
		/// and returns the corresponsing symbol, or null
		/// if no symbol can be found. The name can include
		/// a namespace.

	static NameSpace* root();
		/// Returns the root namespace. Never delete this one!

	void nameSpaces(SymbolTable& table) const;
		/// Fills the symbol table with all namespaces.

	void typeDefs(SymbolTable& table) const;
		/// Fills the symbol table with all type definitions.

	void typeAliases(SymbolTable& table) const;
		/// Fills the symbol table with all type alias (using) definitions.

	void enums(SymbolTable& table) const;
		/// Fills the symbol table with all enums.

	void classes(SymbolTable& table) const;
		/// Fills the symbol table with all classes and structs.

	void functions(SymbolTable& table) const;
		/// Fills the symbol table with all functions.

	void variables(SymbolTable& table) const;
		/// Fills the symbol table with all variables.

	const AliasMap& importedSymbols() const;
		/// Returns a const reference to a SymbolTable containing all
		/// imported symbols.

	const NameSpaceVec& importedNameSpaces() const;
		/// Returns a vector containing all imported namespaces.

	Symbol::Kind kind() const;
	std::string toString() const;

private:
	Symbol* lookup(const std::string& name, std::set<const NameSpace*>& alreadyVisited) const;
		/// Looks up the given name in the symbol table
		/// and returns the corresponsing symbol, or null
		/// if no symbol can be found. The name can include
		/// a namespace.

protected:
	void extract(Symbol::Kind kind, SymbolTable& table) const;
	static void splitName(const std::string& name, std::string& head, std::string& tail);

private:
	SymbolTable _symbols;
	AliasMap _importedSymbols;
	NameSpaceVec _importedNameSpaces;
};


//
// inlines
//
inline const NameSpace::AliasMap& NameSpace::importedSymbols() const
{
	return _importedSymbols;
}


inline const NameSpace::NameSpaceVec& NameSpace::importedNameSpaces() const
{
	return _importedNameSpaces;
}


} } // namespace Poco::CppParser


#endif // CppParser_NameSpace_INCLUDED
