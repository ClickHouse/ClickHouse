//
// TypeDef.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  TypeDef
//
// Definition of the TypeDef class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_TypeDef_INCLUDED
#define CppParser_TypeDef_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Decl.h"


namespace Poco {
namespace CppParser {


class CppParser_API TypeDef: public Decl
	/// This class represents a type definition (typedef).
{
public:
	TypeDef(const std::string& decl, NameSpace* pNameSpace);
		/// Creates the TypeDef.

	~TypeDef();
		/// Destroys the TypeDef.

	Symbol::Kind kind() const;

	std::string baseType() const;
		/// Returns the underlying base type.
};


class CppParser_API TypeAlias: public Decl
	/// This class represents a type alias definition (using).
{
public:
	TypeAlias(const std::string& decl, NameSpace* pNameSpace);
		/// Creates the TypeAlias.

	~TypeAlias();
		/// Destroys the TypeAlias.

	Symbol::Kind kind() const;

	std::string baseType() const;
		/// Returns the underlying base type.
};


} } // namespace Poco::CppParser


#endif // CppParser_TypeDef_INCLUDED
