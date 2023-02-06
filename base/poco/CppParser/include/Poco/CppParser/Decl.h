//
// Decl.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  Decl
//
// Definition of the Decl class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_Decl_INCLUDED
#define CppParser_Decl_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Symbol.h"


namespace Poco {
namespace CppParser {


class CppParser_API Decl: public Symbol
	/// This class represents a simple declaration in a C++ source file.
	/// It is a base class for Function, TypeDef or Variable.
{
public:
	Decl(const std::string& decl, NameSpace* pNameSpace);
		/// Creates the Decl.

	~Decl();
		/// Destroys the Decl.

	const std::string& declaration() const;
		/// Returns the declaration.

	std::string toString() const;

protected:
	std::string _decl;
};


//
// inlines
//
inline const std::string& Decl::declaration() const
{
	return _decl;
}


} } // namespace Poco::CppParser


#endif // CppParser_Decl_INCLUDED
