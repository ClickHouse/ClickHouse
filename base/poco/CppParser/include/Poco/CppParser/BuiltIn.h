//
// BuiltIn.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  BuiltIn
//
// Definition of the BuiltIn class.
//
// Copyright (c) 2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_BuiltIn_INCLUDED
#define CppParser_BuiltIn_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Symbol.h"


namespace Poco {
namespace CppParser {


class CppParser_API BuiltIn: public Symbol
	/// A placeholder for a built-in type.
{
public:
	BuiltIn(const std::string& name, NameSpace* pNameSpace);
		/// Creates the BuiltIn.

	~BuiltIn();
		/// Destroys the BuiltIn.

	Symbol::Kind kind() const;
	std::string toString() const;
};


} } // namespace Poco::CppParser


#endif // CppParser_BuiltIn_INCLUDED
