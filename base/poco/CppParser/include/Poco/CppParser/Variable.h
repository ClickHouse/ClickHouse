//
// Variable.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  Variable
//
// Definition of the Variable class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_Variable_INCLUDED
#define CppParser_Variable_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Decl.h"


namespace Poco {
namespace CppParser {


class CppParser_API Variable: public Decl
	/// This class represents (member) variable declaration.
{
public:
	enum Flags
	{
		VAR_STATIC  = 1,  /// The variable is static.
		VAR_MUTABLE = 2,  /// The variable is mutable.
		VAR_VOLATILE = 4, /// The variable is volatile.
		VAR_CONST    = 8  /// The variable is const.
	};
	
	Variable(const std::string& decl, NameSpace* pNameSpace);
		/// Creates the Variable.

	~Variable();
		/// Destroys the Variable.
		
	int flags() const;
		/// Returns the variable's flags.

	bool isPointer() const;
		/// Returns true iff the variable holds a pointer.

	Symbol::Kind kind() const;

	const std::string& declType() const;
		/// Returns the type of the parameter without const and & if present.
		///
		/// Example: a type const std::string& -> std::string, a type const std::string* returns std::string

		
private:
	int _flags;
	bool _isPointer;
	std::string _type;
};


//
// inlines
//
inline int Variable::flags() const
{
	return _flags;
}


inline bool Variable::isPointer() const
{
	return _isPointer;
}


inline const std::string& Variable::declType() const
{
	return _type;
}


} } // namespace Poco::CppParser


#endif // CppParser_Variable_INCLUDED
