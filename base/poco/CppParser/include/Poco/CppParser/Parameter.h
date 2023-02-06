//
// Parameter.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  Parameter
//
// Definition of the Parameter class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_Parameter_INCLUDED
#define CppParser_Parameter_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Decl.h"


namespace Poco {
namespace CppParser {


class Function;


class CppParser_API Parameter: public Decl
	/// This class represents a parameter to a function.
{
public:
	Parameter(const std::string& decl, Function* pFunction);
		/// Creates the Parameter.

	~Parameter();
		/// Destroys the Parameter.

	Symbol::Kind kind() const;

	bool isReference() const;
		/// Returns true iff the parameter is a reference.

	bool isPointer() const;
		/// Returns true iff the parameter is a pointer.

	bool isConst() const;
		/// Returns true iff the parameter is const.

	bool hasDefaultValue() const;
		/// Returns true if a defaultvalue was set at this parameter,
		/// Example: const std::string& data = std::string("default").

	const std::string& declType() const;
		/// Returns the type of the parameter without const and & if present.
		///
		/// Example: a type const std::string& -> std::string, a type const std::string* returns std::string

	const std::string& defaultValue() const;
		/// If hasDefaultValue() returns true, this method returns the default value, i.e. all data found between
		/// the opening and closing bracket of the init string.
		///
		/// Example: for const std::string& data = std::string("default") it will return "default",
		/// for = std::string() it will return a zero length string, for = ComplexClass(13,12, "test", 0);
		/// it will return 13,12, "test", 0.

	const std::string& defaultDecl() const;
		/// If hasDefaultValue() returns true, this method returns the
		/// default value declaration.
		///
		/// Example: for const std::string& data = std::string("default") it will return std::string("default").

	static bool vectorType(const std::string& type, NameSpace* pNS);

private:
	std::string handleDecl(const std::string& decl);
	/// Removes initialization values, adds param Names if they are missing

private:
	std::string _type;
	bool        _isRef;
	bool        _isPointer;
	bool        _isConst;
	bool        _hasDefaultValue;
	std::string _defaultValue;
	std::string _defaultDecl;
	static int  _count;
};


//
// inlines
//
inline const std::string& Parameter::declType() const
{
	return _type;
}


inline bool Parameter::isReference() const
{
	return _isRef;
}


inline bool Parameter::isConst() const
{
	return _isConst;
}


inline bool Parameter::isPointer() const
{
	return _isPointer;
}


inline bool Parameter::hasDefaultValue() const
{
	return _hasDefaultValue;
}


inline const std::string& Parameter::defaultValue() const
{
	return _defaultValue;
}


inline const std::string& Parameter::defaultDecl() const
{
	return _defaultDecl;
}


} } // namespace Poco::CppParser


#endif // CppParser_Parameter_INCLUDED
