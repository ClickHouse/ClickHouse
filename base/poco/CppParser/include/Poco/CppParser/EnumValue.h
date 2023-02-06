//
// EnumValue.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  EnumValue
//
// Definition of the EnumValue class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_EnumValue_INCLUDED
#define CppParser_EnumValue_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Symbol.h"


namespace Poco {
namespace CppParser {


class Enum;


class CppParser_API EnumValue: public Symbol
	/// This class represents an enumeration value
	/// inside an enum declaration.
{
public:
	EnumValue(const std::string& name, const std::string& value, Enum* pEnum);
		/// Creates the EnumValue, using the name and a value, which may be empty.

	virtual ~EnumValue();
		/// Destroys the EnumValue.

	const std::string& value() const;
		/// Returns the value, which may be empty.

	Symbol::Kind kind() const;
	std::string toString() const;

private:
	std::string _value;
};


//
// inlines
//
inline const std::string& EnumValue::value() const
{
	return _value;
}


} } // namespace Poco::CppParser


#endif // CppParser_EnumValue_INCLUDED
