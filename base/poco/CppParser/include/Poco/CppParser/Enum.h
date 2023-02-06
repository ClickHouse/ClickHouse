//
// Enum.h
//
// Library: CppParser
// Package: SymbolTable
// Module:  Enum
//
// Definition of the Enum class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_Enum_INCLUDED
#define CppParser_Enum_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Symbol.h"
#include <vector>


namespace Poco {
namespace CppParser {


class EnumValue;


class CppParser_API Enum: public Symbol
	/// This class represents an enum declaration.
	///
	/// An enumeration has an optional name and
	/// a collection of EnumValues.
{
public:
	typedef std::vector<EnumValue*> Values;
	typedef Values::const_iterator Iterator;

	Enum(const std::string& name, NameSpace* pNameSpace);
		/// Creates the Enum.
		///
		/// If name is the empty string, an internal name
		/// in the form #AnonEnum<n> (where <n> is a unique integer)
		/// will be assigned.

	~Enum();
		/// Destroys the Enum.

	void addValue(EnumValue* pValue);
		/// Adds an enum value. The Enum takes ownership of the value.
		
	Iterator begin() const;
		/// Returns an iterator for iterating over the Enum's EnumValue's.
		
	Iterator end() const;
		/// Returns an iterator for iterating over the Enum's EnumValue's.
	
	Symbol::Kind kind() const;
	std::string toString() const;

protected:
	static std::string processName(const std::string& name);

private:	
	Values _values;
	static int _count;
};


} } // namespace Poco::CppParser


#endif // CppParser_Enum_INCLUDED
