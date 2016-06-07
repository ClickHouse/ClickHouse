//
// Stringifier.h
//
// $Id$
//
// Library: JSON
// Package: JSON
// Module:  Stringifier
//
// Definition of the Stringifier class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSON_JSONStringifier_INCLUDED
#define JSON_JSONStringifier_INCLUDED


#include "Poco/Dynamic/Var.h"
#include "Poco/JSON/JSON.h"
#include <ostream>


namespace Poco {
namespace JSON {


class JSON_API Stringifier
	/// Helper class for creating a String from a JSON object or array
{
public:
	static void condense(const Dynamic::Var& any, std::ostream& out);
		/// Writes a condensed string representation of the value to the output stream while preserving the insertion order.
		/// This is just a "shortcut" to stringify(any, out) with name indicating the function effect.

	static void stringify(const Dynamic::Var& any, bool preserveInsertionOrder, std::ostream& out, unsigned int indent = 0);
		/// Writes a String representation of the value to the output stream while preserving the insertion order.
		/// When indent is 0, the generated string will be created as small as possible (condensed).
		/// When preserveInsertionOrder is true, the original string object members order will be preserved.
		/// This is a "shortcut" to stringify(any, out, indent, -1, preserveInsertionOrder).

	static void stringify(const Dynamic::Var& any, std::ostream& out, unsigned int indent = 0, int step = -1, bool preserveInsertionOrder = false);
		/// Writes a String representation of the value to the output stream.
		/// When indent is 0, the String will be created as small as possible.
		/// When preserveInsertionOrder is true, the original string object members order will be preserved;
		/// otherwise, object members are sorted by their names.

	static void formatString(const std::string& value, std::ostream& out);
		/// Formats the JSON string and streams it into ostream.
};


inline void Stringifier::condense(const Dynamic::Var& any, std::ostream& out)
{
	stringify(any, out, 0, -1, true);
}


inline void Stringifier::stringify(const Dynamic::Var& any, bool preserveInsertionOrder, std::ostream& out, unsigned int indent)
{
	stringify(any, out, indent, -1, preserveInsertionOrder);
}


}} // namespace Poco::JSON


#endif // JSON_JSONStringifier_INCLUDED
