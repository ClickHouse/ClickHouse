//
// Stringifier.cpp
//
// $Id$
//
// Library: JSON
// Package: JSON
// Module:  Stringifier
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/JSON/Stringifier.h"
#include "Poco/JSON/Array.h"
#include "Poco/JSON/Object.h"
#include <iomanip>


using Poco::Dynamic::Var;


namespace Poco {
namespace JSON {


void Stringifier::stringify(const Var& any, std::ostream& out, unsigned int indent, int step, bool preserveInsertionOrder)
{
	if (step == -1) step = indent;

	if ( any.type() == typeid(Object) )
	{
		const Object& o = any.extract<Object>();
		o.stringify(out, indent == 0 ? 0 : indent, step);
	}
	else if ( any.type() == typeid(Array) )
	{
		const Array& a = any.extract<Array>();
		a.stringify(out, indent == 0 ? 0 : indent, step);
	}
	else if ( any.type() == typeid(Object::Ptr) )
	{
		const Object::Ptr& o = any.extract<Object::Ptr>();
		o->stringify(out, indent == 0 ? 0 : indent, step);
	}
	else if ( any.type() == typeid(Array::Ptr) )
	{
		const Array::Ptr& a = any.extract<Array::Ptr>();
		a->stringify(out, indent == 0 ? 0 : indent, step);
	}
	else if ( any.isEmpty() )
	{
		out << "null";
	}
	else if ( any.isNumeric() || any.isBoolean() )
	{
		out << any.convert<std::string>();
	}
	else
	{
		std::string value = any.convert<std::string>();
		formatString(value, out);
	}
}


void Stringifier::formatString(const std::string& value, std::ostream& out)
{
	out << '"';
	for (std::string::const_iterator it = value.begin(),
		 end = value.end(); it != end; ++it)
	{
		switch (*it)
		{
			case '\\': out << "\\\\"; break;
			case '"': out << "\\\""; break;
			case '/': out << "\\/"; break;
			case '\b': out << "\\b"; break;
			case '\f': out << "\\f"; break;
			case '\n': out << "\\n"; break;
			case '\r': out << "\\r"; break;
			case '\t': out << "\\t"; break;
			default: out << *it; break;
		}
	}
	out << '"';
}


} }  // Namespace Poco::JSON
