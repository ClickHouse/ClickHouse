//
// PrintHandler.cpp
//
// $Id$
//
// Library: JSON
// Package: JSON
// Module:  PrintHandler
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/JSON/PrintHandler.h"
#include "Poco/JSON/Stringifier.h"
#include <iostream>


namespace Poco {
namespace JSON {


PrintHandler::PrintHandler(unsigned indent):
	_out(std::cout),
	_indent(indent),
	_array(0),
	_objStart(true)
{
}


PrintHandler::PrintHandler(std::ostream& out, unsigned indent):
	_out(out),
	_indent(indent),
	_array(0),
	_objStart(true)
{
}


PrintHandler::~PrintHandler()
{
}


void PrintHandler::reset()
{
	_out.flush();
	_tab = "";
	_array = 0;
	_objStart = true;
}


const char* PrintHandler::endLine() const
{
	if (!printFlat()) return "\n";
	else return "";
}


bool PrintHandler::printFlat() const
{
	return _indent == JSON_PRINT_FLAT;
}


unsigned PrintHandler::indent()
{
	if (!printFlat()) return _indent;
	
	return 0;
}


void PrintHandler::startObject()
{
	arrayValue();
	_out << '{';
	_out << endLine();
	_tab.append(indent(), ' ');
	_objStart = true;
}


void PrintHandler::endObject()
{
	if( _tab.length() >= indent())
		_tab.erase(_tab.length() - indent());

	_out << endLine() << _tab << '}';
	_objStart = false;
}


void PrintHandler::startArray()
{
	arrayValue();
	_out << '[' << endLine();
	_tab.append(indent(), ' ');
	++_array;
	_objStart = true;
}


void PrintHandler::endArray()
{
	_tab.erase(_tab.length() - indent());
	_out << endLine() << _tab << ']';
	--_array;
	poco_assert (_array >= 0);
	_objStart = false;
}


void PrintHandler::key(const std::string& k)
{
	if (!_objStart) comma();

	_objStart = true;

	_out << _tab;
	Stringifier::formatString(k, _out);
	if (!printFlat()) _out << ' ';
	_out << ':';
	if (!printFlat()) _out << ' ';
}


void PrintHandler::null()
{
	arrayValue();
	_out << "null";

	_objStart = false;
}


void PrintHandler::value(int v)
{
	arrayValue();
	_out << v;
	_objStart = false;
}


void PrintHandler::value(unsigned v)
{
	arrayValue();
	_out << v;
	_objStart = false;
}


#if defined(POCO_HAVE_INT64)
void PrintHandler::value(Int64 v)
{
	arrayValue();
	_out << v;
	_objStart = false;
}


void PrintHandler::value(UInt64 v)
{
	arrayValue();
	_out << v;
	_objStart = false;
}
#endif


void PrintHandler::value(const std::string& value)
{
	arrayValue();
	Stringifier::formatString(value, _out);
	_objStart = false;
}



void PrintHandler::value(double d)
{
	arrayValue();
	_out << d;
	_objStart = false;
}


void PrintHandler::value(bool b)
{
	arrayValue();
	_out << b;
	_objStart = false;
}


void PrintHandler::comma()
{
	_out << ',' << endLine();
}


void PrintHandler::arrayValue()
{
	if (!_objStart) comma();
	if (array()) {
		_out << _tab;
	}
}


} } // namespace Poco::JSON
