//
// Array.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  Array
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/Array.h"
#include <sstream>


namespace Poco {
namespace MongoDB {


Array::Array():
	Document()
{
}


Array::~Array()
{
}


Element::Ptr Array::get(std::size_t pos) const
{
	std::string name = Poco::NumberFormatter::format(pos);
	return Document::get(name);
}


std::string Array::toString(int indent) const
{
	std::ostringstream oss;

	oss << "[";

	if (indent > 0) oss << std::endl;

	for (ElementSet::const_iterator it = _elements.begin(); it != _elements.end(); ++it)
	{
		if (it != _elements.begin())
		{
			oss << ",";
			if (indent > 0) oss << std::endl;
		}

		for (int i = 0; i < indent; ++i) oss << ' ';

		oss << (*it)->toString(indent > 0 ? indent + 2 : 0);
	}

	if (indent > 0)
	{
		oss << std::endl;
		if (indent >= 2) indent -= 2;
		for (int i = 0; i < indent; ++i) oss << ' ';
	}

	oss << "]";

	return oss.str();
}


} } // Namespace Poco::Mongo
