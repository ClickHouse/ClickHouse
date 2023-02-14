//
// Element.cpp
//
// Library: MongoDB
// Package: MongoDB
// Module:  Element
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/MongoDB/Element.h"


namespace Poco {
namespace MongoDB {


Element::Element(const std::string& name) : _name(name)
{
}


Element::~Element() 
{
}


} } // namespace Poco::MongoDB
