//
// XMLException.cpp
//
// $Id: //poco/1.4/XML/src/XMLException.cpp#1 $
//
// Library: XML
// Package: XML
// Module:  XMLException
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/XML/XMLException.h"
#include <typeinfo>


using Poco::RuntimeException;


namespace Poco {
namespace XML {


POCO_IMPLEMENT_EXCEPTION(XMLException, RuntimeException, "XML Exception")


} } // namespace Poco::XML
