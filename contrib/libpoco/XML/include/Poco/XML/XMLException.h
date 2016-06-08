//
// XMLException.h
//
// $Id: //poco/1.4/XML/include/Poco/XML/XMLException.h#1 $
//
// Library: XML
// Package: XML
// Module:  XMLException
//
// Definition of the XMLException class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_XMLException_INCLUDED
#define XML_XMLException_INCLUDED


#include "Poco/XML/XML.h"
#include "Poco/Exception.h"


namespace Poco {
namespace XML {


POCO_DECLARE_EXCEPTION(XML_API, XMLException, Poco::RuntimeException)
	/// The base class for all XML-related exceptions like SAXException
	/// and DOMException.


} } // namespace Poco::XML


#endif // XML_XMLException_INCLUDED
