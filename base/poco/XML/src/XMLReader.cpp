//
// XMLReader.cpp
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SAX/XMLReader.h"


namespace Poco {
namespace XML {


const XMLString XMLReader::FEATURE_VALIDATION                  = toXMLString("http://xml.org/sax/features/validation");
const XMLString XMLReader::FEATURE_NAMESPACES                  = toXMLString("http://xml.org/sax/features/namespaces");
const XMLString XMLReader::FEATURE_NAMESPACE_PREFIXES          = toXMLString("http://xml.org/sax/features/namespace-prefixes");
const XMLString XMLReader::FEATURE_EXTERNAL_GENERAL_ENTITIES   = toXMLString("http://xml.org/sax/features/external-general-entities");
const XMLString XMLReader::FEATURE_EXTERNAL_PARAMETER_ENTITIES = toXMLString("http://xml.org/sax/features/external-parameter-entities");
const XMLString XMLReader::FEATURE_STRING_INTERNING            = toXMLString("http://xml.org/sax/features/string-interning");
const XMLString XMLReader::PROPERTY_DECLARATION_HANDLER        = toXMLString("http://xml.org/sax/properties/declaration-handler");
const XMLString XMLReader::PROPERTY_LEXICAL_HANDLER            = toXMLString("http://xml.org/sax/properties/lexical-handler");


XMLReader::~XMLReader()
{
}


} } // namespace Poco::XML
