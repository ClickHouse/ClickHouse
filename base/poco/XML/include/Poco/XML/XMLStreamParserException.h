//
// XMLStreamParserException.h
//
// Library: XML
// Package: XML
// Module:  XMLStreamParserException
//
// Definition of the XMLStreamParserException class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_XMLStreamParserException_INCLUDED
#define XML_XMLStreamParserException_INCLUDED


#include "Poco/XML/XMLException.h"


namespace Poco {
namespace XML {


class XMLStreamParser;


class XML_API XMLStreamParserException: public Poco::XML::XMLException
{
public:
	XMLStreamParserException(const std::string& name, Poco::UInt64 line, Poco::UInt64 column, const std::string& description);
	XMLStreamParserException(const XMLStreamParser&, const std::string& description);
	virtual ~XMLStreamParserException() throw ();

	const char* name() const noexcept;
	Poco::UInt64 line() const;
	Poco::UInt64 column() const;
	const std::string& description() const;
	virtual const char* what() const throw ();

private:
	void init();

	std::string _name;
	Poco::UInt64 _line;
	Poco::UInt64 _column;
	std::string _description;
	std::string _what;
};


} } // namespace Poco::XML


#endif // XML_XMLStreamParserException_INCLUDED
