//
// PrettyPrint.cpp
//
// $Id: //poco/1.4/XML/samples/PrettyPrint/src/PrettyPrint.cpp#1 $
//
// This sample demonstrates the SAXParser, WhitespaceFilter,
// InputSource and XMLWriter classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SAX/SAXParser.h"
#include "Poco/SAX/WhitespaceFilter.h"
#include "Poco/SAX/InputSource.h"
#include "Poco/XML/XMLWriter.h"
#include "Poco/Exception.h"
#include <iostream>


using Poco::XML::SAXParser;
using Poco::XML::XMLReader;
using Poco::XML::WhitespaceFilter;
using Poco::XML::InputSource;
using Poco::XML::XMLWriter;
using Poco::Exception;


int main(int argc, char** argv)
{
	// read XML from standard input and pretty-print it to standard output

	SAXParser parser;
	WhitespaceFilter filter(&parser);
	
	XMLWriter writer(std::cout, XMLWriter::CANONICAL | XMLWriter::PRETTY_PRINT);
	writer.setNewLine(XMLWriter::NEWLINE_LF);

	filter.setContentHandler(&writer);
	filter.setDTDHandler(&writer);
	filter.setProperty(XMLReader::PROPERTY_LEXICAL_HANDLER, static_cast<Poco::XML::LexicalHandler*>(&writer));

	try
	{
		InputSource source(std::cin);
		filter.parse(&source);
	}
	catch (Exception& exc)
	{
		std::cerr << exc.displayText() << std::endl;
		return 1;
	}

	return 0;
}
