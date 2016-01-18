//
// ApacheCodeWriter.cpp
//
// $Id: //poco/1.4/PageCompiler/src/ApacheCodeWriter.cpp#1 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ApacheCodeWriter.h"
#include "Page.h"


ApacheCodeWriter::ApacheCodeWriter(const Page& page, const std::string& clazz):
	CodeWriter(page, clazz)
{
}


ApacheCodeWriter::~ApacheCodeWriter()
{
}


void ApacheCodeWriter::writeHeaderIncludes(std::ostream& ostr)
{
	CodeWriter::writeHeaderIncludes(ostr);
	ostr << "#include \"Poco/Net/HTTPRequestHandlerFactory.h\"\n";
}


void ApacheCodeWriter::writeFactoryClass(std::ostream& ostr)
{
	ostr << "\n\n";
	factoryClass(ostr, "Poco::Net::HTTPRequestHandlerFactory");
}


void ApacheCodeWriter::writeImplIncludes(std::ostream& ostr)
{
	CodeWriter::writeImplIncludes(ostr);
	ostr << "#include \"Poco/ClassLibrary.h\"\n";
}


void ApacheCodeWriter::writeFactory(std::ostream& ostr)
{
	ostr << "\n\n";
	factoryImpl(ostr, "");
}


void ApacheCodeWriter::writeManifest(std::ostream& ostr)
{
	std::string ns = page().get("page.namespace", "");
	if (!ns.empty()) ns += "::";
	ostr << "\n\n";
	ostr << "POCO_BEGIN_MANIFEST(Poco::Net::HTTPRequestHandlerFactory)\n";
	ostr << "\tPOCO_EXPORT_CLASS(" << ns << clazz() << "Factory)\n";
	ostr << "POCO_END_MANIFEST\n";
}
