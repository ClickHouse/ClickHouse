//
// OSPCodeWriter.cpp
//
// $Id: //poco/1.4/PageCompiler/src/OSPCodeWriter.cpp#3 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "OSPCodeWriter.h"
#include "Page.h"
#include "Poco/NumberParser.h"


OSPCodeWriter::OSPCodeWriter(const Page& page, const std::string& clazz):
	CodeWriter(page, clazz)
{
}


OSPCodeWriter::~OSPCodeWriter()
{
}


void OSPCodeWriter::writeHeaderIncludes(std::ostream& ostr)
{
	CodeWriter::writeHeaderIncludes(ostr);
	ostr << "#include \"Poco/OSP/Web/WebRequestHandlerFactory.h\"\n";
	ostr << "#include \"Poco/OSP/BundleContext.h\"\n";
}


void OSPCodeWriter::writeHandlerClass(std::ostream& ostr)
{
	std::string base(page().get("page.baseClass", "Poco::Net::HTTPRequestHandler"));

	handlerClass(ostr, base, "Poco::OSP::BundleContext::Ptr");
}


void OSPCodeWriter::writeHandlerMembers(std::ostream& ostr)
{
	std::string base(page().get("page.baseClass", ""));
	if (base.empty())
	{
		ostr << "\n";
		ostr << "protected:\n";
		ostr << "\tPoco::OSP::BundleContext::Ptr context() const\n";
		ostr << "\t{\n";
		ostr << "\t\treturn _pContext;\n";
		ostr << "\t}\n";
		ostr << "\n";
		ostr << "private:\n";
		ostr << "\tPoco::OSP::BundleContext::Ptr _pContext;\n";
	}
}


void OSPCodeWriter::writeFactoryClass(std::ostream& ostr)
{
	ostr << "\n\n";
	factoryClass(ostr, "Poco::OSP::Web::WebRequestHandlerFactory");
}


void OSPCodeWriter::writeImplIncludes(std::ostream& ostr)
{
	CodeWriter::writeImplIncludes(ostr);
	if (page().has("page.session"))
	{
		ostr << "#include \"Poco/OSP/Web/WebSession.h\"\n";
		ostr << "#include \"Poco/OSP/Web/WebSessionManager.h\"\n";
		ostr << "#include \"Poco/OSP/ServiceRegistry.h\"\n";
	}
}


void OSPCodeWriter::writeConstructor(std::ostream& ostr)
{
	std::string base(page().get("page.baseClass", ""));
	ostr << clazz() << "::" << clazz() << "(Poco::OSP::BundleContext::Ptr pContext):\n";
	if (base.empty())
	{
		ostr << "\t_pContext(pContext)\n";
	}
	else
	{
		ostr << "\t" << base << "(pContext)\n";
	}
	ostr << "{\n}\n";
	ostr << "\n\n";
}


void OSPCodeWriter::writeSession(std::ostream& ostr)
{
	if (page().has("page.session"))
	{
		std::string session = page().get("page.session");
		std::string sessionCode;
		if (session.empty()) return;
		if (session[0] == '@')
			sessionCode = "context()->thisBundle()->properties().getString(\"" + session.substr(1) + "\")";
		else
			sessionCode = "\"" + session + "\"";
		std::string sessionTimeoutCode = page().get("page.sessionTimeout", "30");
		int sessionTimeout;
		if (!Poco::NumberParser::tryParse(sessionTimeoutCode, sessionTimeout))
		{
			sessionTimeoutCode = "context()->thisBundle()->properties().getInt(\"" + sessionTimeoutCode + "\")";
		}
		ostr << "\tPoco::OSP::Web::WebSession::Ptr session;\n";
		ostr << "\t{\n";
		ostr << "\t\tPoco::OSP::ServiceRef::Ptr pWebSessionManagerRef = context()->registry().findByName(Poco::OSP::Web::WebSessionManager::SERVICE_NAME);\n";
		ostr << "\t\tif (pWebSessionManagerRef)\n";
		ostr << "\t\t{\n";
		ostr << "\t\t\tPoco::OSP::Web::WebSessionManager::Ptr pWebSessionManager = pWebSessionManagerRef->castedInstance<Poco::OSP::Web::WebSessionManager>();\n";
		if (page().get("page.createSession", "true") != "false")
		{
			ostr << "\t\t\tsession = pWebSessionManager->get(" << sessionCode << ", request, " << sessionTimeoutCode << ", context());\n";
		}
		else
		{
			ostr << "\t\t\tsession = pWebSessionManager->find(" << sessionCode << ", request);\n";
		}
		ostr << "\t\t}\n";
		ostr << "\t}\n";
	}
}


void OSPCodeWriter::writeFactory(std::ostream& ostr)
{
	ostr << "\n\n";
	factoryImpl(ostr, "context()");
}
