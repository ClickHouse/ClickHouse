//
// CodeWriter.cpp
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CodeWriter.h"
#include "Page.h"
#include "Poco/Path.h"
#include "Poco/StringTokenizer.h"
#include "Poco/String.h"


using Poco::Path;
using Poco::StringTokenizer;


CodeWriter::CodeWriter(const Page& page, const std::string& clazz):
	_page(page),
	_class(clazz)
{
}


CodeWriter::~CodeWriter()
{
}


void CodeWriter::writeHeader(std::ostream& ostr, const std::string& headerFileName)
{
	beginGuard(ostr, headerFileName);
	writeHeaderIncludes(ostr);
	ostr << "\n\n";

	std::string decls(_page.headerDecls().str());
	if (!decls.empty())
	{
		ostr << decls << "\n\n";
	}

	beginNamespace(ostr);
	writeHandlerClass(ostr);
	writeFactoryClass(ostr);
	endNamespace(ostr);
	endGuard(ostr, headerFileName);
}


void CodeWriter::writeImpl(std::ostream& ostr, const std::string& headerFileName)
{
	ostr << "#include \"" << headerFileName << "\"\n";
	writeImplIncludes(ostr);
	if (_page.getBool("page.compressed", false))
	{
		ostr << "#include \"Poco/DeflatingStream.h\"\n";
	}
	if (_page.getBool("page.buffered", false))
	{
		ostr << "#include \"Poco/StreamCopier.h\"\n";
		ostr << "#include <sstream>\n";
	}
	ostr << "\n\n";

	std::string decls(_page.implDecls().str());
	if (!decls.empty())
	{
		ostr << decls << "\n\n";
	}

	beginNamespace(ostr);

	std::string path = _page.get("page.path", "");
	if (!path.empty())
	{
		ostr << "\tconst std::string " << _class << "::PATH(\"" << path << "\");\n\n\n";
	}

	writeConstructor(ostr);
	writeHandler(ostr);
	writeFactory(ostr);
	endNamespace(ostr);
	writeManifest(ostr);
}


void CodeWriter::beginNamespace(std::ostream& ostr)
{
	std::string ns = _page.get("page.namespace", "");
	if (!ns.empty())
	{
		StringTokenizer tok(ns, ":", StringTokenizer::TOK_IGNORE_EMPTY | StringTokenizer::TOK_TRIM);
		for (StringTokenizer::Iterator it = tok.begin(); it != tok.end(); ++it)
		{
			ostr << "namespace " << *it << " {\n";
		}
		ostr << "\n\n";
	}
}

void CodeWriter::endNamespace(std::ostream& ostr)
{
	std::string ns = _page.get("page.namespace", "");
	if (!ns.empty())
	{
		ostr << "\n\n";
		StringTokenizer tok(ns, ":", StringTokenizer::TOK_IGNORE_EMPTY | StringTokenizer::TOK_TRIM);
		for (StringTokenizer::Iterator it = tok.begin(); it != tok.end(); ++it)
		{
			ostr << "} ";
		}
		ostr << "// namespace " << ns << "\n";
	}
}


void CodeWriter::beginGuard(std::ostream& ostr, const std::string& headerFileName)
{
	Path p(headerFileName);
	std::string guard(p.getBaseName());
	Poco::translateInPlace(guard, ".-", "__");
	guard += "_INCLUDED";

	ostr << "#ifndef " << guard << "\n";
	ostr << "#define " << guard << "\n";
	ostr << "\n\n";
}


void CodeWriter::endGuard(std::ostream& ostr, const std::string& headerFileName)
{
	Path p(headerFileName);
	std::string guard(p.getBaseName());
	Poco::translateInPlace(guard, ".-", "__");
	guard += "_INCLUDED";
	ostr << "\n\n";
	ostr << "#endif // " << guard << "\n";
}


void CodeWriter::handlerClass(std::ostream& ostr, const std::string& base, const std::string& ctorArg)
{
	std::string exprt(_page.get("page.export", ""));
	if (!exprt.empty()) exprt += ' ';

	ostr << "class " << exprt << _class << ": public " << base << "\n";
	ostr << "{\n";
	ostr << "public:\n";
	if (!ctorArg.empty())
	{
		ostr << "\t" << _class << "(" << ctorArg << ");\n";
		ostr << "\n";
	}
	ostr << "\tvoid handleRequest(Poco::Net::HTTPServerRequest& request, Poco::Net::HTTPServerResponse& response);\n";
	writeHandlerMembers(ostr);

	std::string path = _page.get("page.path", "");
	if (!path.empty())
	{
		ostr << "\n\tstatic const std::string PATH;\n";
	}

	ostr << "};\n";
}


void CodeWriter::factoryClass(std::ostream& ostr, const std::string& base)
{
	ostr << "class " << _class << "Factory: public " << base << "\n";
	ostr << "{\n";
	ostr << "public:\n";
	ostr << "\tPoco::Net::HTTPRequestHandler* createRequestHandler(const Poco::Net::HTTPServerRequest& request);\n";
	ostr << "};\n";
}


void CodeWriter::factoryImpl(std::ostream& ostr, const std::string& arg)
{
	ostr << "Poco::Net::HTTPRequestHandler* " << _class << "Factory::createRequestHandler(const Poco::Net::HTTPServerRequest& request)\n";
	ostr << "{\n";
	ostr << "\treturn new " << _class << "(" << arg << ");\n";
	ostr << "}\n";
}


void CodeWriter::writeHeaderIncludes(std::ostream& ostr)
{
	ostr << "#include \"Poco/Net/HTTPRequestHandler.h\"\n";
}


void CodeWriter::writeHandlerClass(std::ostream& ostr)
{
	std::string base(_page.get("page.baseClass", "Poco::Net::HTTPRequestHandler"));
	std::string ctorArg;
	ctorArg = _page.get("page.context", _page.get("page.ctorArg", ""));

	handlerClass(ostr, base, ctorArg);
}


void CodeWriter::writeHandlerMembers(std::ostream& ostr)
{
	std::string context(_page.get("page.context", ""));
	if (!context.empty())
	{
		ostr << "\n";
		ostr << "protected:\n";
		ostr << "\t" << context << " context() const\n";
		ostr << "\t{\n";
		ostr << "\t\treturn _context;\n";
		ostr << "\t}\n";
		ostr << "\n";
		ostr << "private:\n";
		ostr << "\t" << context << " _context;\n";
	}
}


void CodeWriter::writeFactoryClass(std::ostream& ostr)
{
}


void CodeWriter::writeImplIncludes(std::ostream& ostr)
{
	ostr << "#include \"Poco/Net/HTTPServerRequest.h\"\n";
	ostr << "#include \"Poco/Net/HTTPServerResponse.h\"\n";
	ostr << "#include \"Poco/Net/HTMLForm.h\"\n";
}


void CodeWriter::writeConstructor(std::ostream& ostr)
{
	std::string base(_page.get("page.baseClass", "Poco::Net::HTTPRequestHandler"));
	std::string context(_page.get("page.context", ""));
	std::string ctorArg(_page.get("page.ctorArg", ""));
	if (!context.empty())
	{
		ostr << _class << "::" << _class << "(" << context << " context):\n";
		ostr << "\t_context(context)\n";
		ostr << "{\n}\n";
		ostr << "\n\n";
	}
	else if (!ctorArg.empty())
	{
		ostr << _class << "::" << _class << "(" << ctorArg << " arg):\n";
		ostr << "\t" << base << "(arg)\n";
		ostr << "{\n}\n";
		ostr << "\n\n";
	}
}


void CodeWriter::writeHandler(std::ostream& ostr)
{
	ostr << "void " << _class << "::handleRequest(Poco::Net::HTTPServerRequest& request, Poco::Net::HTTPServerResponse& response)\n";
	ostr << "{\n";
	writeResponse(ostr);
	writeSession(ostr);
	if (_page.has("page.precondition"))
	{
		ostr << "\tif (!(" << _page.get("page.precondition") << ")) return;\n\n";
	}
	writeForm(ostr);
	ostr << _page.preHandler().str();
	writeContent(ostr);
	ostr << "}\n";
}


void CodeWriter::writeFactory(std::ostream& ostr)
{
}


void CodeWriter::writeManifest(std::ostream& ostr)
{
}


void CodeWriter::writeSession(std::ostream& ostr)
{
}


void CodeWriter::writeForm(std::ostream& ostr)
{
	if (_page.getBool("page.form", true))
	{
		std::string partHandler(_page.get("page.formPartHandler", ""));
		if (!partHandler.empty())
		{
			ostr << "\t" << partHandler << " cpspPartHandler(*this);\n";
		}
		ostr << "\tPoco::Net::HTMLForm form(request, request.stream()";
		if (!partHandler.empty())
		{
			ostr << ", cpspPartHandler";
		}
		ostr << ");\n";
	}
}


void CodeWriter::writeResponse(std::ostream& ostr)
{
	std::string contentType(_page.get("page.contentType", "text/html"));
	std::string contentLang(_page.get("page.contentLanguage", ""));
	std::string contentSecurityPolicy(_page.get("page.contentSecurityPolicy", ""));
	std::string cacheControl(_page.get("page.cacheControl", ""));
	bool buffered(_page.getBool("page.buffered", false));
	bool chunked(_page.getBool("page.chunked", !buffered));
	bool compressed(_page.getBool("page.compressed", false));
	if (buffered) compressed = false;
	if (compressed) chunked = true;

	if (chunked)
	{
		ostr << "\tresponse.setChunkedTransferEncoding(true);\n";
	}

	ostr << "\tresponse.setContentType(\"" << contentType << "\");\n";
	if (!contentLang.empty())
	{
		ostr << "\tif (request.has(\"Accept-Language\"))\n"
			 << "\t\tresponse.set(\"Content-Language\", \"" << contentLang << "\");\n";
	}
	if (!contentSecurityPolicy.empty())
	{
		ostr << "\tresponse.set(\"Content-Secure-Policy\", \"" << contentSecurityPolicy << "\");\n";
	}
	if (compressed)
	{
		ostr << "\tbool _compressResponse(request.hasToken(\"Accept-Encoding\", \"gzip\"));\n"
		     << "\tif (_compressResponse) response.set(\"Content-Encoding\", \"gzip\");\n";
	}
	if (!cacheControl.empty())
	{
		ostr << "\tresponse.set(\"Cache-Control\", \"" << cacheControl << "\");\n";
	}
	ostr << "\n";
}


void CodeWriter::writeContent(std::ostream& ostr)
{
	bool buffered(_page.getBool("page.buffered", false));
	bool chunked(_page.getBool("page.chunked", !buffered));
	bool compressed(_page.getBool("page.compressed", false));
	int compressionLevel(_page.getInt("page.compressionLevel", 1));
	if (buffered) compressed = false;
	if (compressed) chunked = true;

	if (buffered)
	{
		ostr << "\tstd::stringstream responseStream;\n";
		ostr << cleanupHandler(_page.handler().str());
		if (!chunked)
		{
			ostr << "\tresponse.setContentLength(static_cast<int>(responseStream.tellp()));\n";
		}
		ostr << "\tPoco::StreamCopier::copyStream(responseStream, response.send());\n";
	}
	else if (compressed)
	{
		ostr << "\tstd::ostream& _responseStream = response.send();\n"
		     << "\tPoco::DeflatingOutputStream _gzipStream(_responseStream, Poco::DeflatingStreamBuf::STREAM_GZIP, " << compressionLevel << ");\n"
		     << "\tstd::ostream& responseStream = _compressResponse ? _gzipStream : _responseStream;\n";
		ostr << cleanupHandler(_page.handler().str());
		ostr << "\tif (_compressResponse) _gzipStream.close();\n";
	}
	else
	{
		ostr << "\tstd::ostream& responseStream = response.send();\n";
		ostr << cleanupHandler(_page.handler().str());
	}
}


std::string CodeWriter::cleanupHandler(std::string handler)
{
	static const std::string EMPTY_WRITE("\tresponseStream << \"\";\n");
	static const std::string NEWLINE_WRITE("\tresponseStream << \"\\n\";\n");
	static const std::string DOUBLE_NEWLINE_WRITE("\tresponseStream << \"\\n\";\n\tresponseStream << \"\\n\";\n");
	static const std::string EMPTY;

	// remove empty writes
	Poco::replaceInPlace(handler, EMPTY_WRITE, EMPTY);
	// remove consecutive newlines
	while (handler.find(DOUBLE_NEWLINE_WRITE) != std::string::npos)
	{
		Poco::replaceInPlace(handler, DOUBLE_NEWLINE_WRITE, NEWLINE_WRITE);
	}
	return handler;
}

