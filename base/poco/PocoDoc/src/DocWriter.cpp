//
// DocWriter.cpp
//
// Copyright (c) 2005-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DocWriter.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Path.h"
#include "Poco/String.h"
#include "Poco/DateTime.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/RegularExpression.h"
#include "Poco/Exception.h"
#include "Poco/Format.h"
#include "Poco/Util/Application.h"
#include "Poco/CppParser/Struct.h"
#include "Poco/CppParser/Enum.h"
#include "Poco/CppParser/EnumValue.h"
#include "Poco/CppParser/Function.h"
#include "Poco/CppParser/Parameter.h"
#include "Poco/CppParser/TypeDef.h"
#include "Poco/CppParser/Variable.h"
#include <map>
#include <fstream>
#include <sstream>
#include <cctype>


using Poco::NumberFormatter;
using Poco::Path;
using Poco::DateTime;
using Poco::DateTimeFormat;
using Poco::DateTimeFormatter;
using Poco::RegularExpression;
using Poco::format;
using Poco::Util::Application;
using namespace Poco::CppParser;


std::string DocWriter::_language;
DocWriter::StringMap DocWriter::_strings;
Poco::Logger* DocWriter::_pLogger(0);
const std::string DocWriter::RFC_URI("https://www.ietf.org/rfc/rfc");
const std::string DocWriter::GITHUB_POCO_URI("https://github.com/pocoproject/poco");


DocWriter::DocWriter(const NameSpace::SymbolTable& symbols, const std::string& path, bool prettifyCode, bool noFrames):
	_prettifyCode(prettifyCode),
	_noFrames(noFrames),
	_htmlMode(false),
	_literalMode(false),
	_symbols(symbols),
	_path(path),
	_pNameSpace(0),
	_pendingLine(false),
	_indent(0),
	_titleId(0)
{
	_pLogger = &Poco::Logger::get("DocWriter");
	
	Application& app = Application::instance();
	_language = app.config().getString("PocoDoc.language", "EN");
	
	logger().information(std::string("Loading translation strings [") + _language + "]");

	loadStrings(_language);
}


DocWriter::~DocWriter()
{
}


void DocWriter::addPage(const std::string& path)
{
	Page page;
	page.path = path;
	Path p(path);
	p.setExtension("html");
	page.fileName = p.getFileName();
	_pages[page.fileName] = page;
}


void DocWriter::write()
{	
	writePages();
	int nameSpaces = 0;
	int classes    = 0;
	for (NameSpace::SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		switch (it->second->kind())
		{
		case Symbol::SYM_NAMESPACE:
			++nameSpaces;
			logger().information("Generating namespace " + it->second->fullName());
			writeNameSpace(static_cast<const NameSpace*>(it->second));
			break;
		case Symbol::SYM_STRUCT:
			++classes;
			logger().information("Generating class/struct " + it->second->fullName());
			writeClass(static_cast<const Struct*>(it->second));
			break;	
		default:
			break;
		}
	}
	logger().information("Generating overview and index");
	writeNavigation();
	logger().information(NumberFormatter::format(nameSpaces) + " namespaces, " + NumberFormatter::format(classes) + " classes.");
	Application& app = Application::instance();
	app.config().setInt("PocoDoc.statistics.namespaces", nameSpaces);
	app.config().setInt("PocoDoc.statistics.classes", classes);
	std::ostringstream pageIndexStream;
	writePageIndex(pageIndexStream);
	app.config().setString("PocoDoc.pageIndex", pageIndexStream.str());
	std::ostringstream nameSpaceIndexStream;
	writeNameSpaceIndex(nameSpaceIndexStream);
	app.config().setString("PocoDoc.nameSpaceIndex", nameSpaceIndexStream.str());
}


void DocWriter::writeNavigation()
{
	Application& app = Application::instance();
	std::string software(app.config().getString("PocoDoc.software", ""));

	std::string path(pathFor("navigation.html"));
	std::ofstream ostr(path.c_str());
	if (!ostr.good()) throw Poco::CreateFileException(path);
	writeHeader(ostr, tr("Navigation"), "js/iframeResizer.contentWindow.min.js");
	beginBody(ostr);
	ostr << "<h3 class=\"overview\"><a href=\"index.html\" target=\"_top\">" << htmlize(software) << "</a></h3>\n";
	
	if (!_pages.empty())
	{
		ostr << "<h4>" << tr("Guides") << "</h4>\n";
		ostr << "<ul class=\"collapsibleList\">\n";
		std::set<std::string> categories;
		for (PageMap::const_iterator it = _pages.begin(); it != _pages.end(); ++it)
		{
			categories.insert(it->second.category);
		}
		for (std::set<std::string>::const_iterator it = categories.begin(); it != categories.end(); ++it)
		{
			std::string node = "category-";
			node += *it;
			ostr << "<li id=\"" << node << "\">";
			ostr << tr(*it) << "\n";
			writeCategoryIndex(ostr, *it, "_top");
			ostr << "</li>\n";
		}
		ostr << "</ul>\n";
	}
	
	ostr << "<h4>" << tr("Namespaces") << "</h4>\n";
	ostr << "<ul>\n";
	
	std::map<std::string, Symbol*> namespaces; // sort namespaces by full name
	for (NameSpace::SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		if (it->second->kind() == Symbol::SYM_NAMESPACE)
		{
			namespaces[it->second->fullName()] = it->second;
		}
	}
	for (std::map<std::string, Symbol*>::const_iterator it = namespaces.begin(); it != namespaces.end(); ++it)
	{
		ostr << "<li>";
		writeTargetLink(ostr, baseNameFor(it->second) + ".html", it->second->fullName(), "_top");
		ostr << "</li>\n";
	}
	ostr << "</ul>\n";
	ostr << "<h4>" << tr("Packages") << "</h4>\n";
	ostr << "<ul class=\"collapsibleList\">\n";
	std::set<std::string> libs;
	libraries(libs);
	for (std::set<std::string>::const_iterator itl = libs.begin(); itl != libs.end(); ++itl)
	{
		std::string node = "library-";
		node += *itl;
		ostr << "<li id=\"" << node << "\">" << *itl << "\n";
		std::set<std::string> pkgs;
		packages(*itl, pkgs);
		ostr << "<ul>\n";
		for (std::set<std::string>::const_iterator itp = pkgs.begin(); itp != pkgs.end(); ++itp)
		{
			std::string uri("package-");
			uri += makeFileName(*itl);
			uri += '.';
			uri += makeFileName(*itp);
			uri += ".html";
			ostr << "<li>";
			writeTargetLink(ostr, uri, *itp, "_top");
			ostr << "</li>\n";
			writePackage(uri, *itl, *itp);
		}
		ostr << "</ul>\n";
	}
	ostr << "</ul>\n";
	ostr << "<div>&nbsp;</div>\n"; // workaround to avoid cutting off a few pixels from last line
	endBody(ostr);
	ostr << "<script>CollapsibleLists.apply(true)</script>" << std::endl;
	writeFooter(ostr);
}


void DocWriter::writePageIndex(std::ostream& ostr)
{
	std::set<std::string> categories;
	for (PageMap::const_iterator it = _pages.begin(); it != _pages.end(); ++it)
	{
		categories.insert(it->second.category);
	}

	ostr << "<table class=\"index\">" << std::endl;
	int column = 0;
	for (std::set<std::string>::const_iterator it = categories.begin(); it != categories.end(); ++it)
	{
		if (column == 0)
		{
			ostr << "<tr>" << std::endl;
		}
		ostr << "<td>" << std::endl;
		ostr << "<h4>" << htmlize(tr(*it)) << "</h4>";
		writeCategoryIndex(ostr, *it, "");
		ostr << "</td>" << std::endl;
		++column;
		if (column == PAGE_INDEX_COLUMNS)
		{
			ostr << "</tr>" << std::endl;
			column = 0;
		}
	}
	if (column != 0)
	{
		while (column < PAGE_INDEX_COLUMNS)
		{
			ostr << "<td></td>" << std::endl;
			++column;
		}
		ostr << "</tr>" << std::endl;
	}
	ostr << "</table>";
}


void DocWriter::writeNameSpaceIndex(std::ostream& ostr)
{
	std::map<std::string, const NameSpace*> nsMap;
	for (NameSpace::SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		if (it->second->kind() == Symbol::SYM_NAMESPACE)
		{
			const NameSpace* pNameSpace = static_cast<const NameSpace*>(it->second);
			nsMap[it->second->fullName()] = pNameSpace;
		}
	}

	ostr << "<table class=\"index\">" << std::endl;
	int column = 0;
	for (std::map<std::string, const NameSpace*>::const_iterator it = nsMap.begin(); it != nsMap.end(); ++it)
	{
		const NameSpace* pNameSpace = it->second;
		if (column == 0)
		{
			ostr << "<tr>" << std::endl;
		}
		ostr << "<td>" << std::endl;
		writeLink(ostr, uriFor(pNameSpace), pNameSpace->fullName());
		ostr << "</td>" << std::endl;
		++column;
		if (column == NAMESPACE_INDEX_COLUMNS)
		{
			ostr << "</tr>" << std::endl;
			column = 0;
		}
	}
	if (column != 0)
	{
		while (column < NAMESPACE_INDEX_COLUMNS)
		{
			ostr << "<td></td>" << std::endl;
			++column;
		}
		ostr << "</tr>" << std::endl;
	}
	ostr << "</table>";
}


void DocWriter::writeEclipseTOC()
{
	Application& app = Application::instance();
	std::string software(app.config().getString("PocoDoc.software", ""));

	std::string path(pathFor("toc.xml"));
	Poco::Path p(path);
	std::string dir = p[p.depth() - 1];
	dir.append("/");
	p.popDirectory();
	std::ofstream ostr(p.toString().c_str());
	if (!ostr.good()) throw Poco::CreateFileException(path);
	
	ostr << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" << std::endl;
	ostr << "<toc id=\"poco\" label=\"" << htmlize(software) << " " << tr("Reference") << "\" topic=\"" << dir << "welcome.html\">" << std::endl;
	
	if (!_pages.empty())
	{
		ostr << "<topic label=\"" << tr("Guides") << "\">" << std::endl;
		std::set<std::string> categories;
		for (PageMap::const_iterator it = _pages.begin(); it != _pages.end(); ++it)
		{
			categories.insert(it->second.category);
		}
		for (std::set<std::string>::const_iterator it = categories.begin(); it != categories.end(); ++it)
		{
			ostr << "<topic label=\"" << *it << "\">" << std::endl;	
			for (PageMap::const_iterator itp = _pages.begin(); itp != _pages.end(); ++itp)
			{
				if (itp->second.category == *it)
				{
					ostr << "<topic label=\"" << itp->second.title << "\" href=\"" << dir << itp->second.fileName << "\"/>" << std::endl;
				}
			}
			ostr << "</topic>" << std::endl;	
		}
		ostr << "</topic>" << std::endl;	
	}
	
	ostr << "<topic label=\"" << tr("Namespaces") << "\">" << std::endl;

	std::map<std::string, Symbol*> namespaces; // sort namespaces by full name
	for (NameSpace::SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		if (it->second->kind() == Symbol::SYM_NAMESPACE)
		{
			namespaces[it->second->fullName()] = it->second;
		}
	}
	for (std::map<std::string, Symbol*>::const_iterator it = namespaces.begin(); it != namespaces.end(); ++it)
	{
		ostr << "<topic label=\"" << it->second->fullName() << "\" href=\"" << dir << baseNameFor(it->second) << ".html" << "\"/>" << std::endl;
	}
	ostr << "</topic>" << std::endl;
	
	ostr << "<topic label=\"" << tr("Packages") << "\">" << std::endl;

	std::set<std::string> libs;
	libraries(libs);
	for (std::set<std::string>::const_iterator itl = libs.begin(); itl != libs.end(); ++itl)
	{
		ostr << "<topic label=\"" << *itl << "\">" << std::endl;
		std::set<std::string> pkgs;
		packages(*itl, pkgs);
		for (std::set<std::string>::const_iterator itp = pkgs.begin(); itp != pkgs.end(); ++itp)
		{
			std::string uri("package-");
			uri += makeFileName(*itl);
			uri += '.';
			uri += makeFileName(*itp);
			uri += "-index.html";
			
			ostr << "<topic label=\"" << *itp << "\" href=\"" << dir << uri << "\"/>" << std::endl;
		}
		ostr << "</topic>" << std::endl;
	}

	ostr << "</topic>" << std::endl;
	
	ostr << "<topic label=\"" << tr("All_Symbols") << "\" href=\"" << dir << "index-all.html\"/>" << std::endl;
	
	ostr << "</toc>" << std::endl;
}


void DocWriter::writeClass(const Struct* pStruct)
{
	_pNameSpace = pStruct;
	std::string path(pathFor(fileNameFor(pStruct)));
	std::ofstream ostr(path.c_str());
	if (!ostr.good()) throw Poco::CreateFileException(path);
	std::string header;
	if (pStruct->isClass())
		header += tr("Class") + " ";
	else
		header += tr("Struct") + " ";
	header += pStruct->fullName();
	writeHeader(ostr, header, "js/iframeResizer.min.js");
	writeTitle(ostr, pStruct->nameSpace(), pStruct->declaration() + (pStruct->isFinal() ? " final" : ""));
	beginBody(ostr);
	writeNavigationFrame(ostr, "library", pStruct->getLibrary());
	beginContent(ostr);
	writeFileInfo(ostr, pStruct);
	const std::string& doc = pStruct->getDocumentation();
	if (pStruct->attrs().has("deprecated"))
	{
		writeDeprecated(ostr, pStruct->isClass() ? "class" : "struct");
	}
	if (!doc.empty())
	{
		if (doc.find("TODO") != std::string::npos)
			logger().notice(std::string("TODO in class documentation for ") + pStruct->fullName());
	
		writeSubTitle(ostr, tr("Description"));
		writeDescription(ostr, pStruct->getDocumentation());
	}
	else if (pStruct->isPublic() && !pStruct->isDerived())
	{
		logger().notice(std::string("Public root class has no documentation: ") + pStruct->fullName());
	}
	writeInheritance(ostr, pStruct);
	writeMethodSummary(ostr, pStruct);
	writeNestedClasses(ostr, pStruct);
	writeTypes(ostr, pStruct);
	writeEnums(ostr, pStruct);
	writeConstructors(ostr, pStruct);
	writeDestructor(ostr, pStruct);
	writeMethods(ostr, pStruct);
	writeVariables(ostr, pStruct);
	writeCopyright(ostr);
	endContent(ostr);
	endBody(ostr);
	writeFooter(ostr);
}


void DocWriter::writeNameSpace(const NameSpace* pNameSpace)
{
	_pNameSpace = pNameSpace;
	std::string path(pathFor(fileNameFor(pNameSpace)));
	std::ofstream ostr(path.c_str());
	if (!ostr.good()) throw Poco::CreateFileException(path);
	writeHeader(ostr, tr("Namespace") + " " + pNameSpace->fullName(), "js/iframeResizer.min.js");
	writeTitle(ostr, pNameSpace->nameSpace(), std::string("namespace ") + pNameSpace->name());
	beginBody(ostr);
	writeNavigationFrame(ostr, "", "");
	beginContent(ostr);
	writeSubTitle(ostr, tr("Overview"));
	writeNameSpacesSummary(ostr, pNameSpace);
	writeClassesSummary(ostr, pNameSpace);
	writeTypesSummary(ostr, pNameSpace);
	writeFunctionsSummary(ostr, pNameSpace);
	writeNameSpaces(ostr, pNameSpace);
	writeClasses(ostr, pNameSpace);
	writeTypes(ostr, pNameSpace);
	writeEnums(ostr, pNameSpace);
	writeFunctions(ostr, pNameSpace);
	writeVariables(ostr, pNameSpace);
	writeCopyright(ostr);
	endContent(ostr);
	endBody(ostr);
	writeFooter(ostr);
}


void DocWriter::writePackage(const std::string& file, const std::string& library, const std::string& package)
{
	std::string path(pathFor(file));
	std::ofstream ostr(path.c_str());
	if (!ostr.good()) throw Poco::CreateFileException(path);
	writeHeader(ostr, tr("Package_Index"), "js/iframeResizer.min.js");
	writeTitle(ostr, tr("Library") + " " + library, tr("Package") + " " + package);
	beginBody(ostr);
	writeNavigationFrame(ostr, "library", library);
	beginContent(ostr);
	writeSubTitle(ostr, tr("Overview"));

	ostr << "<p><b>" << tr("Classes") << ":</b> " << std::endl;
	bool first = true;
	std::string prevName;
	std::string prevNameSpace;
	bool haveFunctions = false;
	for (NameSpace::SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		Symbol* pSym = it->second;
		Struct* pStruct = dynamic_cast<Struct*>(pSym);
		Function* pFunc = dynamic_cast<Function*>(pSym);
		if (pFunc && pFunc->isFunction() && pFunc->getLibrary() == library && pFunc->getPackage() == package) haveFunctions = true;
		bool isClass = pStruct && pStruct->getAccess() == Symbol::ACC_PUBLIC;
		if (isClass)
		{
			if (pSym->getLibrary() == library && pSym->getPackage() == package)
			{
				const std::string& name = pSym->name();
				const std::string& nameSpace = pSym->nameSpace()->fullName();
				if (name != prevName || nameSpace != prevNameSpace)
				{
					writeNameListItem(ostr, it->second->name(), it->second, pSym->nameSpace(), first);
					prevName = name;
					prevNameSpace = nameSpace;
				}
			}
		}
	}
	ostr << "</p>" << std::endl;

	prevName.clear();
	prevNameSpace.clear();
	if (haveFunctions)
	{
		ostr << "<p><b>" << tr("Functions") << ":</b> " << std::endl;
		first = true;
		for (NameSpace::SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
		{
			Symbol* pSym = it->second;
			Function* pFunc = dynamic_cast<Function*>(pSym);
			if (pFunc && pFunc->isFunction())
			{
				if (pSym->getLibrary() == library && pSym->getPackage() == package)
				{
					const std::string& name = pSym->name();
					const std::string& nameSpace = pSym->nameSpace()->fullName();
					if (name != prevName || nameSpace != prevNameSpace)
					{
						writeNameListItem(ostr, it->second->name(), it->second, pSym->nameSpace(), first);
						prevName = name;
						prevNameSpace = nameSpace;
					}
				}
			}
		}
		ostr << "</p>" << std::endl;
	}

	prevName.clear();
	prevNameSpace.clear();
	writeSubTitle(ostr, tr("Classes"));
	for (NameSpace::SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		Symbol* pSym = it->second;
		Struct* pStruct = dynamic_cast<Struct*>(pSym);
		bool isClass = pStruct && pStruct->getAccess() == Symbol::ACC_PUBLIC;
		if (isClass)
		{
			if (pSym->getLibrary() == library && pSym->getPackage() == package)
			{
				const std::string& name = pSym->name();
				const std::string& nameSpace = pSym->nameSpace()->fullName();
				if (name != prevName || nameSpace != prevNameSpace)
				{
					writeClassSummary(ostr, pStruct);
					prevName = name;
					prevNameSpace = nameSpace;
				}
			}
		}
	}

	writeCopyright(ostr);
	endContent(ostr);
	endBody(ostr);
	writeFooter(ostr);
}


std::string DocWriter::fileNameFor(const Symbol* pNameSpace)
{
	std::string result(baseNameFor(pNameSpace));
	if (!result.empty()) result.append(".html");
	return result;
}


std::string DocWriter::baseNameFor(const Symbol* pNameSpace)
{
	std::string result;
	std::string fullName(pNameSpace->fullName());
	std::string::const_iterator it  = fullName.begin();
	std::string::const_iterator end = fullName.end();
	while (it != end)
	{
		if (*it == ':')
		{
			result += '.';
			++it;
		}
		else result += *it;
		if (it != end) ++it;
	}
	return result;
}


std::string DocWriter::pathFor(const std::string& file)
{
	Path p(_path);
	p.makeDirectory();
	p.setFileName(file);
	return p.toString();
}


std::string DocWriter::uriFor(const Symbol* pSymbol)
{
	const Function* pFunc = dynamic_cast<const Function*>(pSymbol);
	if (pFunc && pFunc->isConstructor())
		return fileNameFor(pSymbol->nameSpace());
	else if (dynamic_cast<const NameSpace*>(pSymbol))
		return fileNameFor(pSymbol);
	else
		return fileNameFor(pSymbol->nameSpace()) + "#" + NumberFormatter::format(pSymbol->id());
}


std::string DocWriter::makeFileName(const std::string& str)
{
	std::string result;
	for (std::string::const_iterator it = str.begin(); it != str.end(); ++it)
	{
		if (std::isalnum(*it))
			result += *it;
		else
			result += "_";
	}
	return result;
}


void DocWriter::writeHeader(std::ostream& ostr, const std::string& title, const std::string& extraScript)
{
	Application& app = Application::instance();
	std::string company(app.config().getString("PocoDoc.company", "Applied Informatics"));
	std::string charset(app.config().getString("PocoDoc.charset", "utf-8"));
	DateTime now;
	ostr << "<!DOCTYPE html>" << std::endl;
	ostr << "<html>" << std::endl;
	ostr << "<head>" << std::endl;
	ostr << "<title>" << htmlize(title) << "</title>" << std::endl;
	ostr << "<meta http-equiv=\"content-type\" content=\"text/html; charset=" << charset << "\"/>" << std::endl;
	ostr << "<meta name=\"author\" content=\"" << htmlize(company) << "\"/>" << std::endl;
	ostr << "<meta name=\"generator\" content=\"" << app.config().getString("PocoDoc.generator", "PocoDoc") << "\"/>" << std::endl;
	ostr << "<link rel=\"stylesheet\" href=\"css/styles.css\" type=\"text/css\"/>" << std::endl;
	if (_prettifyCode)
	{
		ostr << "<link href=\"css/prettify.css\" type=\"text/css\" rel=\"stylesheet\"/>" << std::endl;
		ostr << "<script type=\"text/javascript\" src=\"js/prettify.js\"></script>" << std::endl;
	}
	if (!extraScript.empty())
	{
		ostr << "<script type=\"text/javascript\" src=\"" << extraScript << "\"></script>" << std::endl;
	}
	ostr << "<script type=\"text/javascript\" src=\"js/CollapsibleLists.compressed.js\"></script>" << std::endl;
	ostr << "</head>" << std::endl;
	ostr << "<body";
	if (_prettifyCode)
		ostr << " onload=\"prettyPrint()\"";
	ostr << ">" << std::endl;
}


void DocWriter::writeFooter(std::ostream& ostr)
{
	Application& app = Application::instance();
	std::string googleAnalyticsCode(app.config().getString("PocoDoc.googleAnalyticsCode", ""));
	ostr << googleAnalyticsCode;
	ostr << "</body>" << std::endl;
	ostr << "</html>" << std::endl;
}


void DocWriter::writeCopyright(std::ostream& ostr)
{
	Application& app = Application::instance();
	std::string software(app.config().getString("PocoDoc.software", ""));
	std::string version(app.config().getString("PocoDoc.version", ""));
	std::string company(app.config().getString("PocoDoc.company", "Applied Informatics"));
	std::string companyURI(app.config().getString("PocoDoc.companyURI", "http://www.appinf.com/"));
	std::string licenseURI(app.config().getString("PocoDoc.licenseURI", ""));
	DateTime now;
	ostr << "<p class=\"footer\">";
	ostr << htmlize(software) << " " << htmlize(version) << "<br />\n";
	ostr << tr("Copyright") << " &copy; " << now.year() << ", ";
	writeTargetLink(ostr, companyURI, htmlize(company), "_blank");
	if (!licenseURI.empty())
	{
		ostr << " ";
		std::string license("(");
		license.append(tr("License"));
		license.append(")");
		writeTargetLink(ostr, licenseURI, htmlize(license), "_blank");
	}
	ostr << "</p>\n";
}


void DocWriter::writeTitle(std::ostream& ostr, const std::string& category, const std::string& title)
{
	Application& app = Application::instance();
	std::string headerImage(app.config().getString("PocoDoc.headerImage", ""));
	ostr << "<div class=\"header\">\n";
	if (!headerImage.empty())
	{
		ostr << "<img src=\"" << headerImage << "\" alt=\"\">\n";
	}
	ostr << "<h1 class=\"category\">";
	if (category.empty())
		ostr << "&nbsp;";
	else
		ostr << htmlize(category);
	ostr << "</h1>\n";
	ostr << "<h1 class=\"title\">"  << htmlize(title) << "</h1>";
	ostr << "\n</div>\n";
}


void DocWriter::writeTitle(std::ostream& ostr, const NameSpace* pNameSpace, const std::string& title)
{
	Application& app = Application::instance();
	std::string headerImage(app.config().getString("PocoDoc.headerImage", ""));
	ostr << "<div class=\"header\">\n";
	if (!headerImage.empty())
	{
		ostr << "<img src=\"" << headerImage << "\" alt=\"\">\n";
	}
	const std::string& nameSpace = pNameSpace->fullName();
	if (!nameSpace.empty())
	{
		ostr << "<h1 class=\"namespace\">";
		writeLink(ostr, uriFor(pNameSpace), nameSpace, "namespace");
		ostr << "</h1>\n";
	}
	else
	{
		ostr << "<h1 class=\"namespace\">::</h1>\n";
	}

	std::string::size_type posFirstOpen = title.find_first_of('<');
	bool isTemplate = (posFirstOpen != std::string::npos);
	std::string::size_type posFirstClose = std::string::npos;
	std::string templateParam;
	std::string templateParamSpec;
	if (isTemplate)
	{
		int tempCount = 1;
		posFirstClose = posFirstOpen;
		while (tempCount != 0)
		{
			++posFirstClose;
			if (title[posFirstClose] == '>')
				--tempCount;
			if (title[posFirstClose] == '<')
				++tempCount;
		}
		++posFirstClose;
		templateParam = title.substr(0, posFirstClose);
		templateParamSpec = title.substr(posFirstClose+1);
	}

	if (isTemplate)
	{
		ostr << "<h1 class=\"template\">"
		     << htmlize(templateParam)
		     << "</h1>\n"
		     << "<h1 class=\"symbol\">"
		     << htmlize(templateParamSpec)
		     << "</h1>";
	}
	else
	{
		ostr << "<h1 class=\"symbol\">"
		     << htmlize(title)
		     << "</h1>";
	}
	ostr << "\n</div>\n";
}


void DocWriter::writeSubTitle(std::ostream& ostr, const std::string& title)
{
	ostr << "<h2>"
	     << htmlize(title)
	     << "</h2>\n";	
}


void DocWriter::writeNavigationFrame(std::ostream& ostr, const std::string& group, const std::string& item)
{
	std::string query;
	if (!group.empty() && !item.empty())
	{
		query = "?expand=";
		query += group;
		query += "-";
		query += item;
	}
	ostr << "<div id=\"navigation\">\n";
	ostr << "<iframe src=\"navigation.html" << query << "\" onload=\"iFrameResize(this);\" scrolling=\"no\"></iframe>\n";
	ostr << "</div>\n";
}


void DocWriter::beginBody(std::ostream& ostr)
{
	ostr << "<div class=\"body\">\n";
}


void DocWriter::endBody(std::ostream& ostr)
{
	ostr << "\n</div>\n";
}


void DocWriter::beginContent(std::ostream& ostr)
{
	ostr << "<div id=\"content\">\n";
}


void DocWriter::endContent(std::ostream& ostr)
{
	ostr << "\n</div>\n";
}


void DocWriter::writeDescription(std::ostream& ostr, const std::string& text)
{
	ostr << "<div class=\"description\">\n"
	     << "<p>";
	
	_titleId = 0;
	_htmlMode = false;
	TextState state = TEXT_PARAGRAPH;
	std::string::const_iterator it  = text.begin();
	std::string::const_iterator end = text.end();
	while (it != end)
	{
		std::string line;
		while (it != end && *it != '\n') line += *it++;
		writeDescriptionLine(ostr, line, state);
		if (it != end) ++it;
	}
	switch (state)
	{
	case TEXT_PARAGRAPH:
		ostr << "</p>";
		break;
	case TEXT_LIST:
		ostr << "</li>\n</ul>";
		break;
	case TEXT_OLIST:
		ostr << "</li>\n</ol>";
		break;
	case TEXT_LITERAL:
		ostr << "</pre>";
		break;
	default:
		break;
	}
	ostr << "\n</div>\n";
}


void DocWriter::writeDescriptionLine(std::ostream& ostr, const std::string& text, TextState& state)
{
	if (_htmlMode)
	{
		writeText(ostr, text);
	}
	else
	{
		TextState lineState = analyzeLine(text);
		if (lineState == TEXT_LITERAL && state != lineState)
		{
			_indent = 0;
			_pendingLine = false;
		}
		switch (lineState)
		{
		case TEXT_PARAGRAPH:
			switch (state)
			{
			case TEXT_PARAGRAPH:
				writeText(ostr, text);
				break;
			case TEXT_LIST:
				ostr << "</li>\n</ul>\n<p>";
				writeText(ostr, text);
				break;
			case TEXT_OLIST:
				ostr << "</li>\n</ol>\n<p>";
				writeText(ostr, text);
				break;
			case TEXT_LITERAL:
				ostr << "</pre>\n<p>";
				writeText(ostr, text);
				break;
			default:
				break;
			}
			state = TEXT_PARAGRAPH;
			break;
		case TEXT_LIST:
			switch (state)
			{
			case TEXT_PARAGRAPH:
				ostr << "</p>\n<ul>\n<li>";
				writeListItem(ostr, text);
				state = TEXT_LIST;
				break;
			case TEXT_LIST:
			case TEXT_OLIST:
				ostr << "</li>\n<li>";
				writeListItem(ostr, text);
				state = TEXT_LIST;
				break;
			case TEXT_LITERAL:
				writeLiteral(ostr, text);
				break;
			default:
				break;
			}
			break;
		case TEXT_OLIST:
			switch (state)
			{
			case TEXT_PARAGRAPH:
				ostr << "</p>\n<ol>\n<li>";
				writeOrderedListItem(ostr, text);
				state = TEXT_OLIST;
				break;
			case TEXT_LIST:
			case TEXT_OLIST:
				ostr << "</li>\n<li>";
				writeOrderedListItem(ostr, text);
				state = TEXT_OLIST;
				break;
			case TEXT_LITERAL:
				writeLiteral(ostr, text);
				break;
			default:
				break;
			}
			break;
		case TEXT_LITERAL:
			switch (state)
			{
			case TEXT_PARAGRAPH:
				ostr << "</p>\n<pre";
				if (_prettifyCode) ostr << " class=\"prettyprint\"";
				ostr << ">";
				writeLiteral(ostr, text);
				state = TEXT_LITERAL;
				break;
			case TEXT_LIST:
				writeText(ostr, text);
				state = TEXT_LIST;
				break;
			case TEXT_OLIST:
				writeText(ostr, text);
				state = TEXT_OLIST;
				break;
			case TEXT_LITERAL:
				writeLiteral(ostr, text);
				break;
			default:
				break;
			}
			break;
		case TEXT_WHITESPACE:
			switch (state)
			{
			case TEXT_PARAGRAPH:
				ostr << "</p>\n<p>";
				break;
			case TEXT_LIST:
				ostr << "</li>\n</ul>\n<p>";
				state = TEXT_PARAGRAPH;
				break;
			case TEXT_OLIST:
				ostr << "</li>\n</ol>\n<p>";
				state = TEXT_PARAGRAPH;
				break;
			case TEXT_LITERAL:
				writeLiteral(ostr, text);
				break;
			default:
				break;
			}
			break;
		}
	}
}


void DocWriter::writeSummary(std::ostream& ostr, const std::string& text, const std::string& uri)
{
	ostr << "<p>";
	std::string::const_iterator beg = text.begin();
	std::string::const_iterator it  = beg;
	std::string::const_iterator end = text.end();
	while (it != end && *it != '.') ++it;
	if (it != end) ++it;
	writeText(ostr, beg, it);
	if (!uri.empty())
	{
		ostr << "&nbsp;";
		writeImageLink(ostr, uri, "arrow.png", tr("more"));
	}
	ostr << "</p>\n";
}


DocWriter::TextState DocWriter::analyzeLine(const std::string& line)
{
	int nSpaces = 0;
	std::string::const_iterator it  = line.begin();
	std::string::const_iterator end = line.end();
	while (it != end && std::isspace(*it)) { ++it; ++nSpaces; }
	if (it == end)
		return TEXT_WHITESPACE;
	else if (nSpaces < 3)
		return TEXT_PARAGRAPH;
	else if (*it == '-' || *it == '*')
		return TEXT_LIST;
	else if (std::isdigit(*it))
	{
		++it;
		if (it != end && *it == '.')
			return TEXT_OLIST;
	}
	return TEXT_LITERAL;
}


std::string DocWriter::htmlizeName(const std::string& name)
{
	std::string result;
	for (std::string::const_iterator it = name.begin(); it != name.end(); ++it)
	{
		if (*it == ' ')
			result += "&nbsp;";
		else
			result += htmlize(*it);
	}
	return result;
}


void DocWriter::writeText(std::ostream& ostr, const std::string& text)
{
	std::string::const_iterator it(text.begin());
	std::string::const_iterator end(text.end());
	
	while (it != end && std::isspace(*it)) ++it;
	if (it != end)
	{
		if (*it == '!')
		{
			std::string heading("h4");
			++it;
			if (it != end && *it == '!')
			{
				heading = "h3";
				++it;
			}
			if (it != end && *it == '!')
			{
				heading = "h2";
				++it;
			}
			while (it != end && std::isspace(*it)) ++it;
			ostr << "</p><" << heading << ">" << format("<a id=\"%d\">", _titleId++) << htmlize(std::string(it, end)) << "</a></" << heading << "><p>" << std::endl;
			return;
		}
	}
	writeText(ostr, it, end);
	ostr << ' ';
}


void DocWriter::writeText(std::ostream& ostr, std::string::const_iterator begin, const std::string::const_iterator& end)
{
	std::string token;
	nextToken(begin, end, token);
	while (!token.empty())
	{
		if (!writeSymbol(ostr, token, begin, end) && !writeSpecial(ostr, token, begin, end))
		{
			if (token == "[[")
			{
				std::string uri;
				std::string text;
				std::string::const_iterator it(begin);
				while (it != end && std::isspace(*it)) ++it;
				while (it != end && !std::isspace(*it)) uri += *it++;
				while (it != end && std::isspace(*it)) ++it;
				while (it != end && *it != ']') text += *it++;
				while (it != end && *it == ']') ++it;
				if (uri.compare(0, 6, "image:") == 0)
				{
					uri.erase(0, 6);
					writeImage(ostr, uri, text);
				}
				else
				{
					std::string target;
					if (uri.compare(0, 7, "http://") == 0 || uri.compare(0, 8, "https://") == 0)
						target = "_blank";
					writeTargetLink(ostr, uri, text, target);
				}
				begin = it;
				nextToken(begin, end, token);
				continue;
			}
			if (token == "RFC")
			{
				std::string::const_iterator it(begin);
				std::string spc;
				nextToken(begin, end, spc);
				if (spc == " ")
				{
					std::string n;
					nextToken(begin, end, n);
					if (!n.empty() && std::isdigit(n[0]))
					{
						std::string uri(RFC_URI);
						uri += n;
						uri += ".txt";
						writeTargetLink(ostr, uri, token + " " + n, "_blank");
						nextToken(begin, end, token);
						continue;
					}
				}
				begin = it;
			}
			if (token == "GH")
			{
				std::string uri(GITHUB_POCO_URI);
				std::string::const_iterator it(begin);
				std::string spc;
				nextToken(begin, end, spc);
				if (spc == ":")
				{
					std::string proj;
					nextToken(begin, end, proj);
					uri = projectURI(proj);
					nextToken(begin, end, spc);
				}
				if (spc == " ")
				{
					std::string hash;
					nextToken(begin, end, hash);
					if (hash == "#")
					{
						std::string n;
						nextToken(begin, end, n);
						if (!n.empty() && std::isdigit(n[0]))
						{
							uri += "/issues/";
							uri += n;
							writeTargetLink(ostr, uri, token + " #" + n, "_blank");
							nextToken(begin, end, token);
							continue;
						}
					}
				}
				begin = it;
			}
			if (token == "http")
			{
				std::string::const_iterator it(begin);
				std::string css;
				nextToken(begin, end, css);
				if (css == "://")
				{
					std::string uri(token);
					uri += css;
					while (begin != end && !std::isspace(*begin) && *begin != '>' && *begin != ')') uri += *begin++;
					if (uri[uri.length() - 1] == '.')
					{
						uri.resize(uri.length() - 1);
						writeTargetLink(ostr, uri, uri, "_blank");
						ostr << '.';
					}
					else writeTargetLink(ostr, uri, uri, "_blank");
					nextToken(begin, end, token);
					continue;
				}
				else
					ostr << htmlize(token);
				begin = it;
			}
			else
			{
				ostr << htmlize(token);
			}
			nextToken(begin, end, token);
		}
	}
}


void DocWriter::writeDecl(std::ostream& ostr, const std::string& decl)
{
	writeDecl(ostr, decl.begin(), decl.end());
}


void DocWriter::writeDecl(std::ostream& ostr, std::string::const_iterator begin, const std::string::const_iterator& end)
{
	std::string token;
	nextToken(begin, end, token);
	while (!token.empty())
	{
		if (!writeSymbol(ostr, token, begin, end))
		{
			ostr << htmlize(token);
			nextToken(begin, end, token);
		}
	}
}
		

bool DocWriter::writeSymbol(std::ostream& ostr, std::string& token, std::string::const_iterator& begin, const std::string::const_iterator& end)
{
	if (std::isalnum(token[0]) && _pNameSpace)
	{
		std::string id(token);
		std::string next;
		std::string::const_iterator it(begin);
		nextToken(begin, end, next);
		begin = it;
		if (std::isupper(id[0]) || (!next.empty() && next[0] == '('))
		{
			std::string::const_iterator it2(begin);
			while (next == "::")
			{
				nextToken(begin, end, next); // ::
				id += next;
				nextToken(begin, end, next); // id
				id += next;
				it2 = begin;
				nextToken(begin, end, next);
				begin = it2;
			}
			Symbol* pSym = _pNameSpace->lookup(id);
			if (pSym)
			{
				writeLink(ostr, pSym, id);
				nextToken(begin, end, token);
				return true;
			}
			begin = it;
		}
	}
	return false;
}


bool DocWriter::writeSpecial(std::ostream& ostr, std::string& token, std::string::const_iterator& begin, const std::string::const_iterator& end)
{
	if (token == "<%")
	{
		_htmlMode = true;
		ostr << "</p>";
	}
	else if (token == "<{")
	{
		_htmlMode = true;
	}
	else if (token == "%>")
	{
		_htmlMode = false;
		ostr << "<p>";
	}
	else if (token == "}>")
	{
		_htmlMode = false;
	}
	else if (token == "<?")
	{
		std::string prop;
		nextToken(begin, end, token);
		while (!token.empty() && token != "?>")
		{
			prop.append(token);
			nextToken(begin, end, token);
		}
		Poco::trimInPlace(prop);
		Application& app = Application::instance();
		ostr << htmlize(app.config().getString(prop, std::string("NOT FOUND: ") + prop));
	}
	else if (_htmlMode)
	{
		ostr << token;
	}
	else if (token == "<*")
	{
		ostr << "<i>";
	}
	else if (token == "*>")
	{
		ostr << "</i>";
	}
	else if (token == "<!")
	{
		ostr << "<b>";
	}
	else if (token == "!>")
	{
		ostr << "</b>";
	}
	else if (token == "<[")
	{
		ostr << "<tt>";
		_literalMode = true;
	}
	else if (token == "]>")
	{
		ostr << "</tt>";
		_literalMode = false;
	}
	else if (token == "--" && !_literalMode)
	{
		ostr << "&mdash;";
	}
	else if (token == "iff" && !_literalMode)
	{
		ostr << tr("iff");
	}
	else if (token != "----")
	{
		return false;
	}
	nextToken(begin, end, token);
	return true;
}


void DocWriter::nextToken(std::string::const_iterator& it, const std::string::const_iterator& end, std::string& token)
{
	token.clear();
	if (it != end && (std::isalnum(*it) || *it == '_'))
	{
		while (it != end && (std::isalnum(*it) || *it == '_')) token += *it++;
	}
	else if (it != end && std::isspace(*it))
	{
		while (it != end && std::isspace(*it)) token += *it++;
	}
	else if (it != end && *it == '<')
	{
		token += *it++;
		if (it != end && std::ispunct(*it)) token += *it++;
	}
	else if (it != end && *it == '[')
	{
		token += *it++;
		if (it != end && *it == '[') token += *it++;
	}
	else if (it != end && (*it == ']' || *it == '*' || *it == '!' || *it == '%' || *it == '}' || *it == '?'))
	{
		token += *it++;
		if (it != end && *it == '>') token += *it++;
	}
	else if (it != end && *it == '-')
	{
		while (it != end && *it == '-') token += *it++;
	}
	else if (it != end && *it == ':')
	{
		while (it != end && (*it == ':' || *it == '/')) token += *it++;
	}
	else if (it != end)
	{
		token += *it++;
	}
}


void DocWriter::writeListItem(std::ostream& ostr, const std::string& text)
{
	std::string::const_iterator it  = text.begin();
	std::string::const_iterator end = text.end();
	while (it != end && std::isspace(*it)) ++it;
	if ((it != end && *it == '-') || *it == '*')
	{
		++it;
		while (it != end && std::isspace(*it)) ++it;
	}
	writeText(ostr, it, end);
	ostr << ' ';
}


void DocWriter::writeOrderedListItem(std::ostream& ostr, const std::string& text)
{
	std::string::const_iterator it  = text.begin();
	std::string::const_iterator end = text.end();
	while (it != end && std::isspace(*it)) ++it;
	if (it != end && std::isdigit(*it))
	{
		while (it != end && std::isdigit(*it)) ++it;
		if (it != end && *it == '.') ++it;
		while (it != end && std::isspace(*it)) ++it;
	}
	writeText(ostr, it, end);
	ostr << ' ';
}


void DocWriter::writeLiteral(std::ostream& ostr, const std::string& text)
{
	if (_pendingLine)
	{
		ostr << "\n";
		_pendingLine = false;
	}

	std::string::const_iterator it(text.begin());
	std::string::const_iterator end(text.end());
	if (_indent == 0)
	{
		while (it != end && std::isspace(*it))
		{
			++it;
			++_indent;
		}
	}
	else
	{
		int i = 0;
		while (it != end && i < _indent)
		{
			++it;
			++i;
		}
	}
	std::string line(it, end);
	Poco::trimRightInPlace(line);
	if (line.empty())
		_pendingLine = true;
	else
		ostr << htmlize(std::string(it, end)) << "\n";
}


void DocWriter::writeFileInfo(std::ostream& ostr, const Symbol* pSymbol)
{
	std::string library(pSymbol->getLibrary());
	std::string package(pSymbol->getPackage());
	if (library.empty() || library == "ChangeThis")
		logger().notice(std::string("No library name specified in ") + pSymbol->getFile());
	if (package.empty() || package == "ChangeThis")
		logger().notice(std::string("No package name specified in ") + pSymbol->getFile());
	
	ostr << "<p>\n";
	ostr << "<b>" << tr("Library") << ":</b> " << library << "<br />\n"
	     << "<b>" << tr("Package") << ":</b> " << package << "<br />\n"
	     << "<b>" << tr("Header") << ":</b> " << headerFor(pSymbol);
	ostr << "</p>\n";
}


void DocWriter::writeInheritance(std::ostream& ostr, const Struct* pStruct)
{
	std::set<std::string> bases;
	pStruct->bases(bases);
	if (!bases.empty() || pStruct->derivedBegin() != pStruct->derivedEnd())
	{
		writeSubTitle(ostr, tr("Inheritance"));
		if (pStruct->baseBegin() != pStruct->baseEnd())
		{
			ostr << "<p><b>" << tr("Direct_Base_Classes") << ": </b>";
			bool first = true;
			for (Struct::BaseIterator it = pStruct->baseBegin(); it != pStruct->baseEnd(); ++it)
			{
				std::string base;
				if (it->pClass)
				{
					if (it->pClass->nameSpace() == pStruct->nameSpace())
						base = it->pClass->name();
					else
						base = it->pClass->fullName();
				}
				else base = it->name;
				writeNameListItem(ostr, base, it->pClass, pStruct->nameSpace(), first);
			}
			ostr << "</p>\n";
		}
		if (!bases.empty())
		{
			ostr << "<p><b>" << tr("All_Base_Classes") << ": </b>";
			bool first = true;
			for (std::set<std::string>::const_iterator it = bases.begin(); it != bases.end(); ++it)
			{
				std::string base;
				Symbol* pBase = pStruct->nameSpace()->lookup(*it);
				if (pBase)
				{
					if (pBase->nameSpace() == pStruct->nameSpace())
						base = pBase->name();
					else
						base = pBase->fullName();
				}
				else base = *it;
				writeNameListItem(ostr, base, pBase, pStruct->nameSpace(), first);
			}
			ostr << "</p>\n";
		}
		Struct::StructSet derived;
		pStruct->derived(derived);
		if (!derived.empty())
		{
			ostr << "<p><b>" << tr("Known_Derived_Classes") << ": </b>";
			bool first = true;
			for (Struct::StructSet::const_iterator it = derived.begin(); it != derived.end(); ++it)
			{
				std::string derived;
				if ((*it)->nameSpace() == pStruct->nameSpace())
					derived = (*it)->name();
				else
					derived = (*it)->fullName();
				writeNameListItem(ostr, derived, *it, pStruct->nameSpace(), first);
			}
			ostr << "</p>\n";
		}
	}
}


void DocWriter::writeMethodSummary(std::ostream& ostr, const Struct* pStruct)
{
	bool titleWritten = false;
	MethodMap methods;
	Struct::Functions functions;
	pStruct->methods(Symbol::ACC_PUBLIC, functions);
	for (Struct::Functions::const_iterator it = functions.begin(); it != functions.end(); ++it)
	{
		if (methods.find((*it)->name()) == methods.end())
			methods[(*it)->name()] = *it;
	}
	pStruct->methods(Symbol::ACC_PROTECTED, functions);
	for (Struct::Functions::const_iterator it = functions.begin(); it != functions.end(); ++it)
	{
		if (methods.find((*it)->name()) == methods.end())
			methods[(*it)->name()] = *it;
	}
	if (!methods.empty())
	{
		writeSubTitle(ostr, tr("Member_Summary"));
		titleWritten = true;
		ostr << "<p><b>" << tr("Member_Functions") << ": </b>";
		bool first = true;
		for (MethodMap::const_iterator it = methods.begin(); it != methods.end(); ++it)
		{
			writeNameListItem(ostr, it->first, it->second, pStruct, first);
		}
		ostr << "</p>\n";
	}
	methods.clear();
	Struct::FunctionSet inhFunctions;
	pStruct->inheritedMethods(inhFunctions);
	for (Struct::FunctionSet::const_iterator it = inhFunctions.begin(); it != inhFunctions.end(); ++it)
	{
		if (methods.find((*it)->name()) == methods.end())
			methods[(*it)->name()] = *it;
	}
	if (!methods.empty())
	{
		if (!titleWritten)
			writeSubTitle(ostr, tr("Member_Summary"));
		ostr << "<p><b>" << tr("Inherited_Functions") << ": </b>";
		bool first = true;
		for (MethodMap::const_iterator it = methods.begin(); it != methods.end(); ++it)
		{
			writeNameListItem(ostr, it->first, it->second, pStruct, first);
		}
		ostr << "</p>\n";
	}
}


void DocWriter::writeNestedClasses(std::ostream& ostr, const Struct* pStruct)
{
	NameSpace::SymbolTable classes;
	pStruct->classes(classes);
	bool hasNested = false;
	for (NameSpace::Iterator it = classes.begin(); it != classes.end(); ++it)
	{
		if (it->second->getAccess() != Symbol::ACC_PRIVATE)
			hasNested = true;
	}
	if (hasNested)
	{
		writeSubTitle(ostr, tr("Nested_Classes"));
		for (NameSpace::Iterator it = classes.begin(); it != classes.end(); ++it)
		{
			if (it->second->getAccess() != Symbol::ACC_PRIVATE)
				writeClassSummary(ostr, static_cast<const Struct*>(it->second));
		}
	}
}


void DocWriter::writeNameSpacesSummary(std::ostream& ostr, const NameSpace* pNameSpace)
{
	NameSpace::SymbolTable nameSpaces;
	pNameSpace->nameSpaces(nameSpaces);
	if (!nameSpaces.empty())
	{
		ostr << "<p><b>" << tr("Namespaces") << ":</b> " << std::endl;
		bool first = true;
		for (NameSpace::Iterator it = nameSpaces.begin(); it != nameSpaces.end(); ++it)
		{
			writeNameListItem(ostr, it->second->name(), it->second, pNameSpace, first);
		}
		ostr << "</p>" << std::endl;
	}
}


void DocWriter::writeNameSpaces(std::ostream& ostr, const NameSpace* pNameSpace)
{
	NameSpace::SymbolTable nameSpaces;
	pNameSpace->nameSpaces(nameSpaces);
	if (!nameSpaces.empty())
	{
		writeSubTitle(ostr, tr("Namespaces"));
		for (NameSpace::Iterator it = nameSpaces.begin(); it != nameSpaces.end(); ++it)
		{
			ostr << "<h3>";
			std::string what("namespace ");
			what += it->second->name();
			writeLink(ostr, uriFor(it->second), what, "class");
			ostr << "</h3>\n";
		}
	}
}


void DocWriter::writeClassesSummary(std::ostream& ostr, const NameSpace* pNameSpace)
{
	NameSpace::SymbolTable classes;
	pNameSpace->classes(classes);
	if (!classes.empty())
	{
		ostr << "<p><b>" << tr("Classes") << ":</b> " << std::endl;
		bool first = true;
		for (NameSpace::Iterator it = classes.begin(); it != classes.end(); ++it)
		{
			writeNameListItem(ostr, it->second->name(), it->second, pNameSpace, first);
		}
		ostr << "</p>" << std::endl;
	}
}


void DocWriter::writeClasses(std::ostream& ostr, const NameSpace* pNameSpace)
{
	NameSpace::SymbolTable classes;
	pNameSpace->classes(classes);
	if (!classes.empty())
	{
		writeSubTitle(ostr, tr("Classes"));
		for (NameSpace::Iterator it = classes.begin(); it != classes.end(); ++it)
		{
			writeClassSummary(ostr, static_cast<const Struct*>(it->second));
		}
	}
}


void DocWriter::writeClassSummary(std::ostream& ostr, const Struct* pStruct)
{
	ostr << "<h3>";
	std::string what;
	if (pStruct->isClass())
		what += "class ";
	else
		what += "struct ";
	what += pStruct->name();
	writeLink(ostr, uriFor(pStruct), what, "class");
	if (pStruct->getAccess() != Symbol::ACC_PUBLIC)
		writeIcon(ostr, "protected");
	ostr << "</h3>\n";
	writeSummary(ostr, pStruct->getDocumentation(), uriFor(pStruct));
}


void DocWriter::writeTypesSummary(std::ostream& ostr, const NameSpace* pNameSpace)
{
	NameSpace::SymbolTable types;
	pNameSpace->typeDefs(types);
	if (!types.empty())
	{
		ostr << "<p><b>" << tr("Types") << ":</b> " << std::endl;
		bool first = true;
		for (NameSpace::Iterator it = types.begin(); it != types.end(); ++it)
		{
			writeNameListItem(ostr, it->second->name(), it->second, pNameSpace, first);
		}
		ostr << "</p>" << std::endl;
	}
}


void DocWriter::writeTypes(std::ostream& ostr, const NameSpace* pNameSpace)
{
	NameSpace::SymbolTable types;
	pNameSpace->typeDefs(types);
	bool hasTypes = false;
	for (NameSpace::Iterator it = types.begin(); !hasTypes && it != types.end(); ++it)
	{
		if (it->second->getAccess() != Symbol::ACC_PRIVATE)
			hasTypes = true;
	}
	if (hasTypes)
	{
		writeSubTitle(ostr, tr("Types"));
		for (NameSpace::Iterator it = types.begin(); it != types.end(); ++it)
		{
			if (it->second->getAccess() != Symbol::ACC_PRIVATE)
				writeType(ostr, static_cast<const TypeDef*>(it->second));
		}
	}
}


void DocWriter::writeType(std::ostream& ostr, const TypeDef* pType)
{
	ostr << "<h3>";
	writeAnchor(ostr, pType->name(), pType);
	if (pType->getAccess() != Symbol::ACC_PUBLIC)
		writeIcon(ostr, "protected");
	ostr << "</h3>\n";
	ostr << "<p class=\"decl\">";
	writeDecl(ostr, pType->declaration());
	ostr << ";</p>\n";
	writeDescription(ostr, pType->getDocumentation());
}


void DocWriter::writeEnums(std::ostream& ostr, const NameSpace* pNameSpace)
{
	NameSpace::SymbolTable enums;
	pNameSpace->enums(enums);
	bool hasEnums = false;
	for (NameSpace::Iterator it = enums.begin(); !hasEnums && it != enums.end(); ++it)
	{
		if (it->second->getAccess() != Symbol::ACC_PRIVATE)
			hasEnums = true;
	}
	if (hasEnums)
	{
		writeSubTitle(ostr, tr("Enumerations"));
		for (NameSpace::Iterator it = enums.begin(); it != enums.end(); ++it)
		{
			if (it->second->getAccess() != Symbol::ACC_PRIVATE)
				writeEnum(ostr, static_cast<const Enum*>(it->second));
		}
	}
}


void DocWriter::writeEnum(std::ostream& ostr, const Enum* pEnum)
{
	ostr << "<h3>";
	const std::string& name = pEnum->name();
	if (name[0] == '#')
		ostr << "<i>" << tr("Anonymous") << "</i>";
	else
		writeAnchor(ostr, name, pEnum);
	if (pEnum->getAccess() != Symbol::ACC_PUBLIC)
		writeIcon(ostr, "protected");
	ostr << "</h3>\n";
	writeDescription(ostr, pEnum->getDocumentation());
	for (Enum::Iterator it = pEnum->begin(); it != pEnum->end(); ++it)
	{
		const std::string& name = (*it)->name();
		const std::string& value = (*it)->value();
		ostr << "<p class=\"decl\">";
		writeAnchor(ostr, name, *it);
		if (!value.empty())
			ostr << " = " << htmlize(value);
		ostr << "</p>\n";
		writeDescription(ostr, (*it)->getDocumentation());
	}
}


void DocWriter::writeConstructors(std::ostream& ostr, const Struct* pStruct)
{
	Struct::Functions ctors;
	pStruct->constructors(ctors);
	if (!ctors.empty())
	{
		writeSubTitle(ostr, tr("Constructors"));
		for (Struct::Functions::const_iterator it = ctors.begin(); it != ctors.end(); ++it)
		{
			if ((*it)->getAccess() == Symbol::ACC_PUBLIC)
				writeFunction(ostr, *it);
		}
		for (Struct::Functions::const_iterator it = ctors.begin(); it != ctors.end(); ++it)
		{
			if ((*it)->getAccess() == Symbol::ACC_PROTECTED)
				writeFunction(ostr, *it);
		}
	}
}


void DocWriter::writeDestructor(std::ostream& ostr, const Struct* pStruct)
{
	if (pStruct->destructor())
	{
		writeSubTitle(ostr, tr("Destructor"));
		writeFunction(ostr, pStruct->destructor());
	}
}


void DocWriter::writeMethods(std::ostream& ostr, const Struct* pStruct)
{
	Struct::Functions methods;
	pStruct->methods(Symbol::ACC_PUBLIC, methods);
	pStruct->methods(Symbol::ACC_PROTECTED, methods);
	if (!methods.empty())
	{
		writeSubTitle(ostr, tr("Member_Functions"));
		for (Struct::Functions::const_iterator it = methods.begin(); it != methods.end(); ++it)
		{
			writeFunction(ostr, *it);
		}
	}
}


void DocWriter::writeFunctionsSummary(std::ostream& ostr, const NameSpace* pNameSpace)
{
	NameSpace::SymbolTable funcs;
	pNameSpace->functions(funcs);
	if (!funcs.empty())
	{
		ostr << "<p><b>" << tr("Functions") << ":</b> " << std::endl;
		std::string lastName;
		bool first = true;
		for (NameSpace::Iterator it = funcs.begin(); it != funcs.end(); ++it)
		{
			if (it->second->name() != lastName)
			{
				writeNameListItem(ostr, it->second->name(), it->second, pNameSpace, first);
				lastName = it->second->name();
			}
		}
		ostr << "</p>" << std::endl;
	}
}


void DocWriter::writeFunctions(std::ostream& ostr, const NameSpace* pNameSpace)
{
	NameSpace::SymbolTable funcs;
	pNameSpace->functions(funcs);
	if (!funcs.empty())
	{
		writeSubTitle(ostr, tr("Functions"));
		for (NameSpace::Iterator it = funcs.begin(); it != funcs.end(); ++it)
		{
			writeFunction(ostr, static_cast<const Function*>(it->second));
		}
	}
}


void DocWriter::writeFunction(std::ostream& ostr, const Function* pFunc)
{
	ostr << "<h3>";
	writeAnchor(ostr, pFunc->name(), pFunc);
	if (pFunc->getAccess() != Symbol::ACC_PUBLIC)
		writeIcon(ostr, "protected");
	if (pFunc->isVirtual())
		writeIcon(ostr, "virtual");
	else if (pFunc->flags() & Function::FN_STATIC)
		writeIcon(ostr, "static");
	if (pFunc->flags() & Function::FN_INLINE)
		writeIcon(ostr, "inline");
	ostr << "</h3>\n";
	ostr << "<p class=\"decl\">";
	const std::string& decl = pFunc->declaration();
	writeDecl(ostr, decl);
	if (!std::isalnum(decl[decl.length() - 1]))
		ostr << " ";
	ostr << "(";
	bool hasArgs = false;
	for (Function::Iterator it = pFunc->begin(); it != pFunc->end(); ++it)
	{
		hasArgs = true;
		if (it != pFunc->begin())
			ostr << ",";
		ostr << "<br />&nbsp;&nbsp;&nbsp;&nbsp;";
		writeDecl(ostr, (*it)->declaration());
		if ((*it)->hasDefaultValue())
		{
			ostr << " = " << (*it)->defaultDecl();
		}
	}
	if (hasArgs)
		ostr << "<br />";
	ostr << ")";
	if (pFunc->flags() & Function::FN_CONST)
		ostr << " const";
	if (pFunc->flags() & Function::FN_OVERRIDE)
		ostr << " override";
	else if (pFunc->flags() & Function::FN_FINAL)
		ostr << " final";
	if (pFunc->flags() & Function::FN_DELETE)
		ostr << " = delete";
	else if (pFunc->flags() & Function::FN_DEFAULT)
		ostr << " = default";
	else if (pFunc->flags() & Function::FN_PURE_VIRTUAL)
		ostr << " = 0";
	ostr << ";</p>\n";
	
	if (pFunc->attrs().has("deprecated"))
	{
		writeDeprecated(ostr, "function");
	}

	Function* pOverridden = pFunc->getOverridden();
	const std::string& doc(pFunc->getDocumentation());
	if (doc.empty() && pFunc->isPublic() && !pOverridden && !pFunc->isConstructor() && !pFunc->isDestructor())
		logger().notice(std::string("Undocumented public function: ") + pFunc->fullName());

	writeDescription(ostr, doc);
	if (pOverridden)
	{
		ostr << "<div class=\"description\"><p><b>" << tr("See_also") << ":</b> ";
		writeLink(ostr, pOverridden, pOverridden->fullName() + "()");
		ostr << "</p></div>\n";
	}
}


void DocWriter::writeVariables(std::ostream& ostr, const NameSpace* pNameSpace)
{
	NameSpace::SymbolTable vars;
	pNameSpace->variables(vars);
	bool hasVars = false;
	for (NameSpace::Iterator it = vars.begin(); !hasVars && it != vars.end(); ++it)
	{
		if (it->second->getAccess() != Symbol::ACC_PRIVATE)
			hasVars = true;
	}
	if (hasVars)
	{
		
		writeSubTitle(ostr, tr("Variables"));
		for (NameSpace::Iterator it = vars.begin(); it != vars.end(); ++it)
		{
			if (it->second->getAccess() == Symbol::ACC_PUBLIC)
				writeVariable(ostr, static_cast<const Variable*>(it->second));
		}
		for (NameSpace::Iterator it = vars.begin(); it != vars.end(); ++it)
		{
			if (it->second->getAccess() == Symbol::ACC_PROTECTED)
				writeVariable(ostr, static_cast<const Variable*>(it->second));
		}
	}
}


void DocWriter::writeVariable(std::ostream& ostr, const Variable* pVar)
{
	ostr << "<h3>";
	writeAnchor(ostr, pVar->name(), pVar);
	if (pVar->getAccess() != Symbol::ACC_PUBLIC)
		writeIcon(ostr, "protected");
	if (pVar->flags() & Function::FN_STATIC)
		writeIcon(ostr, "static");
	ostr << "</h3>\n";
	ostr << "<p class=\"decl\">";
	writeDecl(ostr, pVar->declaration());
	ostr << ";</p>\n";
	writeDescription(ostr, pVar->getDocumentation());
}


void DocWriter::writeNameListItem(std::ostream& ostr, const std::string& str, const Symbol* pSymbol, const NameSpace* pNameSpace, bool& first)
{
	if (first)
		first = false;
	else
		ostr << ", ";

	if (pSymbol)
		writeLink(ostr, pSymbol, str);
	else
		ostr << htmlizeName(str);
}


void DocWriter::writeLink(std::ostream& ostr, const std::string& uri, const std::string& text)
{
	ostr << "<a href=\"" << uri << "\">" << htmlize(text) << "</a>";
}


void DocWriter::writeLink(std::ostream& ostr, const Symbol* pSymbol, const std::string& name)
{
	ostr << "<a href=\"" << uriFor(pSymbol) << "\" title=\"" << titleFor(pSymbol) << "\">" << htmlizeName(name) << "</a>";
}


void DocWriter::writeLink(std::ostream& ostr, const std::string& uri, const std::string& text, const std::string& linkClass)
{
	ostr << "<a href=\"" << uri << "\" class=\"" << linkClass << "\">" << htmlize(text) << "</a>";
}


void DocWriter::writeTargetLink(std::ostream& ostr, const std::string& uri, const std::string& text, const std::string& target)
{
	if (_noFrames && target != "_blank")
		ostr << "<a href=\"" << uri << "\">" << text << "</a>";
	else if (!target.empty())
		ostr << "<a href=\"" << uri << "\" target=\"" << target << "\">" << text << "</a>";
	else
		ostr << "<a href=\"" << uri << "\">" << htmlize(text) << "</a>";
}


void DocWriter::writeImageLink(std::ostream& ostr, const std::string& uri, const std::string& image, const std::string& alt)
{
	ostr << "<a href=\"" << uri << "\">";
	ostr << "<img src=\"images/" << image << "\" alt=\"" + alt + "\" /> ";
	ostr << "</a>";
}


void DocWriter::writeImage(std::ostream& ostr, const std::string& uri, const std::string& caption)
{
	ostr << "<div class=\"image\">" << std::endl;
	ostr << "<img src=\"" << uri << "\" alt=\"" + caption + "\" title=\"" << htmlize(caption) << "\" /> " << std::endl;
	if (!caption.empty())
	{
		ostr << "<div class=\"imagecaption\">" << htmlize(caption) << "</div>";
	}
	ostr << "</div>" << std::endl;
}


void DocWriter::writeIcon(std::ostream& ostr, const std::string& icon)
{
	ostr << " <img src=\"images/" << icon << ".png\" alt=\"" + icon + "\" title=\"" << icon << "\" class=\"icon\" /> ";
}


void DocWriter::writeAnchor(std::ostream& ostr, const std::string& text, const Symbol* pSymbol)
{
	ostr << "<a id=\"" << pSymbol->id() << "\">" << htmlize(text) << "</a>";
}


void DocWriter::writeDeprecated(std::ostream& ostr, const std::string& what)
{
	ostr << "<div class=\"description\">" << std::endl;
	ostr << "<p><b>" << tr("Deprecated") << ".</b> <i>" << tr("This") << " " << what << " " << tr("is_deprecated") << ".</i></p>" << std::endl;
	ostr << "</div>" << std::endl;
}


std::string DocWriter::headerFor(const Symbol* pSymbol)
{
	Path path(pSymbol->getFile());
	std::string header;
	int i = 0;
	while (i < path.depth() - 1 && path[i] != "include") ++i;
	for (++i; i < path.depth(); ++i)
	{
		header += path[i];
		header += "/";
	}
	header += path.getFileName();
	return header;
}


std::string DocWriter::titleFor(const Symbol* pSymbol)
{
	std::string title;
	switch (pSymbol->kind())
	{
	case Symbol::SYM_NAMESPACE:
		title += "namespace";
		break;
	case Symbol::SYM_STRUCT:
		title += (static_cast<const Struct*>(pSymbol)->isClass() ? "class" : "struct");
		break;
	case Symbol::SYM_ENUM:
		title += "enum ";
		break;
	default:
		break;
	}
	if (!title.empty()) title += " ";
	const Function* pFunc = dynamic_cast<const Function*>(pSymbol);
	if (pFunc && pFunc->isConstructor())
	{
		title += "class " + pSymbol->nameSpace()->fullName();
	}
	else
	{
		title += pSymbol->fullName();
		if (pFunc) title += "()";
	}
	return title;
}


std::string DocWriter::htmlize(const std::string& str)
{
	std::string result;
	for (std::string::const_iterator it = str.begin(); it != str.end(); ++it)
		result += htmlize(*it);
	return result;
}


std::string DocWriter::htmlize(char c)
{
	std::string result;
	switch (c)
	{
	case '"':
		result += "&quot;";
		break;
	case '<':
		result += "&lt;";
		break;
	case '>':
		result += "&gt;";
		break;
	case '&':
		result += "&amp;";
		break;
	default:
		result += c;
	}
	return result;
}


void DocWriter::libraries(std::set<std::string>& libs)
{
	for (NameSpace::SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		const std::string& lib = it->second->getLibrary();
		if (!lib.empty())
			libs.insert(lib);
	}
}


void DocWriter::packages(const std::string& lib, std::set<std::string>& packages)
{
	for (NameSpace::SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		const std::string& plib = it->second->getLibrary();
		const std::string& pkg = it->second->getPackage();
		if (!pkg.empty() && lib == plib)
			packages.insert(pkg);
	}
}


void DocWriter::writePages()
{
	_pNameSpace = rootNameSpace();
	for (PageMap::iterator it = _pages.begin(); it != _pages.end(); ++it)
	{
		writePage(it->second);
	}
}


void DocWriter::writePage(Page& page)
{
	std::ifstream istr(page.path.c_str());
	if (!istr.good()) throw Poco::OpenFileException(page.path);
	std::string title;
	std::string category;
	std::string text;
	int ch = istr.get();
	while (ch != -1 && ch != '\n') { title += (char) ch; ch = istr.get(); }
	ch = istr.get();
	while (ch != -1 && ch != '\n') { category += (char) ch; ch = istr.get(); }
	
	while (std::isspace(ch)) ch = istr.get();
	while (ch != -1) 
	{
		text += (char) ch; 
		if (ch == '\n') text += ' ';
		ch = istr.get();
	}
	
	page.title    = title;
	page.category = category;
	
	TOC toc;
	scanTOC(text, toc);
	
	std::string path(pathFor(page.fileName));
	std::ofstream ostr(path.c_str());
	if (!ostr.good()) throw Poco::CreateFileException(path);
	writeHeader(ostr, title, "js/iframeResizer.min.js");
	writeTitle(ostr, tr(category), title);
	beginBody(ostr);
	writeNavigationFrame(ostr, "category", category);
	beginContent(ostr);
	if (!toc.empty()) 
	{
		writeTOC(ostr, toc);
	}
	writeDescription(ostr, text);
	writeCopyright(ostr);
	endContent(ostr);
	endBody(ostr);
	ostr << "<script>CollapsibleLists.apply(true)</script>" << std::endl;
	writeFooter(ostr);
}


void DocWriter::scanTOC(const std::string& text, TOC& toc)
{
	int titleId = 0;
	std::istringstream istr(text);
	while (!istr.eof())
	{
		std::string line;
		std::getline(istr, line);
		std::string::const_iterator it(line.begin());
		std::string::const_iterator end(line.end());
		while (it != end && std::isspace(*it)) ++it;
		if (it != end && *it == '!')
		{
			++it;
			int level = MAX_TITLE_LEVEL;
			while (it != end && *it == '!')
			{
				level--;
				++it;
			}
			while (it != end && std::isspace(*it)) ++it;
			TOCEntry entry;
			entry.id = titleId++;
			entry.level = level;
			entry.title.assign(it, end);
			toc.push_back(entry);
		}
	}
}


void DocWriter::writeTOC(std::ostream& ostr, const TOC& toc)
{
	ostr << "<div class=\"toc\">" << std::endl;
	ostr << "<ul class=\"collapsibleList\"><li>" << tr("TOC") << std::endl;
	int lastLevel = 0;
	std::vector<int> levelStack;
	levelStack.push_back(0);
	for (TOC::const_iterator it = toc.begin(); it != toc.end(); ++it)
	{
		int level = it->level;
		if (level > levelStack.back())
		{
			levelStack.push_back(level);
			ostr << "<ul>" << std::endl;
		}
		else if (level < levelStack.back())
		{
			ostr << "</li>" << std::endl;
			while (level < levelStack.back())
			{
				levelStack.pop_back();
				ostr << "</ul></li>" << std::endl;
			}
		}
		else 
		{
			ostr << "</li>" << std::endl;
		}
		ostr << "<li class=\"level" << level << "\"><a href=\"#" << it->id << "\">" << htmlize(it->title) << "</a>" << std::endl;
		lastLevel = level;
	}
	while (!levelStack.empty())
	{
		ostr << "</li></ul>" << std::endl;
		levelStack.pop_back();
	}
	ostr << "</div>" << std::endl;
}


void DocWriter::writeCategoryIndex(const std::string& category, const std::string& fileName)
{
	std::ofstream ostr(pathFor(fileName).c_str());
	if (!ostr.good()) throw Poco::CreateFileException(fileName);
	writeHeader(ostr, tr(category));
	beginBody(ostr);
	ostr << "<h4>" << htmlize(tr(category)) << "</h4>";
	writeCategoryIndex(ostr, category, "_top");
	endBody(ostr);
	writeFooter(ostr);
}


void DocWriter::writeCategoryIndex(std::ostream& ostr, const std::string& category, const std::string& target)
{
	ostr << "<ul>\n";
	for (PageMap::const_iterator it = _pages.begin(); it != _pages.end(); ++it)
	{
		if (it->second.category == category)
		{
			ostr << "<li>";
			writeTargetLink(ostr, it->second.fileName, it->second.title, target);
			ostr << "</li>\n";
		}
	}
	ostr << "</ul>\n";
}


NameSpace* DocWriter::rootNameSpace() const
{
	for (NameSpace::SymbolTable::const_iterator it = _symbols.begin(); it != _symbols.end(); ++it)
	{
		if (it->second->kind() == Symbol::SYM_NAMESPACE)
		{
			NameSpace* pNS = static_cast<NameSpace*>(it->second);
			while (pNS->nameSpace()) pNS = pNS->nameSpace();
			return pNS;
		}
	}
	return 0;
}


const std::string& DocWriter::tr(const std::string& id)
{
	StringMap::const_iterator it = _strings.find(id);

	if (it == _strings.end())
	{
		loadString(id, id, _language);
		it = _strings.find(id);
	}
	if (it != _strings.end())
		return it->second;
	else
		return id;
}


void DocWriter::loadString(const std::string& id, const std::string& def, const std::string& language)
{
	Application& app = Application::instance();
	std::pair<std::string, std::string> p(id, app.config().getString("Translations." + language + "." + id, def));
	_strings.insert(p);
}


void DocWriter::loadStrings(const std::string& language)
{
	_strings.clear();
	loadString("AAAIntroduction", "Introduction", language);
	loadString("All_Base_Classes", "All Base Classes", language);
	loadString("All_Symbols", "All Symbols", language);
	loadString("Anonymous", "Anonymous", language);
	loadString("Constructors", "Constructors", language);
	loadString("Class", "Class", language);
	loadString("Deprecated", "Deprecated", language);
	loadString("Description", "Description", language);
	loadString("Destructor", "Destructor", language);
	loadString("Direct_Base_Classes", "Direct Base Classes", language);
	loadString("Enumerations", "Enumerations", language);
	loadString("Functions", "Functions", language);
	loadString("Header", "Header", language);
	loadString("iff", "if and only if", language);
	loadString("Inheritance", "Inheritance", language);
	loadString("Inherited_Functions", "Inherited Functions", language);
	loadString("is_deprecated", "is deprecated and should no longer be used", language);
	loadString("Known_Derived_Classes", "Known Derived Classes", language);
	loadString("Library", "Library", language);
	loadString("License", "License", language);
	loadString("Member_Functions", "Member Functions", language);
	loadString("Member_Summary", "Member Summary", language);
	loadString("more", "more...", language);
	loadString("Namespaces", "Namespaces", language);
	loadString("Namespace", "Namespace", language);
	loadString("Nested_Classes", "Nested Classes", language);
	loadString("Package", "Package", language);
	loadString("Packages", "Packages", language);
	loadString("Package_Index", "Package Index", language);
	loadString("Reference", "Reference", language);
	loadString("See_also", "See also", language);
	loadString("Struct", "Struct", language);
	loadString("Symbol_Index", "Symbol Index", language);
	loadString("This", "This", language);
	loadString("Types", "Types", language);
	loadString("Variables", "Variables", language);
}


std::string DocWriter::projectURI(const std::string& proj)
{
	Application& app = Application::instance();
	std::string key("PocoDoc.projects.");
	key += proj;
	std::string uri = app.config().getString(key, "");
	if (uri.empty()) 
	{
		app.logger().warning("No project URI found for %s", proj);
		uri = GITHUB_POCO_URI;
	}
	return uri;
}
