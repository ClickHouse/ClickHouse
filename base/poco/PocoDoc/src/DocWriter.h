//
// DocWriter.h
//
// Definition of the DocWriter class.
//
// Copyright (c) 2005-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PocoDoc_DocWriter_INCLUDED
#define PocoDoc_DocWriter_INCLUDED


#include "Poco/CppParser/NameSpace.h"
#include "Poco/Logger.h"
#include <vector>
#include <set>
#include <ostream>


namespace Poco {
namespace CppParser {


class Symbol;
class Struct;
class Function;
class TypeDef;
class Enum;
class Variable;


} } // namespace Poco::CppParser


class DocWriter
	/// Given a symbol table obtained from a CppParser, this
	/// class writes reference documentation in HTML format
	/// to a directory.
{
public:
	DocWriter(const Poco::CppParser::NameSpace::SymbolTable& symbols, const std::string& path, bool prettifyCode = true, bool noFrames = false);
		/// Creates the DocWriter.

	~DocWriter();
		/// Destroys the DocWriter.
		
	void write();
		/// Writes all documentation files.
		
	void writeEclipseTOC();
		/// Write Eclipse Table-Of-Contents XML files.
		
	void addPage(const std::string& path);
		/// Adds a page.

protected:
	enum TextState
	{
		TEXT_PARAGRAPH,
		TEXT_LIST,
		TEXT_OLIST,
		TEXT_LITERAL,
		TEXT_WHITESPACE
	};
	
	struct Page
	{
		std::string path;
		std::string fileName;
		std::string title;
		std::string category;
	};
	
	enum
	{
		MAX_TITLE_LEVEL = 3,
		PAGE_INDEX_COLUMNS = 2,
		NAMESPACE_INDEX_COLUMNS = 4
	};
	
	struct TOCEntry
	{
		std::string title;
		int level;
		int id;
	};
	
	typedef std::vector<TOCEntry> TOC;
	typedef std::map<std::string, Poco::CppParser::Function*> MethodMap;	
	typedef std::map<std::string, std::string> StringMap;	
	typedef std::map<std::string, Page> PageMap;

	void writePages();
	void writePage(Page& page);
	void scanTOC(const std::string& text, TOC& toc);
	void writeTOC(std::ostream& ostr, const TOC& toc);
	void writeCategoryIndex(const std::string& category, const std::string& fileName);
	void writeCategoryIndex(std::ostream& ostr, const std::string& category, const std::string& target);
	void writePageIndex(std::ostream& ostr);
	void writeNameSpaceIndex(std::ostream& ostr);
	
	void writeClass(const Poco::CppParser::Struct* pStruct);
	void writeNameSpace(const Poco::CppParser::NameSpace* pNameSpace);
	
	void writeNavigation();
	void writePackage(const std::string& file, const std::string& library, const std::string& package);

	std::string pathFor(const std::string& file);
	static std::string fileNameFor(const Poco::CppParser::Symbol* pNameSpace);
	static std::string baseNameFor(const Poco::CppParser::Symbol* pNameSpace);
	static std::string uriFor(const Poco::CppParser::Symbol* pSymbol);
	static std::string makeFileName(const std::string& str);
	static std::string headerFor(const Poco::CppParser::Symbol* pSymbol);
	static std::string titleFor(const Poco::CppParser::Symbol* pSymbol);

	void writeHeader(std::ostream& ostr, const std::string& title, const std::string& extraScript = "");
	void writeNavigationFrame(std::ostream& ostr, const std::string& group, const std::string& item);
	static void writeFooter(std::ostream& ostr);
	void writeCopyright(std::ostream& ostr);
	static void writeTitle(std::ostream& ostr, const std::string& category, const std::string& title);
	static void writeTitle(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace, const std::string& title);
	static void writeSubTitle(std::ostream& ostr, const std::string& title);
	static void beginBody(std::ostream& ostr);
	static void endBody(std::ostream& ostr);
	static void beginContent(std::ostream& ostr);
	static void endContent(std::ostream& ostr);
	void writeDescription(std::ostream& ostr, const std::string& text);
	void writeDescriptionLine(std::ostream& ostr, const std::string& text, TextState& state);
	void writeSummary(std::ostream& ostr, const std::string& text, const std::string& uri);
	static std::string htmlize(const std::string& str);
	static std::string htmlize(char c);
	static TextState analyzeLine(const std::string& line);
	static std::string htmlizeName(const std::string& name);
	void writeText(std::ostream& ostr, const std::string& text);
	void writeText(std::ostream& ostr, std::string::const_iterator begin, const std::string::const_iterator& end);
	void writeDecl(std::ostream& ostr, const std::string& decl);
	void writeDecl(std::ostream& ostr, std::string::const_iterator begin, const std::string::const_iterator& end);
	bool writeSymbol(std::ostream& ostr, std::string& token, std::string::const_iterator& begin, const std::string::const_iterator& end);
	bool writeSpecial(std::ostream& ostr, std::string& token, std::string::const_iterator& begin, const std::string::const_iterator& end);
	void nextToken(std::string::const_iterator& it, const std::string::const_iterator& end, std::string& token);
	void writeListItem(std::ostream& ostr, const std::string& text);
	void writeOrderedListItem(std::ostream& ostr, const std::string& text);
	void writeLiteral(std::ostream& ostr, const std::string& text);
	void writeFileInfo(std::ostream& ostr, const Poco::CppParser::Symbol* pSymbol);
	void writeInheritance(std::ostream& ostr, const Poco::CppParser::Struct* pStruct);
	void writeMethodSummary(std::ostream& ostr, const Poco::CppParser::Struct* pStruct);
	void writeNestedClasses(std::ostream& ostr, const Poco::CppParser::Struct* pStruct);
	void writeNameSpacesSummary(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace);
	void writeNameSpaces(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace);
	void writeClassesSummary(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace);
	void writeClasses(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace);
	void writeClassSummary(std::ostream& ostr, const Poco::CppParser::Struct* pStruct);
	void writeTypesSummary(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace);
	void writeTypes(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace);
	void writeType(std::ostream& ostr, const Poco::CppParser::TypeDef* pType);
	void writeEnums(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace);
	void writeEnum(std::ostream& ostr, const Poco::CppParser::Enum* pEnum);
	void writeConstructors(std::ostream& ostr, const Poco::CppParser::Struct* pStruct);
	void writeDestructor(std::ostream& ostr, const Poco::CppParser::Struct* pStruct);
	void writeMethods(std::ostream& ostr, const Poco::CppParser::Struct* pNameSpace);
	void writeFunctionsSummary(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace);
	void writeFunctions(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace);
	void writeFunction(std::ostream& ostr, const Poco::CppParser::Function* pFunc);
	void writeVariables(std::ostream& ostr, const Poco::CppParser::NameSpace* pNameSpace);
	void writeVariable(std::ostream& ostr, const Poco::CppParser::Variable* pVar);
	static void writeNameListItem(std::ostream& ostr, const std::string& name, const Poco::CppParser::Symbol* pSymbol, const Poco::CppParser::NameSpace* pNameSpace, bool& first);
	static void writeLink(std::ostream& ostr, const std::string& uri, const std::string& text);
	static void writeLink(std::ostream& ostr, const Poco::CppParser::Symbol* pSymbol, const std::string& text);
	static void writeLink(std::ostream& ostr, const std::string& uri, const std::string& text, const std::string& linkClass);
	void writeTargetLink(std::ostream& ostr, const std::string& uri, const std::string& text, const std::string& target);
	static void writeImageLink(std::ostream& ostr, const std::string& uri, const std::string& image, const std::string& alt);
	static void writeImage(std::ostream& ostr, const std::string& uri, const std::string& caption);
	static void writeIcon(std::ostream& ostr, const std::string& icon);
	static void writeAnchor(std::ostream& ostr, const std::string& text, const Poco::CppParser::Symbol* pSymbol);
	static void writeDeprecated(std::ostream& ostr, const std::string& what);
	void libraries(std::set<std::string>& libs);
	void packages(const std::string& lib, std::set<std::string>& packages);

	Poco::CppParser::NameSpace* rootNameSpace() const;

	static const std::string& tr(const std::string& id);
	static void loadStrings(const std::string& language);
	static void loadString(const std::string& id, const std::string& def, const std::string& language);
	static std::string projectURI(const std::string& id);

	static Poco::Logger& logger();
	
	static const std::string RFC_URI;
	static const std::string GITHUB_POCO_URI;
	
private:	
	bool _prettifyCode;
	bool _noFrames;
	bool _htmlMode;
	bool _literalMode;
	const Poco::CppParser::NameSpace::SymbolTable& _symbols;
	std::string _path;
	const Poco::CppParser::NameSpace* _pNameSpace;
	PageMap _pages;
	bool _pendingLine;
	int  _indent;
	int  _titleId;
	
	static std::string _language;
	static StringMap   _strings;
	
	static Poco::Logger* _pLogger;
};


//
// inlines
//
inline Poco::Logger& DocWriter::logger()
{
	poco_check_ptr (_pLogger);

	return *_pLogger;
}


#endif // PocoDoc_DocWriter_INCLUDED
