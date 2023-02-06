//
// Parser.h
//
// Library: CppParser
// Package: CppParser
// Module:  Parser
//
// Definition of the Parser class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_Parser_INCLUDED
#define CppParser_Parser_INCLUDED


#include "Poco/CppParser/CppParser.h"
#include "Poco/CppParser/Tokenizer.h"
#include "Poco/CppParser/Symbol.h"
#include "Poco/CppParser/NameSpace.h"
#include "Poco/CountingStream.h"
#include <vector>


namespace Poco {
namespace CppParser {


class Enum;
class Struct;
class Function;


class CppParser_API Parser
	/// A minimal parser for C++ (header files).
	///
	/// The parser reads a (preprocessed) source or header file and
	/// builds a symbol table containing as much information as
	/// the parser is able to extract from the file.
	///
	/// A special comment syntax is used for inline API documentation.
	///
	/// A comment starting with three consecutive slashes (///) contains
	/// API documentation for a symbol (class, function, typedef, enum, etc.).
	/// API documentation comments always come after the declaration, with the
	/// exception of structs and classes, where the comments are expected
	/// immediately before the opening brace.
{
public:
	Parser(NameSpace::SymbolTable& gst, const std::string& file, std::istream& istr);
		/// Creates the Parser.

	~Parser();
		/// Destroys the Parser.

	void parse();
		/// Parses the file.

protected:
	const Poco::Token* parseFile(const Poco::Token* pNext);
	const Poco::Token* parseNameSpace(const Poco::Token* pNext);
	const Poco::Token* parseClass(const Poco::Token* pNext);
	const Poco::Token* parseClass(const Poco::Token* pNext, std::string& decl);
	const Poco::Token* parseTemplate(const Poco::Token* pNext);
	const Poco::Token* parseTemplateArgs(const Poco::Token* pNext, std::string& decl);
	const Poco::Token* parseVarFunc(const Poco::Token* pNext);
	const Poco::Token* parseVarFunc(const Poco::Token* pNext, std::string& decl);
	const Poco::Token* parseFriend(const Poco::Token* pNext);
	const Poco::Token* parseExtern(const Poco::Token* pNext);
	const Poco::Token* parseTypeDef(const Poco::Token* pNext);
	const Poco::Token* parseUsing(const Poco::Token* pNext);
	const Poco::Token* parseFunc(const Poco::Token* pNext, std::string& decl);
	const Poco::Token* parseParameters(const Poco::Token* pNext, Function* pFunc);
	const Poco::Token* parseBlock(const Poco::Token* pNext);
	const Poco::Token* parseEnum(const Poco::Token* pNext);
	const Poco::Token* parseEnumValue(const Poco::Token* pNext, Enum* pEnum);
	const Poco::Token* parseBaseClassList(const Poco::Token* pNext, Struct* pClass);
	const Poco::Token* parseClassMembers(const Poco::Token* pNext, Struct* pClass);
	const Poco::Token* parseAccess(const Poco::Token* pNext);
	const Poco::Token* parseIdentifier(const Poco::Token* pNext, std::string& id);
	
	void addSymbol(Symbol* pSymbol, int lineNumber, bool addGST = true);
	void pushNameSpace(NameSpace* pNameSpace, int lineNumber, bool addGST = true);
	void popNameSpace();
	NameSpace* currentNameSpace() const;

	static bool isIdentifier(const Poco::Token* pToken);
	static bool isOperator(const Poco::Token* pToken, int kind);
	static bool isKeyword(const Poco::Token* pToken, int kind);
	static bool isEOF(const Poco::Token* pToken);
	static void expectOperator(const Poco::Token* pToken, int kind, const std::string& msg);
	static void syntaxError(const std::string& msg);
	static void append(std::string& decl, const std::string& token);
	static void append(std::string& decl, const Poco::Token* pToken);

	const Poco::Token* next();
	const Poco::Token* nextPreprocessed();
	const Poco::Token* nextToken();

private:
	typedef std::vector<NameSpace*> NSStack;

	NameSpace::SymbolTable& _gst;
	Poco::CountingInputStream _istr;
	Tokenizer      _tokenizer;
	std::string    _file;
	std::string    _path;
	std::string    _currentPath;
	bool           _inFile;
	std::string    _package;
	std::string    _library;
	NSStack        _nsStack;
	Symbol*        _pCurrentSymbol;
	Symbol::Access _access;
	std::string    _doc;
	std::string    _attrs;
};


} } // namespace Poco::CppParser


#endif // CppParser_Parser_INCLUDED
