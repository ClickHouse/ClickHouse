//
// PageReader.h
//
// $Id: //poco/1.4/PageCompiler/src/PageReader.h#1 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PageReader_INCLUDED
#define PageReader_INCLUDED


#include "Poco/Poco.h"
#include <istream>
#include <ostream>
#include <sstream>


class Page;


class PageReader
	/// This class implements the parser for reading page files
	/// containing JSP-style tags.
{
public:
	PageReader(Page& page, const std::string& path);
		/// Creates the PageReader, using the given Page.

	PageReader(const PageReader& parent, const std::string& path);
		/// Creates the PageReader, using the given PageReader as parent.

	~PageReader();
		/// Destroys the PageReader.

	void parse(std::istream& pageStream);	
		/// Parses a HTML file containing server page directives,
		/// converts the file into C++ code and adds the code
		/// to the reader's Page object. Also parses page
		/// attributes and include directives.

	void emitLineDirectives(bool flag = true);
		/// Enables writing of #line directives to generated code.

protected:
	enum ParsingState
	{
		STATE_MARKUP,
		STATE_IMPLDECL,
		STATE_HDRDECL,
		STATE_PREHANDLER,
		STATE_BLOCK,
		STATE_EXPR,
		STATE_COMMENT,
		STATE_ATTR
	};

	static const std::string MARKUP_BEGIN;
	static const std::string MARKUP_END;
	static const std::string EXPR_BEGIN;
	static const std::string EXPR_END;

	void include(const std::string& path);
	void parseAttributes();
	void nextToken(std::istream& istr, std::string& token);
	void handleAttribute(const std::string& name, const std::string& value);
	std::string where() const;

protected:
	void generateLineDirective(std::ostream& ostr);
	
private:
	PageReader();
	PageReader(const PageReader&);
	PageReader& operator = (const PageReader&);

	Page& _page;
	const PageReader* _pParent;
	std::string _path;
	std::string _attrs;
	int _line;
	bool _emitLineDirectives;
};


#endif // PageReader_INCLUDED
