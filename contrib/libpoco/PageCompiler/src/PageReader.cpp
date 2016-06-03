//
// PageReader.cpp
//
// $Id: //poco/1.4/PageCompiler/src/PageReader.cpp#1 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "PageReader.h"
#include "Page.h"
#include "Poco/FileStream.h"
#include "Poco/CountingStream.h"
#include "Poco/Path.h"
#include "Poco/Exception.h"
#include "Poco/Ascii.h"


const std::string PageReader::MARKUP_BEGIN("\tresponseStream << \"");
const std::string PageReader::MARKUP_END("\";\n");
const std::string PageReader::EXPR_BEGIN("\tresponseStream << (");
const std::string PageReader::EXPR_END(");\n");


PageReader::PageReader(Page& page, const std::string& path):
	_page(page),
	_pParent(0),
	_path(path),
	_line(0),
	_emitLineDirectives(false)
{
	_attrs.reserve(4096);
}


PageReader::PageReader(const PageReader& parent, const std::string& path):
	_page(parent._page),
	_pParent(&parent),
	_path(path),
	_line(0),
	_emitLineDirectives(false)
{
	_attrs.reserve(4096);
}


PageReader::~PageReader()
{
}


void PageReader::emitLineDirectives(bool flag)
{
	_emitLineDirectives = flag;
}


void PageReader::parse(std::istream& pageStream)
{
	ParsingState state = STATE_MARKUP;

	_page.handler() << MARKUP_BEGIN;

	Poco::CountingInputStream countingPageStream(pageStream);
	std::string token;
	nextToken(countingPageStream, token);
	while (!token.empty())
	{
		_line = countingPageStream.getCurrentLineNumber();
		if (token == "<%")
		{
			if (state == STATE_MARKUP)
			{
				_page.handler() << MARKUP_END;
				generateLineDirective(_page.handler());
				state = STATE_BLOCK;
			}
			else _page.handler() << token;
		}
		else if (token == "<%%")
		{
			if (state == STATE_MARKUP)
			{
				_page.handler() << MARKUP_END;
				generateLineDirective(_page.preHandler());
				state = STATE_PREHANDLER;
			}
			else _page.handler() << token;
		}
		else if (token == "<%!")
		{
			if (state == STATE_MARKUP)
			{
				_page.handler() << MARKUP_END;
				generateLineDirective(_page.implDecls());
				state = STATE_IMPLDECL;
			}
			else _page.handler() << token;
		}
		else if (token == "<%!!")
		{
			if (state == STATE_MARKUP)
			{
				_page.handler() << MARKUP_END;
				generateLineDirective(_page.headerDecls());
				state = STATE_HDRDECL;
			}
			else _page.handler() << token;
		}
		else if (token == "<%--")
		{
			if (state == STATE_MARKUP)
			{
				_page.handler() << MARKUP_END;
				state = STATE_COMMENT;
			}
			else _page.handler() << token;
		}
		else if (token == "<%@")
		{
			if (state == STATE_MARKUP)
			{
				_page.handler() << MARKUP_END;
				state = STATE_ATTR;
				_attrs.clear();
			}
			else _page.handler() << token;
		}
		else if (token == "<%=")
		{
			if (state == STATE_MARKUP)
			{
				_page.handler() << MARKUP_END;
				generateLineDirective(_page.handler());
				_page.handler() << EXPR_BEGIN;
				state = STATE_EXPR;
			}
			else _page.handler() << token;
		}
		else if (token == "%>")
		{
			if (state == STATE_EXPR)
			{
				_page.handler() << EXPR_END;
				_page.handler() << MARKUP_BEGIN;
				state = STATE_MARKUP;
			}
			else if (state == STATE_ATTR)
			{
				parseAttributes();
				_attrs.clear();
				_page.handler() << MARKUP_BEGIN;
				state = STATE_MARKUP;
			}
			else if (state != STATE_MARKUP)
			{
				_page.handler() << MARKUP_BEGIN;
				state = STATE_MARKUP;
			}
			else _page.handler() << token;
		}
		else
		{
			switch (state)
			{
			case STATE_MARKUP:
				if (token == "\n")
				{
					_page.handler() << "\\n";
					_page.handler() << MARKUP_END;
					_page.handler() << MARKUP_BEGIN;
				}
				else if (token == "\t")
				{
					_page.handler() << "\\t";
				}
				else if (token == "\"")
				{
					_page.handler() << "\\\"";
				}
				else if (token != "\r")
				{
					_page.handler() << token;
				}
				break;
			case STATE_IMPLDECL:
				_page.implDecls() << token;
				break;
			case STATE_HDRDECL:
				_page.headerDecls() << token;
				break;
			case STATE_PREHANDLER:
				_page.preHandler() << token;
				break;
			case STATE_BLOCK:
				_page.handler() << token;
				break;
			case STATE_EXPR:
				_page.handler() << token;
				break;
			case STATE_COMMENT:
				break;
			case STATE_ATTR:
				_attrs += token;
				break;
			}
		}
		nextToken(countingPageStream, token);
	}

	if (state == STATE_MARKUP)
	{
		_page.handler() << MARKUP_END;
	}
	else throw Poco::SyntaxException("unclosed meta or code block", where());
}


void PageReader::parseAttributes()
{
	static const int eof = std::char_traits<char>::eof();

	std::string basename;
	std::istringstream istr(_attrs);
	int ch = istr.get();
	while (ch != eof && Poco::Ascii::isSpace(ch)) ch = istr.get();
	while (ch != eof && Poco::Ascii::isAlphaNumeric(ch)) { basename += (char) ch; ch = istr.get(); }
	while (ch != eof && Poco::Ascii::isSpace(ch)) ch = istr.get();
	while (ch != eof)
	{
		std::string name(basename + ".");
		std::string value;
		while (ch != eof && Poco::Ascii::isAlphaNumeric(ch)) { name += (char) ch; ch = istr.get(); }
		while (ch != eof && Poco::Ascii::isSpace(ch)) ch = istr.get();
		if (ch != '=') throw Poco::SyntaxException("bad attribute syntax: '=' expected", where());
		ch = istr.get();
		while (ch != eof && Poco::Ascii::isSpace(ch)) ch = istr.get();
		if (ch == '"')
		{
			ch = istr.get();
			while (ch != eof && ch != '"') { value += (char) ch; ch = istr.get(); }
			if (ch != '"') throw Poco::SyntaxException("bad attribute syntax: '\"' expected", where());
		}
		else if (ch == '\'')
		{
			ch = istr.get();
			while (ch != eof && ch != '\'') { value += (char) ch; ch = istr.get(); }
			if (ch != '\'') throw Poco::SyntaxException("bad attribute syntax: ''' expected", where());
		}
		else throw Poco::SyntaxException("bad attribute syntax: '\"' or ''' expected", where());
		ch = istr.get();
		handleAttribute(name, value);
		while (ch != eof && Poco::Ascii::isSpace(ch)) ch = istr.get();
	}
}


void PageReader::nextToken(std::istream& istr, std::string& token)
{
	token.clear();
	int ch = istr.get();
	if (ch != -1)
	{
		if (ch == '<' && istr.peek() == '%')
		{
			token += "<%";
			ch = istr.get();
			ch = istr.peek();
			switch (ch)
			{
			case '%':
			case '@':
			case '=':
				ch = istr.get();
				token += (char) ch;
				break;
			case '!':
				ch = istr.get();
				token += (char) ch;
				if (istr.peek() == '!')
				{
					ch = istr.get();
					token += (char) ch;
				}
				break;
			case '-':
				ch = istr.get();
				token += (char) ch;
				if (istr.peek() == '-')
				{
					ch = istr.get();
					token += (char) ch;
				}
				break;
			}
		}
		else if (ch == '%' && istr.peek() == '>')
		{
			token += "%>";
			ch = istr.get();
		}
		else token += (char) ch;
	}
}


void PageReader::handleAttribute(const std::string& name, const std::string& value)
{
	if (name == "include.page")
	{
		include(value);
	}
	else if (name == "header.include")
	{
		_page.headerDecls() << "#include \"" << value << "\"\n";
	}
	else if (name == "header.sinclude")
	{
		_page.headerDecls() << "#include <" << value << ">\n";
	}
	else if (name == "impl.include")
	{
		_page.implDecls() << "#include \"" << value << "\"\n";
	}
	else if (name == "impl.sinclude")
	{
		_page.implDecls() << "#include <" << value << ">\n";
	}
	else
	{
		_page.set(name, value);
	}
}


void PageReader::include(const std::string& path)
{
	Poco::Path currentPath(_path);
	Poco::Path includePath(path);
	currentPath.resolve(includePath);
	
	_page.handler() << "\t// begin include " << currentPath.toString() << "\n";
	
	Poco::FileInputStream includeStream(currentPath.toString());
	PageReader includeReader(*this, currentPath.toString());
	includeReader.emitLineDirectives(_emitLineDirectives);
	includeReader.parse(includeStream);
	
	_page.handler() << "\t// end include " << currentPath.toString() << "\n";
}


std::string PageReader::where() const
{
	std::stringstream result;
	result << "in file '" << _path << "', line " << _line;
	const PageReader* pParent = _pParent;
	while (pParent)
	{
		result << "\n\tincluded from file '"<<  pParent->_path << "', line " << pParent->_line;
		pParent = pParent->_pParent;
	}
	return result.str();
}


void PageReader::generateLineDirective(std::ostream& ostr)
{
	if (_emitLineDirectives)
	{
		Poco::Path p(_path);
		p.makeAbsolute();
		std::string absPath = p.toString();
		ostr << "#line " << _line << " \"";
		for (std::string::const_iterator it = absPath.begin(); it != absPath.end(); ++it)
		{
			if (*it == '\\')
				ostr << "\\\\";
			else
				ostr << *it;
		}
		ostr << "\"\n";
	}
}
