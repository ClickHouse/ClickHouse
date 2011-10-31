#pragma once

#include <string.h>		/// strncmp, strncasecmp

#include <DB/Parsers/IParserBase.h>


namespace DB
{

using Poco::SharedPtr;


/** Если прямо сейчас не s, то ошибка.
  * Если word_boundary установлен в true, и последний символ строки - словарный (\w),
  *  то проверяется, что последующий символ строки не словарный.
  */
class ParserString : public IParserBase
{
private:
	String s;
	bool word_boundary;
	bool case_insensitive;

	inline bool is_word(char c)
	{
		return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c == '_');
	}
	
public:
	ParserString(const String & s_, bool word_boundary_ = false, bool case_insensitive_ = false)
		: s(s_), word_boundary(word_boundary_), case_insensitive(case_insensitive_) {}
	
protected:
	String getName() { return s; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		if (static_cast<ssize_t>(s.size()) > end - pos || (case_insensitive ? strncasecmp : strncmp)(pos, s.data(), s.size()))
			return false;
		else
		{
			if (word_boundary && s.size() && is_word(*s.rbegin())
				&& pos + s.size() != end && is_word(pos[s.size()]))
				return false;
		
			pos += s.size();
 			return true;
 		}
	}
};

	
/** пробельные символы
  */
class ParserWhiteSpace : public IParserBase
{
public:
	ParserWhiteSpace(bool allow_newlines_ = true) : allow_newlines(allow_newlines_) {}

protected:
	bool allow_newlines;
	
	String getName() { return "white space"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		Pos begin = pos;
		while (*pos == ' ' || *pos == '\t' || (allow_newlines && *pos == '\n') || *pos == '\r' || *pos == '\f')
			++pos;

		return pos != begin;
	}
};


class ParserCStyleComment : public IParserBase
{
protected:
	String getName() { return "C-style comment"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		if (end - pos >= 4 && pos[0] == '/' && pos[1] == '*')
		{
			pos += 2;
			while (end - pos >= 2 && (pos[0] != '*' || pos[1] != '/'))
				++pos;

			if (end - pos < 2)
			{
				expected = "closing of C-style comment '*/'";
				return false;
			}
			else
			{
				pos += 2;
				return true;
			}
		}
		else
			return false;
	}
};


class ParserSQLStyleComment : public IParserBase
{
protected:
	String getName() { return "SQL-style comment"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		if (end - pos >= 2 && pos[0] == '-' && pos[1] == '-')
		{
			pos += 2;
			while (pos != end && *pos != '\n')
				++pos;

			if (pos != end)
				++pos;
			return true;
		}
		else
			return false;
	}
};


/** комментарии '--' или c-style
  */
class ParserComment : public IParserBase
{
protected:
	String getName() { return "comment"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		ParserCStyleComment p1;
		ParserSQLStyleComment p2;
	
		return p1.ignore(pos, end, expected)
			|| p2.ignore(pos, end, expected);
	}
};


class ParserWhiteSpaceOrComments : public IParserBase
{
public:
	ParserWhiteSpaceOrComments(bool allow_newlines_outside_comments_ = true) : allow_newlines_outside_comments(allow_newlines_outside_comments_) {}
	
protected:
	bool allow_newlines_outside_comments;
	
	String getName() { return "white space or comments"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		ParserWhiteSpace p1(allow_newlines_outside_comments);
		ParserComment p2;
	
		bool res = false;
		while (p1.ignore(pos, end, expected) || p2.ignore(pos, end, expected))
			res = true;
		return res;
	}
};

}
