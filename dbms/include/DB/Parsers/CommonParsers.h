#pragma once

#include <string.h>		/// strncmp, strncasecmp

#include <DB/Parsers/IParserBase.h>


namespace DB
{


/** Если прямо сейчас не s, то ошибка.
  * Если word_boundary установлен в true, и последний символ строки - словарный (\w),
  *  то проверяется, что последующий символ строки не словарный.
  */
class ParserString : public IParserBase
{
private:
	const char * s;
	size_t s_size;
	bool word_boundary;
	bool case_insensitive;

public:
	ParserString(const char * s_, bool word_boundary_ = false, bool case_insensitive_ = false)
		: s(s_), s_size(strlen(s)), word_boundary(word_boundary_), case_insensitive(case_insensitive_) {}

protected:
	const char * getName() const { return s; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
	{
		if (static_cast<ssize_t>(s_size) > end - pos || (case_insensitive ? strncasecmp : strncmp)(pos, s, s_size))
			return false;
		else
		{
			if (word_boundary && s_size && isWordCharASCII(s[s_size - 1])
				&& pos + s_size != end && isWordCharASCII(pos[s_size]))
				return false;

			pos += s_size;
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

	const char * getName() const { return "white space"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
	{
		Pos begin = pos;
		while (pos < end && (*pos == ' ' || *pos == '\t' || (allow_newlines && *pos == '\n') || *pos == '\r' || *pos == '\f'))
			++pos;

		return pos != begin;
	}
};


class ParserCStyleComment : public IParserBase
{
protected:
	const char * getName() const { return "C-style comment"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
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
	const char * getName() const { return "SQL-style comment"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
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
	const char * getName() const { return "comment"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
	{
		ParserCStyleComment p1;
		ParserSQLStyleComment p2;

		return p1.ignore(pos, end, max_parsed_pos, expected)
			|| p2.ignore(pos, end, max_parsed_pos, expected);
	}
};


class ParserWhiteSpaceOrComments : public IParserBase
{
public:
	ParserWhiteSpaceOrComments(bool allow_newlines_outside_comments_ = true) : allow_newlines_outside_comments(allow_newlines_outside_comments_) {}

protected:
	bool allow_newlines_outside_comments;

	const char * getName() const { return "white space or comments"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
	{
		ParserWhiteSpace p1(allow_newlines_outside_comments);
		ParserComment p2;

		bool res = false;
		while (p1.ignore(pos, end, max_parsed_pos, expected) || p2.ignore(pos, end, max_parsed_pos, expected))
			res = true;
		return res;
	}
};

}
