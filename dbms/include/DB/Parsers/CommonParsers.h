#pragma once

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
	ParserString(const char * s_, bool word_boundary_ = false, bool case_insensitive_ = false);

protected:
	const char * getName() const override;

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};


/** пробельные символы
  */
class ParserWhiteSpace : public IParserBase
{
public:
	ParserWhiteSpace(bool allow_newlines_ = true);

protected:
	bool allow_newlines;

	const char * getName() const override;

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};


class ParserCStyleComment : public IParserBase
{
protected:
	const char * getName() const override;

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};


class ParserSQLStyleComment : public IParserBase
{
protected:
	const char * getName() const override;

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};


/** комментарии '--' или c-style
  */
class ParserComment : public IParserBase
{
protected:
	const char * getName() const override;

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};


class ParserWhiteSpaceOrComments : public IParserBase
{
public:
	ParserWhiteSpaceOrComments(bool allow_newlines_outside_comments_ = true);

protected:
	bool allow_newlines_outside_comments;

	const char * getName() const override;

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};

}
