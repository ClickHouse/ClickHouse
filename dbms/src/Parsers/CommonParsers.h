#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** If right now is not `s`, then an error.
  * If word_boundary is set to true, and the last character of the string - word (\w),
  *  then it is checked that the next character in the string is not a word character.
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


/** Parse specified keyword such as SELECT or compound keyword such as ORDER BY.
  * All case insensitive. Requires word boundary.
  * For compound keywords, any whitespace characters and comments could be in the middle.
  */
/// Example: ORDER/* Hello */BY
class ParserKeyword : public IParserBase
{
private:
    const char * s;

public:
    ParserKeyword(const char * s_);

protected:
    const char * getName() const override;

    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};


/** whitespace characters
  */
class ParserWhitespace : public IParserBase
{
public:
    ParserWhitespace(bool allow_newlines_ = true);

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


/** comments '--' or c-style
  */
class ParserComment : public IParserBase
{
protected:
    const char * getName() const override;

    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};


class ParserWhitespaceOrComments : public IParserBase
{
public:
    ParserWhitespaceOrComments(bool allow_newlines_outside_comments_ = true);

protected:
    bool allow_newlines_outside_comments;

    const char * getName() const override;

    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};

}
