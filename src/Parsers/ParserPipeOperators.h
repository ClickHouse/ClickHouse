#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ASTSelectQuery;

class ParserPipeWhere
{
public:
    bool parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const;
};

class ParserPipeOrderBy
{
public:
    bool parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const;
};

class ParserPipeLimit
{
public:
    bool parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const;
};

class ParserPipeJoin
{
public:
    bool parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const;
};

class ParserPipeAggregate
{
public:
    bool parse(IParser::Pos & pos, ASTSelectQuery & query, Expected & expected) const;
};

}
