#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Common/typeid_cast.h>
#include <Poco/String.h>


namespace DB
{

/** A nested table. For example, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
  */
class ParserNestedTable : public IParserBase
{
protected:
    const char * getName() const { return "nested table"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


/** Parametric type or Storage. For example:
 *         FixedString(10) or
 *         Partitioned(Log, ChunkID) or
 *         Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
 * Result of parsing - ASTFunction with or without parameters.
 */
class ParserIdentifierWithParameters : public IParserBase
{
protected:
    const char * getName() const { return "identifier with parameters"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


/** Data type or table engine, possibly with parameters. For example, UInt8 or see examples from ParserIdentifierWithParameters
  * Parse result is ASTFunction, with or without arguments.
  */
class ParserIdentifierWithOptionalParameters : public IParserBase
{
protected:
    const char * getName() const { return "identifier with optional parameters"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


template <typename NameParser>
class IParserNameTypePair : public IParserBase
{
protected:
    const char * getName() const { return "name and type pair"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

/** The name and type are separated by a space. For example, URL String. */
using ParserNameTypePair = IParserNameTypePair<ParserIdentifier>;
/** Name and type separated by a space. The name can contain a dot. For example, Hits.URL String. */
using ParserCompoundNameTypePair = IParserNameTypePair<ParserCompoundIdentifier>;

template <typename NameParser>
bool IParserNameTypePair<NameParser>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    NameParser name_parser;
    ParserIdentifierWithOptionalParameters type_parser;

    ASTPtr name, type;
    if (name_parser.parse(pos, name, expected)
        && type_parser.parse(pos, type, expected))
    {
        auto name_type_pair = std::make_shared<ASTNameTypePair>();
        name_type_pair->name = typeid_cast<const ASTIdentifier &>(*name).name;
        name_type_pair->type = type;
        name_type_pair->children.push_back(type);
        node = name_type_pair;
        return true;
    }

    return false;
}

/** List of columns. */
class ParserNameTypePairList : public IParserBase
{
protected:
    const char * getName() const { return "name and type pair list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


template <typename NameParser>
class IParserColumnDeclaration : public IParserBase
{
protected:
    const char * getName() const { return "column declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
    bool isDeclareColumnType(Pos & pos, Expected & expected);
    bool isDeclareColumnCodec(Pos & pos, Expected & expected);
};

using ParserColumnDeclaration = IParserColumnDeclaration<ParserIdentifier>;
using ParserCompoundColumnDeclaration = IParserColumnDeclaration<ParserCompoundIdentifier>;

template <typename NameParser>
bool IParserColumnDeclaration<NameParser>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr column_name;
    ASTPtr column_type;
    ASTPtr column_codec;
    NameParser name_parser;
    ParserKeyword s_alias{"ALIAS"};
    ParserKeyword s_default{"DEFAULT"};
    ParserKeyword s_materialized{"MATERIALIZED"};
    ParserIdentifierWithParameters codec_parser;
    ParserTernaryOperatorExpression expr_parser;
    ParserIdentifierWithOptionalParameters type_parser;

    if (!name_parser.parse(pos, column_name, expected))
        return false;

    if (isDeclareColumnType(pos, expected))
        type_parser.parse(pos, column_type, expected);

    if (isDeclareColumnCodec(pos, expected))
        codec_parser.parse(pos, column_codec, expected);

    String default_specifier;
    ASTPtr default_expression;
    Pos pos_before_specifier = pos;
    if (s_default.ignore(pos, expected) ||
        s_materialized.ignore(pos, expected) ||
        s_alias.ignore(pos, expected))
    {
        default_specifier = Poco::toUpper(std::string{pos_before_specifier->begin, pos_before_specifier->end});

        /// should be followed by an expression
        if (!expr_parser.parse(pos, default_expression, expected))
            return false;
    }

    const auto column_declaration = std::make_shared<ASTColumnDeclaration>();

    column_declaration->name = typeid_cast<ASTIdentifier *>(column_name.get())->name;

    if (column_type)
    {
        column_declaration->type = column_type;
        column_declaration->children.push_back(std::move(column_type));
    }

    if (column_codec)
    {
        column_declaration->codec = column_codec;
        column_declaration->children.push_back(std::move(column_codec));
    }

    if (default_expression)
    {
        column_declaration->default_specifier = default_specifier;
        column_declaration->default_expression = default_expression;
        column_declaration->children.push_back(std::move(default_expression));
    }

    node = column_declaration;
    return true;
}

template<typename NameParser>
bool IParserColumnDeclaration<NameParser>::isDeclareColumnType(Pos & pos, Expected & expected)
{
    auto check_pos = pos;
    return !ParserKeyword{"CODEC"}.check(check_pos, expected) &&
           !ParserKeyword{"ALIAS"}.check(check_pos, expected) &&
           !ParserKeyword{"DEFAULT"}.check(check_pos, expected) &&
           !ParserKeyword{"MATERIALIZED"}.check(check_pos, expected);
}
template<typename NameParser>
bool IParserColumnDeclaration<NameParser>::isDeclareColumnCodec(Pos & pos, Expected & expected)
{
    auto check_pos = pos;
    return ParserKeyword{"CODEC"}.check(check_pos, expected);
}

class ParserColumnDeclarationList : public IParserBase
{
protected:
    const char * getName() const { return "column declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


/** ENGINE = name [PARTITION BY expr] [ORDER BY expr] [SAMPLE BY expr] [SETTINGS name = value, ...] */
class ParserStorage : public IParserBase
{
protected:
    const char * getName() const { return "storage definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


/** Query like this:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name
  * (
  *     name1 type1,
  *     name2 type2,
  *     ...
  * ) ENGINE = engine
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name AS [db2.]name2 [ENGINE = engine]
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name AS ENGINE = engine SELECT ...
  *
  * Or:
  * CREATE|ATTACH DATABASE db [ENGINE = engine]
  *
  * Or:
  * CREATE|ATTACH [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]name [TO [db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
  */
class ParserCreateQuery : public IParserBase
{
protected:
    const char * getName() const { return "CREATE TABLE or ATTACH TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
