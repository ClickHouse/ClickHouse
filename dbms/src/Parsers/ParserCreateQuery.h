#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
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
        getIdentifierName(name, name_type_pair->name);
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
public:
    explicit IParserColumnDeclaration(bool require_type_ = true,
                                      bool parse_key_value_pairs_ = false)
    : require_type(require_type_)
    , parse_key_value_pairs(parse_key_value_pairs_)
    {
    }

protected:

    const char * getName() const { return "column declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);

    bool require_type = true;
    bool parse_key_value_pairs = false;
};

using ParserColumnDeclaration = IParserColumnDeclaration<ParserIdentifier>;
using ParserCompoundColumnDeclaration = IParserColumnDeclaration<ParserCompoundIdentifier>;

template <typename NameParser>
bool IParserColumnDeclaration<NameParser>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    NameParser name_parser;
    ParserIdentifierWithOptionalParameters type_parser;
    ParserKeyword s_default{"DEFAULT"};
    ParserKeyword s_materialized{"MATERIALIZED"};
    ParserKeyword s_alias{"ALIAS"};
    ParserKeyword s_comment{"COMMENT"};
    ParserKeyword s_codec{"CODEC"};
    ParserTernaryOperatorExpression expr_parser;
    ParserStringLiteral string_literal_parser;
    ParserCodec codec_parser;

    ParserKeyValuePairsList pairs_list_parser(TokenType::Whitespace);

    /// mandatory column name
    ASTPtr name;
    if (!name_parser.parse(pos, name, expected))
        return false;

    /** column name should be followed by type name if it
      *    is not immediately followed by {DEFAULT, MATERIALIZED, ALIAS, COMMENT}
      */
    ASTPtr type;
    String default_specifier;
    ASTPtr default_expression;
    ASTPtr comment_expression;
    ASTPtr codec_expression;
    ASTPtr pairs_list;

    if (!s_default.checkWithoutMoving(pos, expected) &&
        !s_materialized.checkWithoutMoving(pos, expected) &&
        !s_alias.checkWithoutMoving(pos, expected) &&
        !s_comment.checkWithoutMoving(pos, expected) &&
        !s_codec.checkWithoutMoving(pos, expected))
    {
        if (!type_parser.parse(pos, type, expected))
            return false;
    }

    Pos pos_before_specifier = pos;
    if (s_default.ignore(pos, expected) || s_materialized.ignore(pos, expected) || s_alias.ignore(pos, expected))
    {
        default_specifier = Poco::toUpper(std::string{pos_before_specifier->begin, pos_before_specifier->end});

        /// should be followed by an expression
        if (!expr_parser.parse(pos, default_expression, expected))
            return false;
    }

    if (require_type && !type && !default_expression)
        return false; /// reject column name without type

    if (s_comment.ignore(pos, expected))
    {
        /// should be followed by a string literal
        if (!string_literal_parser.parse(pos, comment_expression, expected))
            return false;
    }

    if (s_codec.ignore(pos, expected))
    {
        if (!codec_parser.parse(pos, codec_expression, expected))
            return false;
    }

    if (parse_key_value_pairs && !pairs_list_parser.parse(pos, pairs_list, expected))
    {
        return false;
    }

    const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
    node = column_declaration;
    getIdentifierName(name, column_declaration->name);

    if (type)
    {
        column_declaration->type = type;
        column_declaration->children.push_back(std::move(type));
    }

    if (default_expression)
    {
        column_declaration->default_specifier = default_specifier;
        column_declaration->default_expression = default_expression;
        column_declaration->children.push_back(std::move(default_expression));
    }

    if (comment_expression)
    {
        column_declaration->comment = comment_expression;
        column_declaration->children.push_back(std::move(comment_expression));
    }

    if (codec_expression)
    {
        column_declaration->codec = codec_expression;
        column_declaration->children.push_back(std::move(codec_expression));
    }

    if (pairs_list)
    {
        column_declaration->expr_list = pairs_list;
        column_declaration->children.push_back(std::move(pairs_list));
    }

    return true;
}

class ParserColumnDeclarationList : public IParserBase
{
public:
    explicit ParserColumnDeclarationList(bool parse_key_value_pairs_ = false)
    : parse_key_value_pairs(parse_key_value_pairs_)
    {}

protected:
    const char * getName() const { return "column declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);

    bool parse_key_value_pairs = false;
};


/** name BY expr TYPE typename(arg1, arg2, ...) GRANULARITY value */
class ParserIndexDeclaration : public IParserBase
{
public:
    ParserIndexDeclaration() {}

protected:
    const char * getName() const override { return "index declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserColumnAndIndexDeclaration : public IParserBase
{
public:
    ParserColumnAndIndexDeclaration(bool parse_key_value_pairs_ = false)
    : parse_key_value_pairs(parse_key_value_pairs_)
    {}

protected:
    const char * getName() const override { return "column or index declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    bool parse_key_value_pairs = false;
};


class ParserIndexDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "index declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserColumnsOrIndicesDeclarationList : public IParserBase
{
public:
    explicit ParserColumnsOrIndicesDeclarationList(bool parse_key_value_pairs_ = false)
    : parse_key_value_pairs(parse_key_value_pairs_)
    {}

protected:
    const char * getName() const override { return "columns or indices declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    bool parse_key_value_pairs = false;
};


/**
  * ENGINE = name [PARTITION BY expr] [ORDER BY expr] [PRIMARY KEY expr] [SAMPLE BY expr] [SETTINGS name = value, ...]
  */
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
  *     INDEX name1 expr TYPE type1(args) GRANULARITY value,
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
  * CREATE [OR REPLACE]|ATTACH [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]name [TO [db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
  */
class ParserCreateQuery : public IParserBase
{
protected:
    const char * getName() const { return "CREATE TABLE or ATTACH TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};


/*
 * LIFETIME(MIN 10, MAX 100)
 */
class ParserDictionaryLifetime : public IParserBase
{
protected:
    const char * getName() const override;
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/*
 * RANGE(MIN startDate, MAX endDate)
 */
class ParserDictionaryRange : public IParserBase
{
protected:
    const char * getName() const override;
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/**
  * SOURCE(SOURCE_TYPE(SOURCE PARAMS))
  *
  */
class ParserDictionarySource : public IParserBase
{
protected:
    const char * getName() const override;
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/**
  * CREATE DICTIONARY db.name
  */
class ParserCreateDictionaryQuery : public IParserBase
{
protected:
    const char * getName() const override;
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
