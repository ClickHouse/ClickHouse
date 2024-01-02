#pragma once

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserDataType.h>
#include <Poco/String.h>

namespace DB
{

/** A nested table. For example, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
  */
class ParserNestedTable : public IParserBase
{
protected:
    const char * getName() const override { return "nested table"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** Storage engine or Codec. For example:
 *         Memory()
 *         ReplicatedMergeTree('/path', 'replica')
 * Result of parsing - ASTFunction with or without parameters.
 */
class ParserIdentifierWithParameters : public IParserBase
{
protected:
    const char * getName() const override { return "identifier with parameters"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

template <typename NameParser>
class IParserNameTypePair : public IParserBase
{
protected:
    const char * getName() const  override{ return "name and type pair"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** The name and type are separated by a space. For example, URL String. */
using ParserNameTypePair = IParserNameTypePair<ParserIdentifier>;

template <typename NameParser>
bool IParserNameTypePair<NameParser>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    NameParser name_parser;
    ParserDataType type_parser;

    ASTPtr name;
    ASTPtr type;
    if (name_parser.parse(pos, name, expected)
        && type_parser.parse(pos, type, expected))
    {
        auto name_type_pair = std::make_shared<ASTNameTypePair>();
        tryGetIdentifierNameInto(name, name_type_pair->name);
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
    const char * getName() const override { return "name and type pair list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** List of table names. */
class ParserNameList : public IParserBase
{
protected:
    const char * getName() const override { return "name list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


template <typename NameParser>
class IParserColumnDeclaration : public IParserBase
{
public:
    explicit IParserColumnDeclaration(bool require_type_ = true, bool allow_null_modifiers_ = false, bool check_keywords_after_name_ = false)
    : require_type(require_type_)
    , allow_null_modifiers(allow_null_modifiers_)
    , check_keywords_after_name(check_keywords_after_name_)
    {
    }

    void enableCheckTypeKeyword() { check_type_keyword = true; }

protected:
    using ASTDeclarePtr = std::shared_ptr<ASTColumnDeclaration>;

    const char * getName() const  override{ return "column declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    const bool require_type = true;
    const bool allow_null_modifiers = false;
    const bool check_keywords_after_name = false;
    /// just for ALTER TABLE ALTER COLUMN use
    bool check_type_keyword = false;
};

using ParserColumnDeclaration = IParserColumnDeclaration<ParserIdentifier>;
using ParserCompoundColumnDeclaration = IParserColumnDeclaration<ParserCompoundIdentifier>;

template <typename NameParser>
bool IParserColumnDeclaration<NameParser>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    NameParser name_parser;
    ParserDataType type_parser;
    ParserKeyword s_default{"DEFAULT"};
    ParserKeyword s_null{"NULL"};
    ParserKeyword s_not{"NOT"};
    ParserKeyword s_materialized{"MATERIALIZED"};
    ParserKeyword s_ephemeral{"EPHEMERAL"};
    ParserKeyword s_alias{"ALIAS"};
    ParserKeyword s_auto_increment{"AUTO_INCREMENT"};
    ParserKeyword s_comment{"COMMENT"};
    ParserKeyword s_codec{"CODEC"};
    ParserKeyword s_ttl{"TTL"};
    ParserKeyword s_remove{"REMOVE"};
    ParserKeyword s_type{"TYPE"};
    ParserKeyword s_collate{"COLLATE"};
    ParserKeyword s_primary_key{"PRIMARY KEY"};
    ParserExpression expr_parser;
    ParserStringLiteral string_literal_parser;
    ParserLiteral literal_parser;
    ParserCodec codec_parser;
    ParserCollation collation_parser;
    ParserExpression expression_parser;

    /// mandatory column name
    ASTPtr name;
    if (!name_parser.parse(pos, name, expected))
        return false;

    const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
    tryGetIdentifierNameInto(name, column_declaration->name);

    /// This keyword may occur only in MODIFY COLUMN query. We check it here
    /// because ParserDataType parses types as an arbitrary identifiers and
    /// doesn't check that parsed string is existing data type. In this way
    /// REMOVE keyword can be parsed as data type and further parsing will fail.
    /// So we just check this keyword and in case of success return column
    /// declaration with name only.
    if (!require_type && s_remove.checkWithoutMoving(pos, expected))
    {
        if (!check_keywords_after_name)
            return false;

        node = column_declaration;
        return true;
    }

    /** column name should be followed by type name if it
      *    is not immediately followed by {DEFAULT, MATERIALIZED, ALIAS, COMMENT}
      */
    ASTPtr type;
    String default_specifier;
    std::optional<bool> null_modifier;
    bool ephemeral_default = false;
    ASTPtr default_expression;
    ASTPtr comment_expression;
    ASTPtr codec_expression;
    ASTPtr ttl_expression;
    ASTPtr collation_expression;
    bool primary_key_specifier = false;

    auto null_check_without_moving = [&]() -> bool
    {
        if (!allow_null_modifiers)
            return false;

        if (s_null.checkWithoutMoving(pos, expected))
            return true;

        Pos before_null = pos;
        bool res = s_not.check(pos, expected) && s_null.checkWithoutMoving(pos, expected);
        pos = before_null;
        return res;
    };

    if (!null_check_without_moving()
        && !s_default.checkWithoutMoving(pos, expected)
        && !s_materialized.checkWithoutMoving(pos, expected)
        && !s_ephemeral.checkWithoutMoving(pos, expected)
        && !s_alias.checkWithoutMoving(pos, expected)
        && !s_auto_increment.checkWithoutMoving(pos, expected)
        && !s_primary_key.checkWithoutMoving(pos, expected)
        && (require_type
            || (!s_comment.checkWithoutMoving(pos, expected)
                && !s_codec.checkWithoutMoving(pos, expected))))
    {
        if (check_type_keyword && !s_type.ignore(pos, expected))
            return false;
        if (!type_parser.parse(pos, type, expected))
            return false;
        if (s_collate.ignore(pos, expected))
        {
            if (!collation_parser.parse(pos, collation_expression, expected))
                return false;
        }
    }

    if (allow_null_modifiers)
    {
        if (s_not.check(pos, expected))
        {
            if (!s_null.check(pos, expected))
                return false;
            null_modifier.emplace(false);
        }
        else if (s_null.check(pos, expected))
            null_modifier.emplace(true);
    }

    Pos pos_before_specifier = pos;
    if (s_default.ignore(pos, expected) || s_materialized.ignore(pos, expected) || s_alias.ignore(pos, expected))
    {
        default_specifier = Poco::toUpper(std::string{pos_before_specifier->begin, pos_before_specifier->end});

        /// should be followed by an expression
        if (!expr_parser.parse(pos, default_expression, expected))
            return false;
    }
    else if (s_ephemeral.ignore(pos, expected))
    {
        default_specifier = s_ephemeral.getName();
        if (!expr_parser.parse(pos, default_expression, expected) && type)
        {
            ephemeral_default = true;

            auto default_function = std::make_shared<ASTFunction>();
            default_function->name = "defaultValueOfTypeName";
            default_function->arguments = std::make_shared<ASTExpressionList>();
            // Ephemeral columns don't really have secrets but we need to format
            // into a String, hence the strange call
            default_function->arguments->children.emplace_back(std::make_shared<ASTLiteral>(type->as<ASTFunction>()->formatForLogging()));
            default_expression = default_function;
        }

        if (!default_expression && !type)
            return false;
    }
    else if (s_auto_increment.ignore(pos, expected))
    {
        default_specifier = s_auto_increment.getName();
        /// if type is not provided for a column with AUTO_INCREMENT then using INT by default
        if (!type)
        {
            const String type_int("INT");
            Tokens tokens(type_int.data(), type_int.data() + type_int.size());
            Pos tmp_pos(tokens, 0);
            Expected tmp_expected;
            ParserDataType().parse(tmp_pos, type, tmp_expected);
        }
    }
    /// This will rule out unusual expressions like *, t.* that cannot appear in DEFAULT
    if (default_expression && !dynamic_cast<const ASTWithAlias *>(default_expression.get()))
        return false;

    if (require_type && !type && !default_expression)
        return false; /// reject column name without type

    if ((type || default_expression) && allow_null_modifiers && !null_modifier.has_value())
    {
        if (s_not.ignore(pos, expected))
        {
            if (!s_null.ignore(pos, expected))
                return false;
            null_modifier.emplace(false);
        }
        else if (s_null.ignore(pos, expected))
            null_modifier.emplace(true);
    }

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

    if (s_ttl.ignore(pos, expected))
    {
        if (!expression_parser.parse(pos, ttl_expression, expected))
            return false;
    }

    if (s_primary_key.ignore(pos, expected))
    {
        primary_key_specifier = true;
    }

    node = column_declaration;

    if (type)
    {
        column_declaration->type = type;
        column_declaration->children.push_back(std::move(type));
    }

    column_declaration->null_modifier = null_modifier;

    column_declaration->default_specifier = default_specifier;
    if (default_expression)
    {
        column_declaration->ephemeral_default = ephemeral_default;
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

    if (ttl_expression)
    {
        column_declaration->ttl = ttl_expression;
        column_declaration->children.push_back(std::move(ttl_expression));
    }
    if (collation_expression)
    {
        column_declaration->collation = collation_expression;
        column_declaration->children.push_back(std::move(collation_expression));
    }

    column_declaration->primary_key_specifier = primary_key_specifier;

    return true;
}

class ParserColumnDeclarationList : public IParserBase
{
public:
    explicit ParserColumnDeclarationList(bool require_type_ = true, bool allow_null_modifiers_ = true, bool check_keywords_after_name_ = false)
        : require_type(require_type_)
        , allow_null_modifiers(allow_null_modifiers_)
        , check_keywords_after_name(check_keywords_after_name_)
    {
    }

protected:
    const char * getName() const override { return "column declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    const bool require_type;
    const bool allow_null_modifiers;
    const bool check_keywords_after_name;
};


/** name BY expr TYPE typename(arg1, arg2, ...) GRANULARITY value */
class ParserIndexDeclaration : public IParserBase
{
public:
    ParserIndexDeclaration() = default;

protected:
    const char * getName() const override { return "index declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserConstraintDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "constraint declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserProjectionDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "projection declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserForeignKeyDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "foreign key declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserTablePropertyDeclaration : public IParserBase
{
protected:
    const char * getName() const override { return "table property (column, index, constraint) declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserIndexDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "index declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserConstraintDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "constraint declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserProjectionDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "projection declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserTablePropertiesDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "columns or indices declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/**
  * [ENGINE = name] [PARTITION BY expr] [ORDER BY expr] [PRIMARY KEY expr] [SAMPLE BY expr] [SETTINGS name = value, ...]
  */
class ParserStorage : public IParserBase
{
public:
    /// What kind of engine we're going to parse.
    enum EngineKind
    {
        TABLE_ENGINE,
        DATABASE_ENGINE,
    };

    ParserStorage(EngineKind engine_kind_) : engine_kind(engine_kind_) {}

protected:
    const char * getName() const override { return "storage definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    EngineKind engine_kind;
};

/** Query like this:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name [UUID 'uuid'] [ON CLUSTER cluster]
  * (
  *     name1 type1,
  *     name2 type2,
  *     ...
  *     INDEX name1 expr TYPE type1(args) GRANULARITY value,
  *     ...
  * ) ENGINE = engine
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name [UUID 'uuid'] [ON CLUSTER cluster] AS [db2.]name2 [ENGINE = engine]
  *
  * Or:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name [UUID 'uuid'] [ON CLUSTER cluster] AS ENGINE = engine SELECT ...
  *
  * Or (for engines that supports schema inference):
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name [UUID 'uuid'] [ON CLUSTER cluster] ENGINE = engine
  */
class ParserCreateTableQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TABLE or ATTACH TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// CREATE|ATTACH LIVE VIEW [IF NOT EXISTS] [db.]name [UUID 'uuid'] [TO [db.]name] AS SELECT ...
class ParserCreateLiveViewQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE LIVE VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// CREATE|ATTACH WINDOW VIEW [IF NOT EXISTS] [db.]name [TO [db.]name] [INNER ENGINE engine] [ENGINE engine] [WATERMARK strategy] [ALLOWED_LATENESS interval_function] [POPULATE] AS SELECT ...
class ParserCreateWindowViewQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE WINDOW VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserTableOverrideDeclaration : public IParserBase
{
public:
    const bool is_standalone;
    explicit ParserTableOverrideDeclaration(bool is_standalone_ = true) : is_standalone(is_standalone_) { }

protected:
    const char * getName() const override { return "table override declaration"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserTableOverridesDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "table overrides declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// CREATE|ATTACH DATABASE db [ENGINE = engine]
class ParserCreateDatabaseQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE DATABASE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// CREATE[OR REPLACE]|ATTACH [[MATERIALIZED] VIEW] | [VIEW]] [IF NOT EXISTS] [db.]name [UUID 'uuid'] [TO [db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
class ParserCreateViewQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// Parses complete dictionary create query. Uses ParserDictionary and
/// ParserDictionaryAttributeDeclaration. Produces ASTCreateQuery.
/// CREATE DICTIONARY [IF NOT EXISTS] [db.]name (attrs) PRIMARY KEY key SOURCE(s(params)) LAYOUT(l(params)) LIFETIME([min v1 max] v2) [RANGE(min v1 max v2)]
class ParserCreateDictionaryQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE DICTIONARY"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// CREATE NAMED COLLECTION name [ON CLUSTER cluster]
class ParserCreateNamedCollectionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE NAMED COLLECTION"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** Query like this:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name
  * (
  *     name1 type1,
  *     name2 type2,
  *     ...
  *     INDEX name1 expr TYPE type1(args) GRANULARITY value,
  *     ...
  *     PRIMARY KEY expr
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
  * CREATE[OR REPLACE]|ATTACH [[MATERIALIZED] VIEW] | [VIEW]] [IF NOT EXISTS] [db.]name [TO [db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
  */
class ParserCreateQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TABLE or ATTACH TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
