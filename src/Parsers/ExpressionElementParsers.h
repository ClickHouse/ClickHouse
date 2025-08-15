#pragma once

#include <Core/Field.h>
#include <Core/MultiEnum.h>
#include <Parsers/IParserBase.h>


namespace DB
{


/** The SELECT subquery, in parentheses.
  */
class ParserSubquery : public IParserBase
{
protected:
    const char * getName() const override { return "SELECT subquery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** An identifier, for example, x_yz123 or `something special`
  * If allow_query_parameter_ = true, also parses substitutions in form {name:Identifier}
  */
class ParserIdentifier : public IParserBase
{
public:
    explicit ParserIdentifier(bool allow_query_parameter_ = false, Highlight highlight_type_ = Highlight::identifier)
        : allow_query_parameter(allow_query_parameter_), highlight_type(highlight_type_) {}
    Highlight highlight() const override { return highlight_type; }

protected:
    const char * getName() const override { return "identifier"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    bool allow_query_parameter;
    Highlight highlight_type;
};


/** An identifier for tables written as string literal, for example, 'mytable.avro'
  */
class ParserTableAsStringLiteralIdentifier : public IParserBase
{
public:
    explicit ParserTableAsStringLiteralIdentifier() = default;

protected:
    const char * getName() const override { return "string literal table identifier"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    Highlight highlight() const override { return Highlight::identifier; }
};


/** An identifier, possibly containing a dot, for example, x_yz123 or `something special` or Hits.EventTime,
 *  possibly with UUID clause like `db name`.`table name` UUID 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'.
 *  There is also special delimiters `.:` and `.^` for JSON type subcolumns. In case of special delimiter
 *  the next identifier part after it will include special delimiter and be back quoted always: json.a.b.:UInt32 -> ['json', 'a', 'b', ':`UInt32`'].
 *  It's needed to distinguish identifiers json.a.b.:UInt32 and json.a.b.`:UInt32`.
 *  There is also a special syntax sugar for reading JSON subcolumns of type Array(JSON): json.a.b[][].c -> json.a.b.:Array(Array(JSON)).c
  */
class ParserCompoundIdentifier : public IParserBase
{
public:
    enum class SpecialDelimiter : char
    {
        NONE = '\0',
        JSON_PATH_DYNAMIC_TYPE = ':',
        JSON_PATH_PREFIX = '^',
    };

    explicit ParserCompoundIdentifier(bool table_name_with_optional_uuid_ = false, bool allow_query_parameter_ = false, Highlight highlight_type_ = Highlight::identifier)
        : table_name_with_optional_uuid(table_name_with_optional_uuid_), allow_query_parameter(allow_query_parameter_), highlight_type(highlight_type_)
    {
    }

protected:
    const char * getName() const override { return "compound identifier"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    bool table_name_with_optional_uuid;
    bool allow_query_parameter;
    Highlight highlight_type;
};

/** *, t.*, db.table.*, COLUMNS('<regular expression>') APPLY(...) or EXCEPT(...) or REPLACE(...)
  */
class ParserColumnsTransformers : public IParserBase
{
public:
    enum class ColumnTransformer : UInt8
    {
        APPLY,
        EXCEPT,
        REPLACE,
    };
    using ColumnTransformers = MultiEnum<ColumnTransformer, UInt8>;
    static constexpr auto AllTransformers = ColumnTransformers{ColumnTransformer::APPLY, ColumnTransformer::EXCEPT, ColumnTransformer::REPLACE};

    explicit ParserColumnsTransformers(ColumnTransformers allowed_transformers_ = AllTransformers, bool is_strict_ = false)
        : allowed_transformers(allowed_transformers_)
        , is_strict(is_strict_)
    {}

protected:
    const char * getName() const override { return "COLUMNS transformers"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    ColumnTransformers allowed_transformers;
    bool is_strict;
};


/// Just *
class ParserAsterisk : public IParserBase
{
public:
    using ColumnTransformers = ParserColumnsTransformers::ColumnTransformers;
    explicit ParserAsterisk(ColumnTransformers allowed_transformers_ = ParserColumnsTransformers::AllTransformers)
        : allowed_transformers(allowed_transformers_)
    {}

protected:
    const char * getName() const override { return "asterisk"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    ColumnTransformers allowed_transformers;
};

/** Something like t.* or db.table.*
  */
class ParserQualifiedAsterisk : public IParserBase
{
protected:
    const char * getName() const override { return "qualified asterisk"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** COLUMNS(columns_names) or COLUMNS('<regular expression>')
  */
class ParserColumnsMatcher : public IParserBase
{
public:
    using ColumnTransformers = ParserColumnsTransformers::ColumnTransformers;
    explicit ParserColumnsMatcher(ColumnTransformers allowed_transformers_ = ParserColumnsTransformers::AllTransformers)
        : allowed_transformers(allowed_transformers_)
    {}

protected:
    const char * getName() const override { return "COLUMNS matcher"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    ColumnTransformers allowed_transformers;
};

/** Qualified columns matcher identifier.COLUMNS(columns_names) or identifier.COLUMNS('<regular expression>')
  */
class ParserQualifiedColumnsMatcher : public IParserBase
{
public:
    using ColumnTransformers = ParserColumnsTransformers::ColumnTransformers;
    explicit ParserQualifiedColumnsMatcher(ColumnTransformers allowed_transformers_ = ParserColumnsTransformers::AllTransformers)
        : allowed_transformers(allowed_transformers_)
    {}

protected:
    const char * getName() const override { return "qualified COLUMNS matcher"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    ColumnTransformers allowed_transformers;
};

// Allows to make queries like SELECT SUM(<expr>) FILTER(WHERE <cond>) FROM ...
class ParserFilterClause : public IParserBase
{
    const char * getName() const override { return "filter"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

// Window reference (the thing that goes after OVER) for window function.
// Can be either window name or window definition.
class ParserWindowReference : public IParserBase
{
    const char * getName() const override { return "window reference"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserWindowDefinition : public IParserBase
{
    const char * getName() const override { return "window definition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

// The WINDOW clause of a SELECT query that defines a list of named windows.
// Returns an ASTExpressionList of ASTWindowListElement's.
class ParserWindowList : public IParserBase
{
    const char * getName() const override { return "WINDOW clause"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserCodecDeclarationList : public IParserBase
{
protected:
    const char * getName() const override { return "codec declaration list"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** Parse compression codec
  * CODEC(ZSTD(2))
  */
class ParserCodec : public IParserBase
{
protected:
    const char * getName() const override { return "codec"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// STATISTICS(tdigest(200))
class ParserStatisticsType : public IParserBase
{
protected:
    const char * getName() const override { return "statistics"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** Parse collation
  * COLLATE utf8_unicode_ci NOT NULL
  */
class ParserCollation : public IParserBase
{
protected:
    const char * getName() const override { return "collation"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
private:
    static const char * valid_collations[];
};

/// Fast path of cast operator "::".
/// It tries to read literal as text.
/// If it fails, later operator will be transformed to function CAST.
/// Examples: "0.1::Decimal(38, 38)", "[1, 2]::Array(UInt8)"
class ParserCastOperator : public IParserBase
{
protected:
    const char * getName() const override { return "CAST operator"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** NULL literal.
  */
class ParserNull : public IParserBase
{
protected:
    const char * getName() const override { return "NULL"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** Bool literal.
  */
class ParserBool : public IParserBase
{
protected:
    const char * getName() const override { return "Bool"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** Numeric literal.
  */
class ParserNumber : public IParserBase
{
protected:
    const char * getName() const override { return "number"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    Highlight highlight() const override { return Highlight::number; }
};

/** Unsigned integer, used in right hand side of tuple access operator (x.1).
  */
class ParserUnsignedInteger : public IParserBase
{
protected:
    const char * getName() const override { return "unsigned integer"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** String in single quotes.
  * String in heredoc $here$txt$here$ equivalent to 'txt'.
  */
class ParserStringLiteral : public IParserBase
{
protected:
    const char * getName() const override { return "string literal"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    Highlight highlight() const override { return Highlight::string; }
};


/** An array or tuple of literals.
  * Arrays can also be parsed as an application of [] operator and tuples as an application of 'tuple' function.
  * But parsing the whole array/tuple as a whole constant seriously speeds up the analysis of expressions in the case of very large collection.
  * We try to parse the array or tuple as a collection of literals first (fast path),
  *  and if it did not work out (when the collection consists of complex expressions) -
  *  parse as an application of [] operator or 'tuple' function (slow path).
  */
template <typename Collection>
class ParserCollectionOfLiterals : public IParserBase
{
public:
    ParserCollectionOfLiterals(TokenType opening_bracket_, TokenType closing_bracket_)
        : opening_bracket(opening_bracket_), closing_bracket(closing_bracket_) {}
protected:
    const char * getName() const override { return "collection of literals"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
private:
    TokenType opening_bracket;
    TokenType closing_bracket;
};

/// A tuple of literals with same type.
class ParserTupleOfLiterals : public IParserBase
{
public:
    ParserCollectionOfLiterals<Tuple> tuple_parser{TokenType::OpeningRoundBracket, TokenType::ClosingRoundBracket};
protected:
    const char * getName() const override { return "tuple"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        return tuple_parser.parse(pos, node, expected);
    }
};

class ParserArrayOfLiterals : public IParserBase
{
public:
    ParserCollectionOfLiterals<Array> array_parser{TokenType::OpeningSquareBracket, TokenType::ClosingSquareBracket};
protected:
    const char * getName() const override { return "array"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        return array_parser.parse(pos, node, expected);
    }
};

/** Parses all collections of literals and their various combinations
  * Used in parsing parameters for SET query
  */
class ParserAllCollectionsOfLiterals : public IParserBase
{
public:
    explicit ParserAllCollectionsOfLiterals(bool allow_map_ = true) : allow_map(allow_map_) {}

protected:
    const char * getName() const override { return "combination of maps, arrays, tuples"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool allow_map;
};


/** The literal is one of: NULL, UInt64, Int64, Float64, String.
  */
class ParserLiteral : public IParserBase
{
protected:
    const char * getName() const override { return "literal"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** The alias is the identifier before which `AS` comes. For example: AS x_yz123.
  */
class ParserAlias : public IParserBase
{
public:
    explicit ParserAlias(bool allow_alias_without_as_keyword_) : allow_alias_without_as_keyword(allow_alias_without_as_keyword_) { }

private:
    static const char * restricted_keywords[];

    bool allow_alias_without_as_keyword;

    const char * getName() const override { return "alias"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** Prepared statements.
  * Parse query with parameter expression {name:type}.
  */
class ParserIdentifierOrSubstitution : public IParserBase
{
protected:
    const char * getName() const override { return "identifier or substitution"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** Prepared statements.
  * Parse query with parameter expression {name:type}.
  */
class ParserSubstitution : public IParserBase
{
protected:
    const char * getName() const override { return "substitution"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    Highlight highlight() const override { return Highlight::substitution; }
};


/** MySQL-style global variable: @@var
  */
class ParserMySQLGlobalVariable : public IParserBase
{
protected:
    const char * getName() const override { return "MySQL-style global variable"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** An expression element, possibly with an alias, if appropriate.
  */
class ParserWithOptionalAlias : public IParserBase
{
public:
    ParserWithOptionalAlias(ParserPtr && elem_parser_, bool allow_alias_without_as_keyword_)
    : elem_parser(std::move(elem_parser_)), allow_alias_without_as_keyword(allow_alias_without_as_keyword_) {}
protected:
    ParserPtr elem_parser;
    bool allow_alias_without_as_keyword;

    const char * getName() const override { return "element of expression with optional alias"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** Element of ORDER BY expression - same as expression element, but in addition, ASC[ENDING] | DESC[ENDING] could be specified
  *  and optionally, NULLS LAST|FIRST
  *  and optionally, COLLATE 'locale'.
  *  and optionally, WITH FILL [FROM x] [TO y] [STEP z]
  */
class ParserOrderByElement : public IParserBase
{
protected:
    const char * getName() const override { return "element of ORDER BY expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** Element of INTERPOLATE expression
  */
class ParserInterpolateElement : public IParserBase
{
protected:
    const char * getName() const override { return "element of INTERPOLATE expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** Parser for function with arguments like KEY VALUE (space separated)
  * no commas allowed, just space-separated pairs.
  */
class ParserFunctionWithKeyValueArguments : public IParserBase
{
public:
    explicit ParserFunctionWithKeyValueArguments(bool brackets_can_be_omitted_ = false) : brackets_can_be_omitted(brackets_can_be_omitted_)
    {
    }

protected:

    const char * getName() const override { return "function with key-value arguments"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    /// brackets for function arguments can be omitted
    bool brackets_can_be_omitted;
};

/** Table engine, possibly with parameters. See examples from ParserIdentifierWithParameters
  * Parse result is ASTFunction, with or without arguments.
  */
class ParserIdentifierWithOptionalParameters : public IParserBase
{
protected:
    const char * getName() const  override{ return "identifier with optional parameters"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** Element of TTL expression - same as expression element, but in addition,
 *   TO DISK 'xxx' | TO VOLUME 'xxx' | DELETE could be specified
  */
class ParserTTLElement : public IParserBase
{
protected:
    const char * getName() const override { return "element of TTL expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// Part of the UPDATE command or TTL with GROUP BY of the form: col_name = expr
class ParserAssignment : public IParserBase
{
protected:
    const char * getName() const  override{ return "column assignment"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

ASTPtr createFunctionCast(const ASTPtr & expr_ast, const ASTPtr & type_ast);

}
