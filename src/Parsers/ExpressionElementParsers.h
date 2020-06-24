#pragma once

#include <Core/Field.h>
#include <Parsers/IParserBase.h>


namespace DB
{


class ParserArray : public IParserBase
{
protected:
    const char * getName() const override { return "array"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** If in parenthesis an expression from one element - returns this element in `node`;
  *  or if there is a SELECT subquery in parenthesis, then this subquery returned in `node`;
  *  otherwise returns `tuple` function from the contents of brackets.
  */
class ParserParenthesisExpression : public IParserBase
{
protected:
    const char * getName() const override { return "parenthesized expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** The SELECT subquery is in parenthesis.
  */
class ParserSubquery : public IParserBase
{
protected:
    const char * getName() const override { return "SELECT subquery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** An identifier, for example, x_yz123 or `something special`
  */
class ParserIdentifier : public IParserBase
{
protected:
    const char * getName() const override { return "identifier"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** An identifier, possibly containing a dot, for example, x_yz123 or `something special` or Hits.EventTime,
 *  possibly with UUID clause like `db name`.`table name` UUID 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
  */
class ParserCompoundIdentifier : public IParserBase
{
public:
    ParserCompoundIdentifier(bool table_name_with_optional_uuid_ = false)
    : table_name_with_optional_uuid(table_name_with_optional_uuid_) {}
protected:
    const char * getName() const override { return "compound identifier"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    bool table_name_with_optional_uuid;
};

/// Just *
class ParserAsterisk : public IParserBase
{
protected:
    const char * getName() const override { return "asterisk"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** Something like t.* or db.table.*
  */
class ParserQualifiedAsterisk : public IParserBase
{
protected:
    const char * getName() const override { return "qualified asterisk"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** COLUMNS('<regular expression>')
  */
class ParserColumnsMatcher : public IParserBase
{
protected:
    const char * getName() const override { return "COLUMNS matcher"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** A function, for example, f(x, y + 1, g(z)).
  * Or an aggregate function: sum(x + f(y)), corr(x, y). The syntax is the same as the usual function.
  * Or a parametric aggregate function: quantile(0.9)(x + y).
  *  Syntax - two pairs of parentheses instead of one. The first is for parameters, the second for arguments.
  * For functions, the DISTINCT modifier can be specified, for example, count(DISTINCT x, y).
  */
class ParserFunction : public IParserBase
{
public:
    ParserFunction(bool allow_function_parameters_ = true) : allow_function_parameters(allow_function_parameters_) {}
protected:
    const char * getName() const override { return "function"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    bool allow_function_parameters;
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

class ParserCastExpression : public IParserBase
{
protected:
    const char * getName() const override { return "CAST expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserSubstringExpression : public IParserBase
{
protected:
    const char * getName() const override { return "SUBSTRING expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserTrimExpression : public IParserBase
{
protected:
    const char * getName() const override { return "TRIM expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserLeftExpression : public IParserBase
{
protected:
    const char * getName() const override { return "LEFT expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserRightExpression : public IParserBase
{
protected:
    const char * getName() const override { return "RIGHT expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserExtractExpression : public IParserBase
{
protected:
    const char * getName() const override { return "EXTRACT expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserDateAddExpression : public IParserBase
{
protected:
    const char * getName() const override { return "DATE_ADD expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserDateDiffExpression : public IParserBase
{
protected:
    const char * getName() const override { return "DATE_DIFF expression"; }
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


/** Numeric literal.
  */
class ParserNumber : public IParserBase
{
protected:
    const char * getName() const override { return "number"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
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
  */
class ParserStringLiteral : public IParserBase
{
protected:
    const char * getName() const override { return "string literal"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
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
    ParserAlias(bool allow_alias_without_as_keyword_)
        : allow_alias_without_as_keyword(allow_alias_without_as_keyword_) {}
private:
    static const char * restricted_keywords[];

    bool allow_alias_without_as_keyword;

    const char * getName() const override { return "alias"; }
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
};


/** MySQL-style global variable: @@var
  */
class ParserMySQLGlobalVariable : public IParserBase
{
protected:
    const char * getName() const override { return "MySQL-style global variable"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** The expression element is one of: an expression in parentheses, an array, a literal, a function, an identifier, an asterisk.
  */
class ParserExpressionElement : public IParserBase
{
protected:
    const char * getName() const override { return "element of expression"; }
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

/** Parser for function with arguments like KEY VALUE (space separated)
  * no commas alowed, just space-separated pairs.
  */
class ParserFunctionWithKeyValueArguments : public IParserBase
{
public:
    ParserFunctionWithKeyValueArguments(bool brackets_can_be_omitted_ = false)
        : brackets_can_be_omitted(brackets_can_be_omitted_) {}
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

}
