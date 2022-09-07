#pragma once

#include <list>

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/SelectUnionMode.h>
#include <Common/IntervalKind.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc99-extensions"
#endif

namespace DB
{

/** Consequent pairs of rows: the operator and the corresponding function. For example, "+" -> "plus".
  * The parsing order of the operators is significant.
  */
using Operators_t = const char **;


/** List of elements separated by something. */
class ParserList : public IParserBase
{
public:
    ParserList(ParserPtr && elem_parser_, ParserPtr && separator_parser_, bool allow_empty_ = true, char result_separator_ = ',')
        : elem_parser(std::move(elem_parser_))
        , separator_parser(std::move(separator_parser_))
        , allow_empty(allow_empty_)
        , result_separator(result_separator_)
    {
    }

    template <typename F>
    static bool parseUtil(Pos & pos, Expected & expected, const F & parse_element, IParser & separator_parser_, bool allow_empty_ = true)
    {
        Pos begin = pos;
        if (!parse_element())
        {
            pos = begin;
            return allow_empty_;
        }

        while (true)
        {
            begin = pos;
            if (!separator_parser_.ignore(pos, expected) || !parse_element())
            {
                pos = begin;
                return true;
            }
        }

        return false;
    }

    template <typename F>
    static bool parseUtil(Pos & pos, Expected & expected, const F & parse_element, TokenType separator, bool allow_empty_ = true)
    {
        ParserToken sep_parser{separator};
        return parseUtil(pos, expected, parse_element, sep_parser, allow_empty_);
    }

    template <typename F>
    static bool parseUtil(Pos & pos, Expected & expected, const F & parse_element, bool allow_empty_ = true)
    {
        return parseUtil(pos, expected, parse_element, TokenType::Comma, allow_empty_);
    }

protected:
    const char * getName() const override { return "list of elements"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
private:
    ParserPtr elem_parser;
    ParserPtr separator_parser;
    bool allow_empty;
    char result_separator;
};

class ParserUnionList : public IParserBase
{
public:
    template <typename ElemFunc, typename SepFunc>
    static bool parseUtil(Pos & pos, const ElemFunc & parse_element, const SepFunc & parse_separator)
    {
        Pos begin = pos;
        if (!parse_element())
        {
            pos = begin;
            return false;
        }

        while (true)
        {
            begin = pos;
            if (!parse_separator() || !parse_element())
            {
                pos = begin;
                return true;
            }
        }

        return false;
    }

    auto getUnionModes() const { return union_modes; }

protected:
    const char * getName() const override { return "list of union elements"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
private:
    SelectUnionModes union_modes;
};

/** An expression with an infix binary left-associative operator.
  * For example, a + b - c + d.
  */
class ParserLeftAssociativeBinaryOperatorList : public IParserBase
{
private:
    Operators_t operators;
    Operators_t overlapping_operators_to_skip = { (const char *[]){ nullptr } };
    ParserPtr first_elem_parser;
    ParserPtr remaining_elem_parser;
    /// =, !=, <, > ALL (subquery) / ANY (subquery)
    bool comparison_expression = false;

public:
    /** `operators_` - allowed operators and their corresponding functions
      */
    ParserLeftAssociativeBinaryOperatorList(Operators_t operators_, ParserPtr && first_elem_parser_)
        : operators(operators_), first_elem_parser(std::move(first_elem_parser_))
    {
    }

    ParserLeftAssociativeBinaryOperatorList(Operators_t operators_,
            Operators_t overlapping_operators_to_skip_, ParserPtr && first_elem_parser_, bool comparison_expression_ = false)
        : operators(operators_), overlapping_operators_to_skip(overlapping_operators_to_skip_),
          first_elem_parser(std::move(first_elem_parser_)), comparison_expression(comparison_expression_)
    {
    }

    ParserLeftAssociativeBinaryOperatorList(Operators_t operators_, ParserPtr && first_elem_parser_,
        ParserPtr && remaining_elem_parser_)
        : operators(operators_), first_elem_parser(std::move(first_elem_parser_)),
          remaining_elem_parser(std::move(remaining_elem_parser_))
    {
    }

protected:
    const char * getName() const override { return "list, delimited by binary operators"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** Expression with an infix operator of arbitrary arity.
  * For example, a AND b AND c AND d.
  */
class ParserVariableArityOperatorList : public IParserBase
{
private:
    const char * infix;
    const char * function_name;
    ParserPtr elem_parser;

public:
    ParserVariableArityOperatorList(const char * infix_, const char * function_, ParserPtr && elem_parser_)
        : infix(infix_), function_name(function_), elem_parser(std::move(elem_parser_))
    {
    }

protected:
    const char * getName() const override { return "list, delimited by operator of variable arity"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** An expression with a prefix unary operator.
  * Example, NOT x.
  */
class ParserPrefixUnaryOperatorExpression : public IParserBase
{
private:
    Operators_t operators;
    ParserPtr elem_parser;

public:
    /** `operators_` - allowed operators and their corresponding functions
      */
    ParserPrefixUnaryOperatorExpression(Operators_t operators_, ParserPtr && elem_parser_)
        : operators(operators_), elem_parser(std::move(elem_parser_))
    {
    }

protected:
    const char * getName() const override { return "expression with prefix unary operator"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// CAST operator "::". This parser is used if left argument
/// of operator cannot be read as simple literal from text of query.
/// Example: "[1, 1 + 1, 1 + 2]::Array(UInt8)"
class ParserCastExpression : public IParserBase
{
private:
    ParserPtr elem_parser;

public:
    explicit ParserCastExpression(ParserPtr && elem_parser_)
        : elem_parser(std::move(elem_parser_))
    {
    }

protected:
    const char * getName() const override { return "CAST expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// Optional conversion to INTERVAL data type. Example: "INTERVAL x SECOND" parsed as "toIntervalSecond(x)".
class ParserIntervalOperatorExpression : public IParserBase
{
protected:
    const char * getName() const  override { return "INTERVAL operator expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserExpression : public IParserBase
{
protected:
    const char * getName() const override { return "lambda expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

// It's used to parse expressions in table function.
class ParserTableFunctionExpression : public IParserBase
{
private:
    ParserExpression elem_parser;

protected:
    const char * getName() const override { return "table function expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserExpressionWithOptionalAlias : public IParserBase
{
public:
    explicit ParserExpressionWithOptionalAlias(bool allow_alias_without_as_keyword, bool is_table_function = false);
protected:
    ParserPtr impl;

    const char * getName() const override { return "expression with optional alias"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
    {
        return impl->parse(pos, node, expected);
    }
};


/** A comma-separated list of expressions, probably empty. */
class ParserExpressionList : public IParserBase
{
public:
    explicit ParserExpressionList(bool allow_alias_without_as_keyword_, bool is_table_function_ = false)
        : allow_alias_without_as_keyword(allow_alias_without_as_keyword_), is_table_function(is_table_function_) {}

protected:
    bool allow_alias_without_as_keyword;
    bool is_table_function; // This expression list is used by a table function

    const char * getName() const override { return "list of expressions"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserNotEmptyExpressionList : public IParserBase
{
public:
    explicit ParserNotEmptyExpressionList(bool allow_alias_without_as_keyword)
        : nested_parser(allow_alias_without_as_keyword) {}
private:
    ParserExpressionList nested_parser;
protected:
    const char * getName() const override { return "not empty list of expressions"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserOrderByExpressionList : public IParserBase
{
protected:
    const char * getName() const override { return "order by expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserGroupingSetsExpressionList : public IParserBase
{
protected:
    const char * getName() const override { return "grouping sets expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserGroupingSetsExpressionListElements : public IParserBase
{
protected:
    const char * getName() const override { return "grouping sets expression elements"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserInterpolateExpressionList : public IParserBase
{
protected:
    const char * getName() const override { return "interpolate expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/// Parser for key-value pair, where value can be list of pairs.
class ParserKeyValuePair : public IParserBase
{
protected:
    const char * getName() const override { return "key-value pair"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/// Parser for list of key-value pairs.
class ParserKeyValuePairsList : public IParserBase
{
protected:
    const char * getName() const override { return "list of pairs"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserTTLExpressionList : public IParserBase
{
protected:
    const char * getName() const override { return "ttl expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

#ifdef __clang__
#pragma clang diagnostic pop
#endif
