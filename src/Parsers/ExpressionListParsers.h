#pragma once

#include <list>

#include <Parsers/IParserBase.h>
#include <Parsers/CommonParsers.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/SelectUnionMode.h>
#include <Common/IntervalKind.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc99-extensions"

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


class ParserArray : public IParserBase
{
protected:
    const char * getName() const override { return "array"; }
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
    explicit ParserFunction(bool allow_function_parameters_ = true, bool is_table_function_ = false)
        : allow_function_parameters(allow_function_parameters_), is_table_function(is_table_function_)
    {
    }

protected:
    const char * getName() const override { return "function"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    bool allow_function_parameters;
    bool is_table_function;
};


/** Similar to ParserFunction (and yields ASTFunction), but can also parse identifiers without braces.
  */
class ParserExpressionWithOptionalArguments : public IParserBase
{
protected:
    const char * getName() const override { return "expression with optional parameters"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


/** An expression with an infix binary left-associative operator.
  * For example, a + b - c + d.
  */
class ParserLeftAssociativeBinaryOperatorList : public IParserBase
{
private:
    Operators_t operators;
    ParserPtr elem_parser;

public:
    /** `operators_` - allowed operators and their corresponding functions
      */
    ParserLeftAssociativeBinaryOperatorList(Operators_t operators_, ParserPtr && elem_parser_)
        : operators(operators_), elem_parser(std::move(elem_parser_))
    {
    }

protected:
    const char * getName() const override { return "list, delimited by binary operators"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserExpression : public IParserBase
{
public:
    explicit ParserExpression(bool allow_trailing_commas_ = false) : allow_trailing_commas(allow_trailing_commas_) {}

protected:
    const char * getName() const override { return "lambda expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    bool allow_trailing_commas;
};


// It's used to parse expressions in table function.
class ParserTableFunctionExpression : public IParserBase
{
protected:
    const char * getName() const override { return "table function expression"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserExpressionWithOptionalAlias : public IParserBase
{
public:
    explicit ParserExpressionWithOptionalAlias(bool allow_alias_without_as_keyword_, bool is_table_function_ = false, bool allow_trailing_commas_ = false);
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
    explicit ParserExpressionList(bool allow_alias_without_as_keyword_, bool is_table_function_ = false, bool allow_trailing_commas_ = false)
        : allow_alias_without_as_keyword(allow_alias_without_as_keyword_)
        , is_table_function(is_table_function_)
        , allow_trailing_commas(allow_trailing_commas_) {}

protected:
    bool allow_alias_without_as_keyword;
    bool is_table_function; // This expression list is used by a table function
    bool allow_trailing_commas;

    const char * getName() const override { return "list of expressions"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserNotEmptyExpressionList : public IParserBase
{
public:
    explicit ParserNotEmptyExpressionList(bool allow_alias_without_as_keyword_, bool allow_trailing_commas_ = false)
        : nested_parser(allow_alias_without_as_keyword_, false, allow_trailing_commas_) {}
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

#pragma clang diagnostic pop
