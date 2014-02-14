#pragma once

#include <list>

#include <boost/assign/list_of.hpp>

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/CommonParsers.h>


namespace DB
{

/** Оператор и соответствующая ему функция. Например, "+" -> "plus"
  * Не std::map, так как порядок парсинга операторов задаётся явно и может отличаться от алфавитного.
  */
typedef std::list<std::pair<String, String> > Operators_t;


/** Список элементов, разделённых чем-либо. */
class ParserList : public IParserBase
{
public:
	ParserList(ParserPtr elem_parser_, ParserPtr separator_parser_, bool allow_empty_ = true)
		: elem_parser(elem_parser_), separator_parser(separator_parser_), allow_empty(allow_empty_)
	{
	}
protected:
	String getName() { return "list of elements (" + elem_parser->getName() + ")"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
private:
	ParserPtr elem_parser;
	ParserPtr separator_parser;
	bool allow_empty;
};


/** Выражение с инфиксным бинарным лево-ассоциативным оператором.
  * Например, a + b - c + d.
  */
class ParserLeftAssociativeBinaryOperatorList : public IParserBase
{
private:
	Operators_t operators;
	ParserPtr elem_parser;

public:
	/** operators_ - допустимые операторы и соответствующие им функции
	  */
	ParserLeftAssociativeBinaryOperatorList(const Operators_t & operators_, ParserPtr elem_parser_)
		: operators(operators_), elem_parser(elem_parser_)
	{
	}
	
protected:
	String getName() { return "list, delimited by binary operators"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Выражение с инфиксным оператором произвольной арности.
  * Например, a AND b AND c AND d.
  */
class ParserVariableArityOperatorList : public IParserBase
{
private:
	ParserString infix_parser;
	String function_name;
	ParserPtr elem_parser;

public:
	ParserVariableArityOperatorList(const String & infix_, const String & function_, ParserPtr elem_parser_)
		: infix_parser(infix_, true, true), function_name(function_), elem_parser(elem_parser_)
	{
	}

protected:
	String getName() { return "list, delimited by operator of variable arity"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Выражение с префиксным унарным оператором.
  * Например, NOT x.
  */
class ParserPrefixUnaryOperatorExpression : public IParserBase
{
private:
	Operators_t operators;
	ParserPtr elem_parser;

public:
	/** operators_ - допустимые операторы и соответствующие им функции
	  */
	ParserPrefixUnaryOperatorExpression(const Operators_t & operators_, ParserPtr elem_parser_)
		: operators(operators_), elem_parser(elem_parser_)
	{
	}
	
protected:
	String getName() { return "expression with prefix unary operator"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


class ParserAccessExpression : public IParserBase
{
private:
	ParserPtr elem_parser;
	ParserLeftAssociativeBinaryOperatorList operator_parser;
public:
	ParserAccessExpression();
	
protected:
	String getName() { return "access expression"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		return operator_parser.parse(pos, end, node, expected);
	}
};


class ParserUnaryMinusExpression : public IParserBase
{
private:
	ParserPtr elem_parser;
	ParserPrefixUnaryOperatorExpression operator_parser;
public:
	ParserUnaryMinusExpression()
		: elem_parser(new ParserAccessExpression),
		operator_parser(boost::assign::map_list_of("-", "negate"), elem_parser)
	{
	}
	
protected:
	String getName() { return "unary minus expression"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


class ParserMultiplicativeExpression : public IParserBase
{
private:
	ParserPtr elem_parser;
	ParserLeftAssociativeBinaryOperatorList operator_parser;
public:
	ParserMultiplicativeExpression()
		: elem_parser(new ParserUnaryMinusExpression),
		operator_parser(boost::assign::map_list_of
				("*", 	"multiply")
				("/", 	"divide")
				("%", 	"modulo"),
			elem_parser)
	{
	}
	
protected:
	String getName() { return "multiplicative expression"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		return operator_parser.parse(pos, end, node, expected);
	}
};


class ParserAdditiveExpression : public IParserBase
{
private:
	ParserPtr elem_parser;
	ParserLeftAssociativeBinaryOperatorList operator_parser;
public:
	ParserAdditiveExpression()
		: elem_parser(new ParserMultiplicativeExpression),
		operator_parser(boost::assign::map_list_of
				("+", 	"plus")
				("-", 	"minus"),
			elem_parser)
	{
	}
	
protected:
	String getName() { return "additive expression"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		return operator_parser.parse(pos, end, node, expected);
	}
};


class ParserComparisonExpression : public IParserBase
{
private:
	ParserPtr elem_parser;
	ParserLeftAssociativeBinaryOperatorList operator_parser;
public:
	ParserComparisonExpression()
		: elem_parser(new ParserAdditiveExpression),
		operator_parser(boost::assign::map_list_of
				("==", 			"equals")
				("!=", 			"notEquals")
				("<>", 			"notEquals")
				("<=", 			"lessOrEquals")
				(">=", 			"greaterOrEquals")
				("<", 			"less")
				(">", 			"greater")
				("=", 			"equals")
				("LIKE", 		"like")
				("NOT LIKE",	"notLike")
				("IN",			"in")
				("NOT IN",		"notIn"),
			elem_parser)
	{
	}
	
protected:
	String getName() { return "comparison expression"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		return operator_parser.parse(pos, end, node, expected);
	}
};


class ParserLogicalNotExpression : public IParserBase
{
private:
	ParserPtr elem_parser;
	ParserPrefixUnaryOperatorExpression operator_parser;
public:
	ParserLogicalNotExpression()
		: elem_parser(new ParserComparisonExpression),
		operator_parser(boost::assign::map_list_of("NOT", "not"), elem_parser)
	{
	}
	
protected:
	String getName() { return "logical-NOT expression"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		return operator_parser.parse(pos, end, node, expected);
	}
};


class ParserLogicalAndExpression : public IParserBase
{
private:
	ParserPtr elem_parser;
	ParserVariableArityOperatorList operator_parser;
public:
	ParserLogicalAndExpression()
		: elem_parser(new ParserLogicalNotExpression),
		operator_parser("AND", "and", elem_parser)
	{
	}
	
protected:
	String getName() { return "logical-AND expression"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		return operator_parser.parse(pos, end, node, expected);
	}
};


class ParserLogicalOrExpression : public IParserBase
{
private:
	ParserPtr elem_parser;
	ParserVariableArityOperatorList operator_parser;
public:
	ParserLogicalOrExpression()
		: elem_parser(new ParserLogicalAndExpression),
		operator_parser("OR", "or", elem_parser)
	{
	}
	
protected:
	String getName() { return "logical-OR expression"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		return operator_parser.parse(pos, end, node, expected);
	}
};


/** Выражение с тернарным оператором.
  * Например, a = 1 ? b + 1 : c * 2.
  */
class ParserTernaryOperatorExpression : public IParserBase
{
private:
	ParserPtr elem_parser;

public:
	ParserTernaryOperatorExpression()
		: elem_parser(new ParserLogicalOrExpression)
	{
	}

protected:
	String getName() { return "expression with ternary operator"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


class ParserLambdaExpression : public IParserBase
{
private:
	ParserPtr elem_parser;
	
public:
	ParserLambdaExpression()
		: elem_parser(new ParserTernaryOperatorExpression)
	{
	}
	
protected:
	String getName() { return "lambda expression"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


class ParserExpressionWithOptionalAlias : public IParserBase
{
public:
	ParserExpressionWithOptionalAlias();
protected:
	ParserPtr impl;

	String getName() { return "expression with optional alias"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		return impl->parse(pos, end, node, expected);
	}
};


/** Список выражений, разделённых запятыми, возможно пустой. */
class ParserExpressionList : public IParserBase
{
protected:
	String getName() { return "list of expressions"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


class ParserNotEmptyExpressionList : public IParserBase
{
private:
	ParserExpressionList nested_parser;
protected:
	String getName() { return "not empty list of expressions"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


class ParserOrderByExpressionList : public IParserBase
{
protected:
	String getName() { return "order by expression"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


}
