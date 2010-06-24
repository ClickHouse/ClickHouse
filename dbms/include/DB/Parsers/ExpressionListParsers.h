#ifndef DBMS_PARSERS_EXPRESSIONLISTPARSERS_H
#define DBMS_PARSERS_EXPRESSIONLISTPARSERS_H

#include <map>

#include <boost/assign/list_of.hpp>

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/CommonParsers.h>


namespace DB
{

/** Оператор и соответствующая ему функция. Например, "+" -> "plus" */
typedef std::map<String, String> Operators_t;


/** Выражение с инфиксным бинарным лево-ассоциативным оператором.
  * Например, a + b - c + d.
  * NOTE: если оператор словесный (например, OR), то после него не требуется границы слова.
  *  то есть, можно написать a = b ORx = y.
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


/** Выражение с префиксным унарным оператором.
  * Например, NOT x.
  * NOTE: если оператор словесный (например, NOT), то после него не требуется границы слова.
  *  то есть, можно написать NOTx.
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
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		return operator_parser.parse(pos, end, node, expected);
	}
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
				("==", 	"equals")
				("!=", 	"notEquals")
				("<>", 	"notEquals")
				("<=", 	"lessOrEquals")
				(">=", 	"greaterOrEquals")
				("<", 	"less")
				(">", 	"greater")
				("=", 	"equals"),
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
	ParserLeftAssociativeBinaryOperatorList operator_parser;
public:
	ParserLogicalAndExpression()
		: elem_parser(new ParserLogicalNotExpression),
		operator_parser(boost::assign::map_list_of("AND", "and"), elem_parser)
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
	ParserLeftAssociativeBinaryOperatorList operator_parser;
public:
	ParserLogicalOrExpression()
		: elem_parser(new ParserLogicalAndExpression),
		operator_parser(boost::assign::map_list_of("OR", "or"), elem_parser)
	{
	}
	
protected:
	String getName() { return "logical-OR expression"; }
	
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		return operator_parser.parse(pos, end, node, expected);
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


}


#endif
