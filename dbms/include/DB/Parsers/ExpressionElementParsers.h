#ifndef DBMS_PARSERS_EXPRESSIONELEMENTPARSERS_H
#define DBMS_PARSERS_EXPRESSIONELEMENTPARSERS_H

#include <DB/Parsers/IParserBase.h>


namespace DB
{


class ParserArray : public IParserBase
{
protected:
	String getName() { return "array"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Если в скобках выражение из одного элемента - возвращает в node этот элемент;
  *  иначе возвращает функцию tuple от содержимого скобок.
  */
class ParserParenthesisExpression : public IParserBase
{
protected:
	String getName() { return "expression in parenthesis"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Идентификатор, например, x_yz123
  */
class ParserIdentifier : public IParserBase
{
protected:
	String getName() { return "identifier"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Функция, например, f(x, y + 1, g(z))
  */
class ParserFunction : public IParserBase
{
protected:
	String getName() { return "function"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** NULL.
  */
class ParserNull : public IParserBase
{
protected:
	String getName() { return "NULL"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Число.
  */
class ParserNumber : public IParserBase
{
protected:
	String getName() { return "number"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Строка в одинарных кавычках.
  */
class ParserStringLiteral : public IParserBase
{
protected:
	String getName() { return "string literal"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Литерал - одно из: NULL, UInt64, Int64, Float64, String.
  */
class ParserLiteral : public IParserBase
{
protected:
	String getName() { return "literal"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Элемент выражения - одно из: выражение в круглых скобках, массив, литерал, функция, идентификатор, звёздочка.
  */
class ParserExpressionElement : public IParserBase
{
protected:
	String getName() { return "element of expression"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


}

#endif
