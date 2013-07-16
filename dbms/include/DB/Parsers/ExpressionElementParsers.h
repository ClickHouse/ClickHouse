#pragma once

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
  *  или если в скобках - подзапрос SELECT - то возвращает в node этот подзапрос;
  *  иначе возвращает функцию tuple от содержимого скобок.
  */
class ParserParenthesisExpression : public IParserBase
{
protected:
	String getName() { return "expression in parenthesis"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Подзапрос SELECT в скобках.
  */
class ParserSubquery : public IParserBase
{
protected:
	String getName() { return "SELECT subquery"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Идентификатор, например, x_yz123 или `something special`
  */
class ParserIdentifier : public IParserBase
{
protected:
	String getName() { return "identifier"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Идентификатор, возможно, содержащий точку, например, x_yz123 или `something special` или Hits.EventTime
  */
class ParserCompoundIdentifier : public IParserBase
{
protected:
	String getName() { return "compound identifier"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Функция, например, f(x, y + 1, g(z)).
  * Или агрегатная функция: sum(x + f(y)), corr(x, y). По синтаксису - такая же, как обычная функция.
  * Или параметрическая агрегатная функция: quantile(0.9)(x + y).
  *  Синтаксис - две пары круглых скобок вместо одной. Первая - для параметров, вторая - для аргументов.
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


/** Алиас - идентификатор, перед которым идёт AS. Например: AS x_yz123.
  */
class ParserAlias : public IParserBase
{
protected:
	String getName() { return "alias"; }
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


/** Элемент выражения, возможно, с алиасом, если уместно.
  */
class ParserWithOptionalAlias : public IParserBase
{
public:
	ParserWithOptionalAlias(ParserPtr elem_parser_) : elem_parser(elem_parser_) {}
protected:
	ParserPtr elem_parser;
	
	String getName() { return "element of expression with optional alias"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Элемент выражения ORDER BY - то же самое, что и элемент выражения, но после него ещё может быть указано ASC[ENDING] | DESC[ENDING]
 * 	и, возможно, COLLATE 'locale'.
  */
class ParserOrderByElement : public IParserBase
{
protected:
	String getName() { return "element of ORDER BY expression"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


}
