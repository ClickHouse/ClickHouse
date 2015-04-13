#pragma once

#include <DB/Parsers/IParserBase.h>


namespace DB
{


class ParserArray : public IParserBase
{
protected:
	const char * getName() const { return "array"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Если в скобках выражение из одного элемента - возвращает в node этот элемент;
  *  или если в скобках - подзапрос SELECT - то возвращает в node этот подзапрос;
  *  иначе возвращает функцию tuple от содержимого скобок.
  */
class ParserParenthesisExpression : public IParserBase
{
protected:
	const char * getName() const { return "parenthesized expression"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Подзапрос SELECT в скобках.
  */
class ParserSubquery : public IParserBase
{
protected:
	const char * getName() const { return "SELECT subquery"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Идентификатор, например, x_yz123 или `something special`
  */
class ParserIdentifier : public IParserBase
{
protected:
	const char * getName() const { return "identifier"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Идентификатор, возможно, содержащий точку, например, x_yz123 или `something special` или Hits.EventTime
  */
class ParserCompoundIdentifier : public IParserBase
{
protected:
	const char * getName() const { return "compound identifier"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Функция, например, f(x, y + 1, g(z)).
  * Или агрегатная функция: sum(x + f(y)), corr(x, y). По синтаксису - такая же, как обычная функция.
  * Или параметрическая агрегатная функция: quantile(0.9)(x + y).
  *  Синтаксис - две пары круглых скобок вместо одной. Первая - для параметров, вторая - для аргументов.
  */
class ParserFunction : public IParserBase
{
protected:
	const char * getName() const { return "function"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** NULL.
  */
class ParserNull : public IParserBase
{
protected:
	const char * getName() const { return "NULL"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Число.
  */
class ParserNumber : public IParserBase
{
protected:
	const char * getName() const { return "number"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Строка в одинарных кавычках.
  */
class ParserStringLiteral : public IParserBase
{
protected:
	const char * getName() const { return "string literal"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Литерал - одно из: NULL, UInt64, Int64, Float64, String.
  */
class ParserLiteral : public IParserBase
{
protected:
	const char * getName() const { return "literal"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Алиас - идентификатор, перед которым идёт AS. Например: AS x_yz123.
  */
class ParserAlias : public IParserBase
{
protected:
	const char * getName() const { return "alias"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Элемент выражения - одно из: выражение в круглых скобках, массив, литерал, функция, идентификатор, звёздочка.
  */
class ParserExpressionElement : public IParserBase
{
protected:
	const char * getName() const { return "element of expression"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Элемент выражения, возможно, с алиасом, если уместно.
  */
class ParserWithOptionalAlias : public IParserBase
{
public:
	ParserWithOptionalAlias(ParserPtr && elem_parser_) : elem_parser(std::move(elem_parser_)) {}
protected:
	ParserPtr elem_parser;

	const char * getName() const { return "element of expression with optional alias"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Элемент выражения ORDER BY - то же самое, что и элемент выражения, но после него ещё может быть указано ASC[ENDING] | DESC[ENDING]
 * 	и, возможно, COLLATE 'locale'.
  */
class ParserOrderByElement : public IParserBase
{
protected:
	const char * getName() const { return "element of ORDER BY expression"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


}
