#pragma once

#include <Parsers/IParserBase.h>


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


/// Just *
class ParserAsterisk : public IParserBase
{
protected:
    const char * getName() const { return "asterisk"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Something like t.* or db.table.*
  */
class ParserQualifiedAsterisk : public IParserBase
{
protected:
    const char * getName() const { return "qualified asterisk"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Функция, например, f(x, y + 1, g(z)).
  * Или агрегатная функция: sum(x + f(y)), corr(x, y). По синтаксису - такая же, как обычная функция.
  * Или параметрическая агрегатная функция: quantile(0.9)(x + y).
  *  Синтаксис - две пары круглых скобок вместо одной. Первая - для параметров, вторая - для аргументов.
  * Для функций может быть указан модификатор DISTINCT, например count(DISTINCT x, y).
  */
class ParserFunction : public IParserBase
{
protected:
    const char * getName() const { return "function"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

class ParserCastExpression : public IParserBase
{
    /// this name is used for identifying CAST expression among other function calls
    static constexpr auto name = "CAST";

protected:
    const char * getName() const override { return name; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};


/** NULL literal.
  */
class ParserNull : public IParserBase
{
protected:
    const char * getName() const { return "NULL"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Numeric literal.
  */
class ParserNumber : public IParserBase
{
protected:
    const char * getName() const { return "number"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

/** Unsigned integer, used in right hand side of tuple access operator (x.1).
  */
class ParserUnsignedInteger : public IParserBase
{
protected:
    const char * getName() const { return "unsigned integer"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** String in single quotes.
  */
class ParserStringLiteral : public IParserBase
{
protected:
    const char * getName() const { return "string literal"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


/** Массив литералов.
  * Массивы могут распарситься и как применение оператора [].
  * Но парсинг всего массива как целой константы серьёзно ускоряет анализ выражений в случае очень больших массивов.
  * Мы пробуем распарсить массив как массив литералов сначала (fast path),
  *  а если не получилось (когда массив состоит из сложных выражений) - парсим как применение оператора [] (slow path).
  */
class ParserArrayOfLiterals : public IParserBase
{
protected:
    const char * getName() const { return "array"; }
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
struct ParserAliasBase
{
    static const char * restricted_keywords[];
};

template <typename ParserIdentifier>
class ParserAliasImpl : public IParserBase, ParserAliasBase
{
public:
    ParserAliasImpl(bool allow_alias_without_as_keyword_)
        : allow_alias_without_as_keyword(allow_alias_without_as_keyword_) {}
protected:
    bool allow_alias_without_as_keyword;

    const char * getName() const { return "alias"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};


class ParserTypeInCastExpression;

extern template class ParserAliasImpl<ParserIdentifier>;
extern template class ParserAliasImpl<ParserTypeInCastExpression>;

using ParserAlias = ParserAliasImpl<ParserIdentifier>;
using ParserCastExpressionAlias = ParserAliasImpl<ParserTypeInCastExpression>;


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
template <typename ParserAlias>
class ParserWithOptionalAliasImpl : public IParserBase
{
public:
    ParserWithOptionalAliasImpl(ParserPtr && elem_parser_, bool allow_alias_without_as_keyword_)
        : elem_parser(std::move(elem_parser_)), allow_alias_without_as_keyword(allow_alias_without_as_keyword_) {}
protected:
    ParserPtr elem_parser;
    bool allow_alias_without_as_keyword;

    const char * getName() const { return "element of expression with optional alias"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

extern template class ParserWithOptionalAliasImpl<ParserAlias>;
extern template class ParserWithOptionalAliasImpl<ParserCastExpressionAlias>;

using ParserWithOptionalAlias = ParserWithOptionalAliasImpl<ParserAlias>;
using ParserCastExpressionWithOptionalAlias = ParserWithOptionalAliasImpl<ParserCastExpressionAlias>;


/** Element of ORDER BY expression - same as expression element, but in addition, ASC[ENDING] | DESC[ENDING] could be specified
  *  and optionally, NULLS LAST|FIRST
  *  and optionally, COLLATE 'locale'.
  */
class ParserOrderByElement : public IParserBase
{
protected:
    const char * getName() const { return "element of ORDER BY expression"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

/** Путь шарда в ZooKeeper вместе с весом.
  */
class ParserWeightedZooKeeperPath : public IParserBase
{
protected:
    const char * getName() const { return "weighted ZooKeeper path"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
