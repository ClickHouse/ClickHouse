#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/CommonParsers.h>


namespace DB
{

/** Вложенная таблица. Например, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
  */
class ParserNestedTable : public IParserBase
{
protected:
	const char * getName() const { return "nested table"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected);
};


/** Параметрический тип или Storage. Например:
 * 		FixedString(10) или
 * 		Partitioned(Log, ChunkID) или
 * 		Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
  * Результат парсинга - ASTFunction с параметрами или без.
  */
class ParserIdentifierWithParameters : public IParserBase
{
protected:
	const char * getName() const { return "identifier with parameters"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected);
};


/** Тип или Storage, возможно, параметрический. Например, UInt8 или примеры из ParserIdentifierWithParameters
  * Результат парсинга - ASTFunction с параметрами или без.
  */
class ParserIdentifierWithOptionalParameters : public IParserBase
{
protected:
	const char * getName() const { return "identifier with optional parameters"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected);
};


template <class NameParser>
class IParserNameTypePair : public IParserBase
{
protected:
	const char * getName() const { return "name and type pair"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected);
};

/** Имя и тип через пробел. Например, URL String. */
typedef IParserNameTypePair<ParserIdentifier> ParserNameTypePair;
/** Имя и тип через пробел. Имя может содержать точку. Например, Hits.URL String. */
typedef IParserNameTypePair<ParserCompoundIdentifier> ParserCompoundNameTypePair;

template <class NameParser>
bool IParserNameTypePair<NameParser>::parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected)
{
	NameParser name_parser;
	ParserIdentifierWithOptionalParameters type_parser;
	ParserWhiteSpaceOrComments ws_parser;

	Pos begin = pos;

	ASTPtr name, type;
	if (name_parser.parse(pos, end, name, expected)
		&& ws_parser.ignore(pos, end, expected)
		&& type_parser.parse(pos, end, type, expected))
	{
		ASTNameTypePair * name_type_pair = new ASTNameTypePair(StringRange(begin, pos));
		node = name_type_pair;
		name_type_pair->name = typeid_cast<ASTIdentifier &>(*name).name;
		name_type_pair->type = type;
		name_type_pair->children.push_back(type);
		return true;
	}

	pos = begin;
	return false;
}

/** Список столбцов.  */
class ParserNameTypePairList : public IParserBase
{
protected:
	const char * getName() const { return "name and type pair list"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected);
};


template <class NameParser>
class IParserColumnDeclaration : public IParserBase
{
protected:
	const char * getName() const { return "column declaration"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected);
};

typedef IParserColumnDeclaration<ParserIdentifier> ParserColumnDeclaration;
typedef IParserColumnDeclaration<ParserCompoundIdentifier> ParserCompoundColumnDeclaration;

template <class NameParser>
bool IParserColumnDeclaration<NameParser>::parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected)
{
	NameParser name_parser;
	ParserIdentifierWithOptionalParameters type_parser;
	ParserWhiteSpaceOrComments ws;
	ParserString s_default{"DEFAULT"};
	ParserString s_materialized{"MATERIALIZED"};
	ParserString s_alias{"ALIAS"};
	ParserExpressionElement expr_parser;

	const auto begin = pos;

	/// mandatory column name
	ASTPtr name;
	if (!name_parser.parse(pos, end, name, expected))
		return false;

	ws.ignore(pos, end, expected);

	/** column name should be followed by type name if it
	 *	is not immediately followed by {DEFAULT, MATERIALIZED, ALIAS} */
	ASTPtr type;
	const auto fallback_pos = pos;
	if (!s_default.check(pos, end, expected) &&
		!s_materialized.check(pos, end, expected) &&
		!s_alias.check(pos, end, expected))
	{
		/// reject sole column name without type
		if (type_parser.parse(pos, end, type, expected))
			ws.ignore(pos, end, expected);
	} else pos = fallback_pos;

	/// parse {DEFAULT, MATERIALIZED, ALIAS}
	String default_specifier;
	ASTPtr default_expression;
	const auto pos_before_specifier = pos;
	if (s_default.ignore(pos, end, expected) ||
		s_materialized.ignore(pos, end, expected) ||
		s_alias.ignore(pos, end, expected))
	{
		default_specifier.assign(pos_before_specifier, pos);

		/// should be followed by an expression
		ws.ignore(pos, end, expected);

		if (!expr_parser.parse(pos, end, default_expression, expected))
		{
			pos = begin;
			return false;
		}
	}
	else if (!type)
	{
		pos = begin;
		return false;
	}

	/// @todo remove this
	if (!type) {
		const auto no_type = "NO_TYPE";
		auto pos = no_type;
		type_parser.parse(pos, no_type + std::strlen(no_type), type, expected);
	}

	const auto name_type_pair = new ASTNameTypePair{StringRange{begin, pos}};
	node = name_type_pair;
	name_type_pair->name = typeid_cast<ASTIdentifier &>(*name).name;
	name_type_pair->type = type;
	name_type_pair->children.push_back(std::move(type));

	return true;
}

class ParserColumnDeclarationList : public IParserBase
{
protected:
	const char * getName() const { return "column declaration list"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected);
};


/** ENGINE = name. */
class ParserEngine : public IParserBase
{
protected:
	const char * getName() const { return "ENGINE"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected);
};


/** Запрос типа такого:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name
  * (
  * 	name1 type1,
  * 	name2 type2,
  * 	...
  * ) ENGINE = engine
  *
  * Или:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name AS [db2.]name2 [ENGINE = engine]
  *
  * Или:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] [db.]name AS ENGINE = engine SELECT ...
  *
  * Или:
  * CREATE|ATTACH DATABASE db
  *
  * Или:
  * CREATE|ATTACH [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]name [ENGINE = engine] [POPULATE] AS SELECT ...
  */
class ParserCreateQuery : public IParserBase
{
protected:
	const char * getName() const { return "CREATE TABLE or ATTACH TABLE query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected);
};

}
