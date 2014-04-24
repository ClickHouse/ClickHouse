#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Вложенная таблица. Например, Nested(UInt32 CounterID, FixedString(2) UserAgentMajor)
  */
class ParserNestedTable : public IParserBase
{
protected:
	const char * getName() const { return "nested table"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected);
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
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected);
};


/** Тип или Storage, возможно, параметрический. Например, UInt8 или примеры из ParserIdentifierWithParameters
  * Результат парсинга - ASTFunction с параметрами или без.
  */
class ParserIdentifierWithOptionalParameters : public IParserBase
{
protected:
	const char * getName() const { return "identifier with optional parameters"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected);
};


/** Имя и тип через пробел. Например, URL String. */
class ParserNameTypePair : public IParserBase
{
protected:
	const char * getName() const { return "name and type pair"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected);
};


/** Список столбцов.  */
class ParserNameTypePairList : public IParserBase
{
protected:
	const char * getName() const { return "name and type pair list"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected);
};


/** ENGINE = name. */
class ParserEngine : public IParserBase
{
protected:
	const char * getName() const { return "ENGINE"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected);
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
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected);
};

}
