#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Тип или Storage, возможно, параметрический. Например, UInt8 или FixedString(10) или Partitioned(Log, ChunkID)
  * Результат парсинга - ASTFunction с параметрами или без. 
  */
class ParserIdentifierWithOptionalParameters : public IParserBase
{
protected:
	String getName() { return "identifier with optional parameters"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Имя и тип через пробел. Например, URL String. */
class ParserNameTypePair : public IParserBase
{
protected:
	String getName() { return "name and type pair"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};


/** Запрос типа такого:
  * CREATE|ATTACH TABLE [IF NOT EXISTS] name
  * (
  * 	name1 type1,
  * 	name2 type2,
  * 	...
  * ) ENGINE = engine
  */
class ParserCreateQuery : public IParserBase
{
protected:
	String getName() { return "CREATE TABLE or ATTACH TABLE query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};

}
