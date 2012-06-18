#pragma once

#include <DB/Parsers/IParserBase.h>


namespace DB
{

/** Запрос типа такого:
  * SHOW TABLES [FROM db] [LIKE 'str']
  * или
  * SHOW DATABASES.
  */
class ParserShowTablesQuery : public IParserBase
{
protected:
	String getName() { return "SHOW TABLES|DATABASES query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};

}
