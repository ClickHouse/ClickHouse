#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>

namespace DB
{
/** Запрос типа такого:
  * ALTER TABLE [db.]name
  * 	[ADD COLUMN col_type [AFTER col_after],]
  *		[DROP COLUMN col_drop, ...]
  */
class ParserAlterQuery : public IParserBase
{
protected:
	const char * getName() const { return "ALTER query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected);
};

}
