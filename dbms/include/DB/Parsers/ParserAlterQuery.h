#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>

namespace DB
{
/** Запрос типа такого:
  * ALTER TABLE [db.]name
  * 	[ADD COLUMN col_name type [AFTER col_after],]
  *		[DROP COLUMN col_drop, ...]
  * 	[MODIFY COLUMN col_modify type, ...]
  */
class ParserAlterQuery : public IParserBase
{
protected:
	const char * getName() const { return "ALTER query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected);
};

}
