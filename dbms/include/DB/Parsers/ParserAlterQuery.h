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
  * 	[DROP|DETACH|ATTACH [UNREPLICATED] PARTITION|PART partition, ...]
  * 	[FETCH PARTITION partition FROM ...]
  * 	[FREEZE PARTITION]
  */
class ParserAlterQuery : public IParserBase
{
protected:
	const char * getName() const { return "ALTER query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
