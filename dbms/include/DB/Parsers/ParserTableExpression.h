#pragma once

#include <DB/Parsers/IParserBase.h>


namespace DB
{

/** Имя таблицы (с или без имени БД), табличная функция, подзапрос.
  * Без модификаторов FINAL, SAMPLE и т. п.
  * Без алиаса.
  */
class ParserTableExpression : public IParserBase
{
protected:
	const char * getName() const { return "table or subquery or table function"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
