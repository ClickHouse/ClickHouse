#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Запрос типа такого:
  * RENAME TABLE [db.]name TO [db.]name, [db.]name TO [db.]name, ...
  * (Переименовываться может произвольное количество таблиц.)
  */
class ParserRenameQuery : public IParserBase
{
protected:
    const char * getName() const { return "RENAME query"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
