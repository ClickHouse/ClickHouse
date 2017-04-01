#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


/** Варианты:
  *
  * Обычный вариант:
  * INSERT INTO [db.]table (c1, c2, c3) VALUES (v11, v12, v13), (v21, v22, v23), ...
  * INSERT INTO [db.]table VALUES (v11, v12, v13), (v21, v22, v23), ...
  *
  * Вставка данных в произвольном формате.
  * Сами данные идут после перевода строки, если он есть, или после всех пробельных символов, иначе.
  * INSERT INTO [db.]table (c1, c2, c3) FORMAT format \n ...
  * INSERT INTO [db.]table FORMAT format \n ...
  *
  * Вставка результата выполнения SELECT запроса.
  * INSERT INTO [db.]table (c1, c2, c3) SELECT ...
  * INSERT INTO [db.]table SELECT ...
  */
class ParserInsertQuery : public IParserBase
{
protected:
    const char * getName() const { return "INSERT query"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
