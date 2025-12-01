#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Query like this:
  * EXTERNAL DDL FROM external_source(...) DROP|CREATE|RENAME ...
  * Example:
  *     EXTERNAL DDL FROM MySQL(clickhouse_db, mysql_db) DROP TABLE mysql_db.name;
  */
class ParserExternalDDLQuery : public IParserBase
{
protected:
    const char * getName() const override { return "EXTERNAL DDL query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
