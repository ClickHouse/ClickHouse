#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

namespace MySQLParser
{

/** An MySQL identifier, In most cases, it is the same as ParserIdentifier, but it allows 123abc or 123_abc
  * For more information: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
  */
class ParserMySQLIdentifier : public IParserBase
{
protected:
    const char * getName() const override { return "mysql identifier"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
