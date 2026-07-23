#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parses:
  *   CREATE HYPOTHETICAL INDEX [IF NOT EXISTS] name ON [db.]table (expr) TYPE type(args) GRANULARITY n
  *   DROP HYPOTHETICAL INDEX [IF EXISTS] name ON [db.]table
  *   DROP ALL HYPOTHETICAL INDEXES
  */
class ParserHypotheticalIndexQuery : public IParserBase
{
protected:
    const char * getName() const override { return "HYPOTHETICAL INDEX query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
