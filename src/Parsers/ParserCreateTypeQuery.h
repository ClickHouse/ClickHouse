#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/** Parser for CREATE TYPE query:
  *     CREATE TYPE [IF NOT EXISTS | OR REPLACE] type_name[(parameter, ...)] AS base_type
  */
class ParserCreateTypeQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE TYPE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
