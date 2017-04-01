#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** KILL QUERY WHERE <logical expression upon system.processes fields> [SYNC|ASYNC|TEST]
  */
class ParserKillQueryQuery : public IParserBase
{
protected:
    const char * getName() const override { return "KILL QUERY query"; }
    bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) override;
};

}

