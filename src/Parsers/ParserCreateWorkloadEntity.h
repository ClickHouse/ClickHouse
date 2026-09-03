#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Special parser for the CREATE WORKLOAD and CREATE RESOURCE queries.
class ParserCreateWorkloadEntity : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE workload entity query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
