#pragma once

#include "IParserBase.h"

namespace DB
{
/// DROP WORKLOAD workload1
class ParserDropWorkloadQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP WORKLOAD query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
