#pragma once

#include "IParserBase.h"

namespace DB
{

/// CREATE WORKLOAD production IN all SETTINGS weight = 3, max_speed = '1G' FOR network_read, max_speed = '2G' FOR network_write
class ParserCreateWorkloadQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE WORKLOAD query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
