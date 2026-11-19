#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Parse either a partition value as a (possibly compound) literal or a partition ID.
/// Produce ASTPartition.
class ParserPartition : public IParserBase
{
protected:
    const char * getName() const override { return "partition"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
