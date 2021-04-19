#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/// Stub for MySQL protocol compatibility
class ParserTransactionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "Transaction query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}


