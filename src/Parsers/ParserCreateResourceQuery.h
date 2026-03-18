#pragma once

#include "IParserBase.h"

namespace DB
{

/// CREATE RESOURCE cache_io (WRITE DISK s3diskWithCache, READ DISK s3diskWithCache)
class ParserCreateResourceQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE RESOURCE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
