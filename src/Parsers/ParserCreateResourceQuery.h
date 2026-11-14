#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// CREATE RESOURCE cache_io (WRITE DISK s3diskWithCache, READ DISK s3diskWithCache)
class ParserCreateResourceQuery : public IParserBase
{
public:
    std::map<String, Documentation> getDocumentation() const override;

protected:
    const char * getName() const override { return "CREATE RESOURCE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
