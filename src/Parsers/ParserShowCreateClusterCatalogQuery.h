#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/// Parses both `SHOW CREATE CLUSTER` and `SHOW CREATE SHARD`; the concrete kind is recorded in
/// `ASTShowCreateClusterCatalogQuery::kind`.
class ParserShowCreateClusterCatalogQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SHOW CREATE CLUSTER/SHARD query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
