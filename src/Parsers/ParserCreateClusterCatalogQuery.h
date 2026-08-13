#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/// Parses both `CREATE CLUSTER` and `CREATE SHARD`; the concrete kind is recorded in
/// `ASTCreateClusterCatalogQuery::kind`.
class ParserCreateClusterCatalogQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE CLUSTER/SHARD query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
