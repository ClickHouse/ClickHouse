#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/// Parses both `DROP CLUSTER` and `DROP SHARD`; the concrete kind is recorded in
/// `ASTDropClusterCatalogQuery::kind`.
class ParserDropClusterCatalogQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP CLUSTER/SHARD query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
