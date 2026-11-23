#include <Storages/MergeTree/SkippingIndexCache.h>

#include <Parsers/IAST.h>
#include <Storages/IndicesDescription.h>

namespace DB
{

UInt128 SkippingIndexCache::hashIndexDefinition(const IndexDescription & index)
{
    /// The definition AST covers the name, the expression, the type and its arguments.
    /// The granularity is part of the AST too, but only when spelled out explicitly, so hash it separately;
    /// it determines the number of index marks and thus the granule blocks' layout.
    auto ast_hash = index.definition_ast->getTreeHash(/*ignore_aliases=*/ true);

    SipHash hash;
    hash.update(ast_hash.low64);
    hash.update(ast_hash.high64);
    hash.update(index.granularity);
    return hash.get128();
}

}
