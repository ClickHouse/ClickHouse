#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// For a table with an IVF index, rewrites ORDER BY dist(vec, q) LIMIT k by adding WHERE cid IN (SELECT cid FROM centroids
/// WHERE level = 0 ORDER BY dist(vec, q) LIMIT nprobe), so that primary key pruning reads only the probed clusters.
class IvfClusterPruningPass final : public IQueryTreePass
{
public:
    String getName() override { return "IvfClusterPruning"; }

    String getDescription() override
    {
        return "Inject coarse-cluster pruning (cid IN (SELECT ...)) for IVF-organized tables so ORDER BY distance LIMIT prunes by primary key";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
