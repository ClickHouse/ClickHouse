#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Replaces all the hasAny(arr, arr1) OR hasAny(arr, arr2) OR ... with hasAny(arr, arr1 + arr2 + ...).
  * This is done to avoid creating a lot of hasAny functions in the query tree.
  * The function will be replaced with a single hasAny function with a concatenated array.
 */
class ConvertOrHasAnyChainPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertOrHasAnyChain"; }

    String getDescription() override { return "Replaces all the 'or's with hasAny on the same target with a single one"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
