#pragma once

#include <memory>
#include <vector>
#include <Interpreters/SelectQueryOptions.h>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
using QueryTreeNodes = std::vector<QueryTreeNodePtr>;

struct TemporaryTableHolder;
using TemporaryTableHolderPtr = std::shared_ptr<TemporaryTableHolder>;

class QueryPlan;

using OrderedMaterializedCTEs = std::vector<QueryTreeNodes>;

OrderedMaterializedCTEs collectMaterializedCTEs(const QueryTreeNodePtr & node, const SelectQueryOptions & select_query_options);

/// Plants one `DelayedMaterializingCTEsStep` per dependency level of `materialized_ctes` on top of
/// `query_plan`, building the plan of every CTE that has none yet. Plan optimization turns the step
/// into the `MaterializingCTEsStep` whose gate orders every reader of a CTE after its writer.
void addBuildSubqueriesForMaterializedCTEsIfNeeded(
    QueryPlan & query_plan,
    const SelectQueryOptions & select_query_options,
    const OrderedMaterializedCTEs & materialized_ctes);

}
