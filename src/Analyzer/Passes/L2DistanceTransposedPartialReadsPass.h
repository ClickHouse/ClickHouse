#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Convert `L2DistanceTransposed(vec, reference_vec, N)` into single `L2DistanceTransposed(vec.1, vec.2, ..., vec.N, qbit_size, reference_vec)`.
  * This allows to read the first N groups of vec instead of all.
  *
  * Example: SELECT id, vec, L2DistanceTransposed(vec, reference_vec, 3) AS dist FROM qbit;
  * Result: SELECT id, vec, L2DistanceTransposed(vec.1, vec.2, vec.3, 3, reference_vec) AS dist FROM qbit;
  */
class L2DistanceTransposedPartialReadsPass final : public IQueryTreePass
{
public:
    String getName() override { return "L2DistanceTransposedPartialReadsPass"; }

    String getDescription() override { return "Optimise L2DistanceTransposed to access only necessary subcolumns"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
