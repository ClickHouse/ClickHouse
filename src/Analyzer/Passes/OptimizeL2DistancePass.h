// #pragma once

// #include <Analyzer/IQueryTreePass.h>
// #include <Parsers/IAST.h>

// namespace DB
// {

// /** Replace L2Distance(...) with sqrt(L2SquaredDistance(...)) in SQL queries.
//   * Example: SELECT vec FROM t ORDER BY L2Distance([1,2,3], vec) DESC LIMIT 10
//   * Result: SELECT vec FROM t ORDER BY sqrt(L2SquaredDistance([1,2,3], vec)) DESC LIMIT 10
//   */
// class L2DistanceOptimizationPass final : public IQueryTreePass
// {
// public:
//     String getName() override { return "L2DistanceOptimization"; }

//     String getDescription() override { return "Replace L2Distance(...) with sqrt(L2SquaredDistance(...))"; }

//     void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
// };

// }
