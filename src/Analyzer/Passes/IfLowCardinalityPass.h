// #pragma once

// #include <Analyzer/IQueryTreePass.h>

// namespace DB
// {
//     /** Convert if with constant branches to return LowCardinality.
//       * Replace if(cond, 'hello', 'world') with LowCardinality(if(cond, 'hello', 'world')).
//       */
//     class IfConstantBranchesToLowCardinalityPass final : public IQueryTreePass
//     {
//     public:
//         String getName() override { return "IfConstantBranchesToLowCardinality"; }

//         String getDescription() override { return "Optimize if with constant branches to return LowCardinality"; }

//         void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;

//     };

// }
