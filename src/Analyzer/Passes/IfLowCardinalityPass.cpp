#include <Analyzer/Passes/IfLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{
    void IfConstantBranchesToLowCardinalityPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
    {
        /// Implement your optimization logic here
        IfNodePtr if_node = query_tree_node->as<IfNode>();
        if (!if_node)
            return;

        auto & analyzer = context->getAnalyzer();

        // Recursively transform constant branches to LowCardinality
        auto transformConstantBranchesToLowCardinality = [&](ASTPtr & node) {
            if (!node)
                return;
            if (auto * func_node = node->as<FunctionNode>()) {
                if (func_node->getFunctionName() == "if") {
                    for (size_t i = 1; i < func_node->arguments.size(); ++i) {
                        transformConstantBranchesToLowCardinality(func_node->arguments[i]);
                    }
                }
            } else if (auto * literal_node = node->as<LiteralNode>()) {
                if (literal_node->value.getType() == Field::Types::String) {
                    literal_node->value = LowCardinality(literal_node->value.get<String>());
                }
            } else {
                for (auto & child : node->children) {
                    transformConstantBranchesToLowCardinality(child);
                }
            }
        };

        transformConstantBranchesToLowCardinality(if_node->then_branch);
        transformConstantBranchesToLowCardinality(if_node->else_branch);
    }
}
