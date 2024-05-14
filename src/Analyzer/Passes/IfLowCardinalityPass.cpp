#include <Analyzer/Passes/IfLowCardinalityPass.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{
    void IfConstantBranchesToLowCardinalityPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
    {
        /// Implement your optimization logic here
        IfTreePtr if_node = std::dynamic_pointer_cast<IfTree>(query_tree_node);
        if (!if_node)
            return;

        auto & analyzer = context->getAnalyzer();

        // Recursively transform constant branches to LowCardinality
        auto transformConstantBranchesToLowCardinality = [&](ASTPtr & node) {
            if (!node)
                return;
            if (auto * func_node = typeid_cast<FunctionNode *>(node.get())) {
                if (func_node->name == "if") {
                    for (size_t i = 1; i < func_node->arguments.size(); ++i) {
                        transformConstantBranchesToLowCardinality(func_node->arguments[i]);
                    }
                }
            } else if (auto * literal_node = typeid_cast<LiteralNode *>(node.get())) {
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
