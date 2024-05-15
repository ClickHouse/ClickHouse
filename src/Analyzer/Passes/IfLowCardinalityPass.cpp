#include <Analyzer/Passes/IfLowCardinalityPass.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
    void IfLowCardinalityPass::optimizeIfFunction(ASTPtr & node)
    {
        if (auto * if_function = typeid_cast<ASTFunction *>(node.get()))
        {
            if (if_function->name == "if" && if_function->arguments.size() == 4)
            {
                const auto & then_branch = if_function->arguments.at(2);
                const auto & else_branch = if_function->arguments.at(3);

                DataTypePtr then_type = ExpressionActions(false).getReturnType(then_branch);
                DataTypePtr else_type = ExpressionActions(false).getReturnType(else_branch);

                if (then_type->getName() == "String" && else_type->getName() == "String")
                {
                    DataTypePtr result_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
                    if_function->children.at(0) = result_type->createASTLiteral();
                }
            }
        }
    }

    void IfLowCardinalityPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
    {
        if (!query_tree_node)
            return;

        optimizeIfFunction(query_tree_node);
    }
}
