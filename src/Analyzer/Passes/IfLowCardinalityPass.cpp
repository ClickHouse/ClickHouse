// #include <Analyzer/Passes/IfLowCardinalityPass.h>
// #include <DataTypes/DataTypeLowCardinality.h>
// #include <Interpreters/ExpressionActions.h>
// #include <Parsers/ASTFunction.h>

// namespace DB
// {

// namespace
// {

// class IfLowCardinalityPassVisitor : public InDepthQueryTreeVisitorWithContext<IfLowCardinalityPassVisitor>
// {
// public:
//     using Base = InDepthQueryTreeVisitorWithContext<IfLowCardinalityPassVisitor>;
//     using Base::Base;

//     explicit IfLowCardinalityPassVisitor(ContextPtr context)
//         : Base(std::move(context))
//     {}

//     void enterImpl(QueryTreeNodePtr & node)
//     {
//         auto * function_node = node->as<ASTFunction>();
//         if (!function_node || function_node->name != "if" || function_node->arguments.size() != 4)
//             return;

//         const auto & then_branch = function_node->arguments.at(2);
//         const auto & else_branch = function_node->arguments.at(3);

//         DataTypePtr then_type = ExpressionActions(false).getReturnType(then_branch);
//         DataTypePtr else_type = ExpressionActions(false).getReturnType(else_branch);

//         if (then_type->getName() == "String" && else_type->getName() == "String")
//         {
//             DataTypePtr result_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
//             function_node->arguments.at(0) = result_type->createASTLiteral();
//         }
//     }
// };

// }

// void IfLowCardinalityPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
// {
//     IfLowCardinalityPassVisitor visitor(std::move(context));
//     visitor.visit(query_tree_node);
// }

// }
