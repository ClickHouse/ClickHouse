#include <Storages/MergeTree/MergeTreeIndexTextPrePostProcessorUtils.h>

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

void replaceExpressionToIdentifier(ASTPtr & ast, const String & expression_name, const String & identifier_name)
{
    if (!ast)
        return;

    if ((ast->as<ASTIdentifier>() || ast->as<ASTFunction>()) && ast->getColumnName() == expression_name)
    {
        ast = make_intrusive<ASTIdentifier>(identifier_name);
        return;
    }

    for (auto & child : ast->children)
        replaceExpressionToIdentifier(child, expression_name, identifier_name);
}

namespace
{

/// A transform is analysed under the global context, so reading a table or a dictionary during
/// analysis happens with full access, and folding can surface the result before validation runs.
/// Such access depends on the arguments, not the function name, so authorizing names cannot cover it.
void rejectDataDependentConstructs(const ASTPtr & ast, const ContextPtr & context)
{
    if (ast->as<ASTSubquery>() || ast->as<ASTSelectWithUnionQuery>() || ast->as<ASTSelectQuery>() || ast->as<ASTTableIdentifier>())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "A text index transform expression must not contain subqueries or table references");

    if (const auto * function = ast->as<ASTFunction>())
    {
        if (functionIsInOrGlobalInOperator(function->name) && function->arguments && function->arguments->children.size() == 2)
        {
            const auto & set = function->arguments->children[1];
            if (set->as<ASTIdentifier>() || set->as<ASTTableIdentifier>())
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "A text index transform expression must not use a table or a set as the right side of `{}`", function->name);
        }

        /// Only the name, which for `dictGet` and the like does not touch the dictionary; analysis would.
        if (auto resolver = FunctionFactory::instance().tryGet(function->name, context); resolver && !resolver->isDeterministic())
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "A text index transform expression must not contain non-deterministic functions. Got `{}`", function->name);
    }

    for (const auto & child : ast->children)
        rejectDataDependentConstructs(child, context);
}

}

ActionsDAG buildActionsDAGFromAST(ASTPtr expression_ast, const NamesAndTypesList & source_columns)
{
    auto context = Context::getGlobalContextInstance();

    {
        /// `TreeRewriter` inlines SQL UDF bodies, so look into them as well.
        auto inlined = expression_ast->clone();
        if (!UserDefinedSQLFunctionFactory::instance().empty())
            UserDefinedSQLFunctionVisitor::visit(inlined, context);
        rejectDataDependentConstructs(inlined, context);
    }

    auto syntax_result = TreeRewriter(context).analyze(expression_ast, source_columns);
    auto actions_dag = ExpressionAnalyzer(expression_ast, syntax_result, context).getActionsDAG(false, true);

    auto expression_name = expression_ast->getColumnName();
    actions_dag.project({{expression_name, expression_name}});
    actions_dag.removeUnusedActions();

    /// Every capture is named `__lambda` and `ActionsDAG::mergeNodes` matches by name, so name captures by content.
    for (const auto & node : actions_dag.getNodes())
    {
        const auto * function_capture = dynamic_cast<const FunctionCapture *>(node.function_base.get());
        if (!function_capture)
            continue;

        const auto & capture = function_capture->getCapture();
        const_cast<ActionsDAG::Node &>(node).result_name = fmt::format(
            "__text_index_lambda({} -> {} | {})", capture.lambda_arguments.toString(), capture.return_name, fmt::join(capture.captured_names, ", "));
    }

    return actions_dag;
}

void validateTransformActionsDAG(const ActionsDAG & actions_dag, const String & transform_name, const String & source_name)
{
    const ActionsDAG::NodeRawConstPtrs & outputs = actions_dag.getOutputs();
    if (outputs.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The {} expression must return a single column. Got {} output columns", transform_name, outputs.size());

    if (outputs.front()->type != ActionsDAG::ActionType::FUNCTION)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The {} expression must be a function. Got '{}' action type", transform_name, outputs.front()->type);

    if (outputs.front()->result_name == source_name)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The {} must have at least one expression on top of the source column. Got '{}'", transform_name, outputs.front()->result_name);

    if (actions_dag.hasNonDeterministic())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The {} expression must not contain non-deterministic functions", transform_name);

    if (actions_dag.hasArrayJoin())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The {} expression must not contain arrayJoin", transform_name);
}

ColumnPtr executeUnaryExpressionActions(
    const ExpressionActions & actions,
    ColumnPtr column,
    const DataTypePtr & type,
    const String & column_name,
    size_t n_rows)
{
    Block block({ColumnWithTypeAndName(column, type, column_name)});
    actions.execute(block, n_rows);
    return block.safeGetByPosition(0).column;
}

}
