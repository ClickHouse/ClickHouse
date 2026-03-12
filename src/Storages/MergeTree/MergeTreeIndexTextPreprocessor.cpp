#include <Storages/MergeTree/MergeTreeIndexTextPreprocessor.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/IColumn_fwd.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ExpressionListParsers.h>
#include <Storages/IndicesDescription.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace
{

constexpr char preprocessor_lambda_arg[] = "__text_index_x";
constexpr char preprocessor_column_name[] = "__text_index_column";

/// Replaces subtrees in the AST whose canonical name matches `expression_name` with an identifier named `identifier_name`.
/// Unlike RenameColumnVisitor which only handles plain identifiers, this also handles
/// function expressions (e.g., replacing the `lower(val)` subtree with a lambda variable).
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

ASTPtr convertASTForIndexColumn(const IndexDescription & index, const ASTPtr & expression_ast, bool replace_index_column)
{
    chassert(index.column_names.size() == 1);
    chassert(index.data_types.size() == 1);
    chassert(index.expression_list_ast != nullptr);
    chassert(index.expression_list_ast->children.size() == 1);

    if (expression_ast == nullptr)
        return nullptr;

    /// Transform a preprocessor AST like `lower(val)` into `arrayMap(x -> lower(x), val)`.
    /// This is done at the AST level so that ActionsVisitor can build the DAG naturally.
    if (isArray(index.data_types.front()))
    {
        /// Firstly replace the index expression with lambda argument.
        ASTPtr new_expression = expression_ast->clone();
        replaceExpressionToIdentifier(new_expression, index.column_names.front(), preprocessor_lambda_arg);

        /// Create the array argument of arrayMap function.
        auto array_map_arg = replace_index_column
            ? make_intrusive<ASTIdentifier>(preprocessor_column_name)
            : index.expression_list_ast->children.front();

        /// Pack preprocessor expression into lambda.
        auto lambda_arg = makeASTFunction("tuple", make_intrusive<ASTIdentifier>(preprocessor_lambda_arg));
        auto lambda_ast = makeASTFunction("lambda", lambda_arg, new_expression);
        return makeASTFunction("arrayMap", lambda_ast, array_map_arg);
    }

    if (replace_index_column)
    {
        ASTPtr new_expression = expression_ast->clone();
        replaceExpressionToIdentifier(new_expression, index.column_names.front(), preprocessor_column_name);
        return new_expression;
    }

    return expression_ast->clone();
}

ASTPtr convertASTForConstant(const IndexDescription & index, const ASTPtr & expression_ast)
{
    chassert(index.column_names.size() == 1);
    chassert(index.data_types.size() == 1);

    if (expression_ast == nullptr)
        return nullptr;

    ASTPtr body = expression_ast->clone();
    replaceExpressionToIdentifier(body, index.column_names.front(), preprocessor_column_name);
    return body;
}

/// Creates and validates an ActionsDAG for a preprocessor expression.
ActionsDAG createActionsDAGForPreprocessor(
    const NamesAndTypesList & source_columns,
    const String & source_name,
    const DataTypePtr & source_type,
    ASTPtr expression_ast)
{
    if (expression_ast == nullptr)
        return ActionsDAG();

    auto context = Context::getGlobalContextInstance();
    auto syntax_result = TreeRewriter(context).analyze(expression_ast, source_columns);
    auto actions_dag = ExpressionAnalyzer(expression_ast, syntax_result, context).getActionsDAG(false, true);

    auto expression_name = expression_ast->getColumnName();
    actions_dag.project({{expression_name, expression_name}});
    actions_dag.removeUnusedActions();

    const ActionsDAG::NodeRawConstPtrs & outputs = actions_dag.getOutputs();
    if (outputs.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must return a single column. Got {} output columns", outputs.size());

    if (outputs.front()->type != ActionsDAG::ActionType::FUNCTION)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must be a function. Got '{}' action type", outputs.front()->type);

    if (outputs.front()->result_name == source_name)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor must have at least one expression on top of the source column. Got '{}'", outputs.front()->result_name);

    if (!outputs.front()->result_type->equals(*source_type))
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression should return the same type as the source column. Got '{}', expected '{}'", outputs.front()->result_type->getName(), source_type->getName());

    if (actions_dag.hasNonDeterministic())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must not contain non-deterministic functions");

    if (actions_dag.hasArrayJoin())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The preprocessor expression must not contain arrayJoin");

    return actions_dag;
}

}

MergeTreeIndexTextPreprocessor::MergeTreeIndexTextPreprocessor(ASTPtr expression_ast, const IndexDescription & index_description)
    : index_column_type(index_description.data_types.front())
    /// Use source index columns to execute index and preprocessor expressions.
    , original_actions(createActionsDAGForPreprocessor(
        index_description.expression->getRequiredColumnsWithTypes(),
        index_description.column_names.front(),
        index_column_type,
        convertASTForIndexColumn(index_description, expression_ast, false)))
    /// Assume that index expression is already executed and use a placeholder column to execute preprocessor expression.
    , actions_for_index_column(createActionsDAGForPreprocessor(
        {{preprocessor_column_name, index_column_type}},
        preprocessor_column_name,
        index_column_type,
        convertASTForIndexColumn(index_description, expression_ast, true)))
    /// Take constant string and execute preprocessor expression.
    , actions_for_constant(createActionsDAGForPreprocessor(
        {{preprocessor_column_name, std::make_shared<DataTypeString>()}},
        preprocessor_column_name,
        std::make_shared<DataTypeString>(),
        convertASTForConstant(index_description, expression_ast)))
{
}

std::pair<ColumnPtr, size_t> MergeTreeIndexTextPreprocessor::processColumn(const ColumnWithTypeAndName & column, size_t start_row, size_t n_rows) const
{
    ColumnPtr index_column = column.column;
    if (actions_for_index_column.getActions().empty())
        return {index_column, start_row};

    chassert(column.type->equals(*index_column_type));
    chassert(index_column->getDataType() == column.type->getTypeId());

    /// Only copy if needed
    if (start_row != 0 || n_rows != index_column->size())
        index_column = index_column->cut(start_row, n_rows);

    Block block({ColumnWithTypeAndName(index_column, index_column_type, preprocessor_column_name)});
    actions_for_index_column.execute(block, n_rows);
    return {block.safeGetByPosition(0).column, 0};
}

String MergeTreeIndexTextPreprocessor::processConstant(const String & input) const
{
    if (actions_for_constant.getActions().empty())
        return input;

    auto input_type = std::make_shared<DataTypeString>();
    auto input_column = input_type->createColumnConst(1, Field(input));
    Block block{{ColumnWithTypeAndName(input_column, input_type, preprocessor_column_name)}};

    size_t n_rows = 1;
    actions_for_constant.execute(block, n_rows);
    return String{block.safeGetByPosition(0).column->getDataAt(0)};
}

}
