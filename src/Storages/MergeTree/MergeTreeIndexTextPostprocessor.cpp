#include <Storages/MergeTree/MergeTreeIndexTextPostprocessor.h>

#include <Core/ColumnWithTypeAndName.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IndicesDescription.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}

namespace
{

/// Name of the placeholder column used when building the postprocessor ActionsDAG.
constexpr char postprocessor_token_name[] = "__text_index_token";

/// Replaces subtrees in the AST whose canonical name matches `expression_name` with an identifier
/// named `identifier_name`. This handles both plain identifiers and function expressions.
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

}

MergeTreeIndexTextPostprocessor::MergeTreeIndexTextPostprocessor(ASTPtr expression_ast, const IndexDescription & index_description)
    : string_type(std::make_shared<DataTypeString>())
{
    if (!expression_ast)
        return;

    chassert(index_description.column_names.size() == 1);

    /// Replace the index column name with the token placeholder.
    /// The postprocessor always operates on String tokens (not the original column type).
    ASTPtr transformed_ast = expression_ast->clone();
    replaceExpressionToIdentifier(transformed_ast, index_description.column_names.front(), postprocessor_token_name);

    /// Build ActionsDAG treating the input as a plain String token.
    NamesAndTypesList source_columns{{postprocessor_token_name, string_type}};

    auto context = Context::getGlobalContextInstance();
    auto syntax_result = TreeRewriter(context).analyze(transformed_ast, source_columns);
    auto actions_dag = ExpressionAnalyzer(transformed_ast, syntax_result, context).getActionsDAG(false, true);

    auto expression_name = transformed_ast->getColumnName();
    actions_dag.project({{expression_name, expression_name}});
    actions_dag.removeUnusedActions();

    const ActionsDAG::NodeRawConstPtrs & outputs = actions_dag.getOutputs();
    if (outputs.size() != 1)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "The postprocessor expression must return a single column. Got {} output columns",
            outputs.size());

    if (outputs.front()->type != ActionsDAG::ActionType::FUNCTION)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "The postprocessor expression must be a function. Got '{}' action type",
            outputs.front()->type);

    if (outputs.front()->result_name == postprocessor_token_name)
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "The postprocessor must have at least one expression on top of the token. Got '{}'",
            outputs.front()->result_name);

    if (!outputs.front()->result_type->equals(*string_type))
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "The postprocessor expression must return String type. Got '{}'",
            outputs.front()->result_type->getName());

    if (actions_dag.hasNonDeterministic())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The postprocessor expression must not contain non-deterministic functions");

    if (actions_dag.hasArrayJoin())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "The postprocessor expression must not contain arrayJoin");

    actions.emplace(std::move(actions_dag));
}

std::vector<String> MergeTreeIndexTextPostprocessor::applyBatch(std::vector<String> tokens) const
{
    if (!actions || tokens.empty())
        return tokens;

    auto tokens_col = ColumnString::create();
    for (const auto & token : tokens)
        tokens_col->insertData(token.data(), token.size());

    ColumnPtr result = processTokensBatch(std::move(tokens_col));

    tokens.clear();
    for (size_t i = 0; i < result->size(); ++i)
    {
        auto ref = result->getDataAt(i);
        if (ref.size() > 0)
            tokens.push_back(String{ref.data(), ref.size()});
    }
    return tokens;
}

ColumnPtr MergeTreeIndexTextPostprocessor::processTokensBatch(ColumnPtr tokens_column) const
{
    if (!actions)
        return tokens_column;

    size_t n_rows = tokens_column->size();
    Block block{{ColumnWithTypeAndName(std::move(tokens_column), string_type, postprocessor_token_name)}};
    actions->execute(block, n_rows);
    return block.safeGetByPosition(0).column;
}

ColumnPtr MergeTreeIndexTextPostprocessor::processTokensArrayBatch(ColumnPtr tokens_array_column) const
{
    if (!actions)
        return tokens_array_column;

    const auto & col_array = assert_cast<const ColumnArray &>(*tokens_array_column);
    const IColumn::Offsets & src_offsets = col_array.getOffsets();
    const size_t num_rows = src_offsets.size();

    /// Apply the postprocessor on all token strings across all rows in one execution.
    ColumnPtr flat_transformed = processTokensBatch(col_array.getDataPtr());

    /// Rebuild the ColumnArray with updated offsets.
    /// Tokens transformed to empty string are filtered out (e.g. stop words).
    auto result_data = ColumnString::create();
    auto result_offsets_col = ColumnArray::ColumnOffsets::create();
    result_offsets_col->reserve(num_rows);
    auto & result_offsets = result_offsets_col->getData();

    size_t flat_idx = 0;
    size_t write_offset = 0;
    for (size_t row = 0; row < num_rows; ++row)
    {
        const size_t row_end = src_offsets[row];
        while (flat_idx < row_end)
        {
            auto ref = flat_transformed->getDataAt(flat_idx);
            if (ref.size() > 0)
            {
                result_data->insertData(ref.data(), ref.size());
                ++write_offset;
            }
            ++flat_idx;
        }
        result_offsets.push_back(write_offset);
    }

    return ColumnArray::create(std::move(result_data), std::move(result_offsets_col));
}

}
