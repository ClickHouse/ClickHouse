#include <Storages/MergeTree/MergeTreeIndexTextPostprocessor.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IndicesDescription.h>
#include <Storages/MergeTree/MergeTreeIndexTextPrePostProcessorUtils.h>

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
constexpr char postprocessor_lambda_arg[] = "__text_index_lambda_arg";

}

MergeTreeIndexTextPostprocessor::MergeTreeIndexTextPostprocessor(ASTPtr expression_ast, const IndexDescription & index_description)
    : string_type(std::make_shared<DataTypeString>())
{
    if (!expression_ast)
        return;

    chassert(index_description.column_names.size() == 1);

    original_expression_ast = expression_ast->clone();
    index_column_name = index_description.column_names.front();

    /// Replace the index column name with the token placeholder.
    /// The postprocessor always operates on String tokens (not the original column type).
    ASTPtr transformed_ast = expression_ast->clone();
    replaceExpressionToIdentifier(transformed_ast, index_column_name, postprocessor_token_name);

    /// Build ActionsDAG treating the input as a plain String token.
    NamesAndTypesList source_columns{{postprocessor_token_name, string_type}};
    ActionsDAG actions_dag = buildActionsDAGFromAST(transformed_ast, source_columns);
    validateTransformActionsDAG(actions_dag, "postprocessor", postprocessor_token_name);

    const ActionsDAG::NodeRawConstPtrs & outputs = actions_dag.getOutputs();
    if (!outputs.front()->result_type->equals(*string_type))
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "The postprocessor expression must return String type. Got '{}'",
            outputs.front()->result_type->getName());

    actions.emplace(std::move(actions_dag));
}

std::vector<String> MergeTreeIndexTextPostprocessor::processTokens(std::vector<String> tokens) const
{
    if (!actions || tokens.empty())
        return tokens;

    ColumnString::MutablePtr tokens_col = ColumnString::create();
    tokens_col->reserve(tokens.size());
    for (const String & token : tokens)
        tokens_col->insertData(token.data(), token.size());

    ColumnPtr result = processTokensBatch(tokens_col.get());

    tokens.clear();
    tokens.reserve(result->size());
    for (size_t i = 0; i < result->size(); ++i)
    {
        std::string_view ref = result->getDataAt(i);
        if (!ref.empty())
            tokens.push_back(String{ref.data(), ref.size()});
    }
    return tokens;
}

ColumnPtr MergeTreeIndexTextPostprocessor::processTokensBatch(const ColumnString * tokens) const
{
    if (!actions)
        return tokens->getPtr();

    return executeUnaryExpressionActions(*actions, tokens->getPtr(), string_type, postprocessor_token_name, tokens->size());
}

ColumnPtr MergeTreeIndexTextPostprocessor::processTokensArrayBatch(const ColumnArray * tokens) const
{
    chassert(actions); /// Always called when hasActions() is true.

    const IColumn::Offsets & src_offsets = tokens->getOffsets();
    const size_t num_rows = src_offsets.size();

    /// Apply the postprocessor on all token strings across all rows in one execution.
    const ColumnString * flat_tokens = typeid_cast<const ColumnString *>(tokens->getDataPtr().get());
    chassert(flat_tokens); /// Array(String) data column must be ColumnString
    ColumnPtr flat_transformed = processTokensBatch(flat_tokens);

    /// Rebuild the ColumnArray with updated offsets.
    /// Tokens transformed to empty string are filtered out (e.g. stop words).
    ColumnString::MutablePtr result_data = ColumnString::create();
    result_data->reserve(flat_transformed->size());
    ColumnArray::ColumnOffsets::MutablePtr result_offsets_col = ColumnArray::ColumnOffsets::create();
    result_offsets_col->reserve(num_rows);
    IColumn::Offsets & result_offsets = result_offsets_col->getData();

    size_t flat_idx = 0;
    size_t write_offset = 0;
    for (size_t row = 0; row < num_rows; ++row)
    {
        const size_t row_end = src_offsets[row];
        while (flat_idx < row_end)
        {
            std::string_view ref = flat_transformed->getDataAt(flat_idx);
            if (!ref.empty())
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

ActionsDAG MergeTreeIndexTextPostprocessor::getOriginalActionsDAG(const String & col_name, const DataTypePtr & col_type) const
{
    chassert(actions);

    ASTPtr expr = original_expression_ast->clone();

    if (isArray(col_type))
    {
        /// Wrap element-wise: arrayMap(x -> postprocessor_expr(x), col_name).
        /// Mirrors how MergeTreeIndexTextPreprocessor handles Array index columns.
        replaceExpressionToIdentifier(expr, index_column_name, postprocessor_lambda_arg);
        expr = makeASTFunction("arrayMap",
            makeASTLambda({postprocessor_lambda_arg}, std::move(expr)),
            make_intrusive<ASTIdentifier>(col_name));
        NamesAndTypesList source_columns{{col_name, col_type}};
        return buildActionsDAGFromAST(std::move(expr), source_columns);
    }

    replaceExpressionToIdentifier(expr, index_column_name, col_name);
    NamesAndTypesList source_columns{{col_name, col_type}};
    return buildActionsDAGFromAST(std::move(expr), source_columns);
}
}
