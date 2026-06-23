#include <Storages/MergeTree/MergeTreeIndexTextPostprocessor.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ITokenizer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
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
/// Lambda argument used when tokenizing each element of an Array column in the row-level fallback.
constexpr char postprocessor_element_arg[] = "__text_index_element";

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

VectorWithMemoryTracking<String> MergeTreeIndexTextPostprocessor::processTokens(VectorWithMemoryTracking<String> tokens) const
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

    /// Apply the postprocessor on all token strings across all rows in one execution.
    const ColumnString * flat_tokens = typeid_cast<const ColumnString *>(tokens->getDataPtr().get());
    chassert(flat_tokens); /// Array(String) data column must be ColumnString
    ColumnPtr flat_transformed = processTokensBatch(flat_tokens);

    /// The transform maps each token 1:1, so the original offsets still apply and can be reused.
    /// Tokens transformed to empty string (e.g. stop words) are skipped in addDocumentsFromArray.
    return ColumnArray::create(flat_transformed->convertToFullColumnIfConst(), tokens->getOffsetsPtr());
}

ActionsDAG MergeTreeIndexTextPostprocessor::getOriginalActionsDAG(
    const String & col_name, const DataTypePtr & col_type, const String & tokenizer_description) const
{
    chassert(actions);

    ASTPtr expr = original_expression_ast->clone();
    replaceExpressionToIdentifier(expr, index_column_name, postprocessor_lambda_arg);

    /// Build the token stream the postprocessor maps over so that it matches the index-build path
    /// (tokenize first, then postprocess each token). Two cases:
    ///   - Array column: tokenize every element and flatten, mirroring tokenizeToArray which runs the
    ///     tokenizer per element. For the 'array' tokenizer this keeps each element as a single token; for
    ///     any other tokenizer it splits multi-token elements (e.g. 'foo bar' -> 'foo', 'bar').
    ///   - Non-array column: tokenize the whole value with tokens(col, '<tokenizer>').
    /// tokens always yields String tokens (normalizing FixedString elements to String to match the build
    /// path and the postprocessor validation) and drops empty tokens, so an empty element never reaches the
    /// postprocessor and cannot fabricate a token the index never stored.
    ASTPtr tokens_ast = make_intrusive<ASTIdentifier>(col_name);
    if (isArray(col_type))
    {
        tokens_ast = makeASTFunction("arrayFlatten",
            makeASTFunction("arrayMap",
                makeASTLambda({postprocessor_element_arg},
                    makeASTFunction("tokens",
                        make_intrusive<ASTIdentifier>(postprocessor_element_arg),
                        make_intrusive<ASTLiteral>(Field(tokenizer_description)))),
                std::move(tokens_ast)));
    }
    else
    {
        tokens_ast = makeASTFunction("tokens", std::move(tokens_ast), make_intrusive<ASTLiteral>(Field(tokenizer_description)));
    }

    /// arrayMap(x -> postprocessor(x), <tokens>)
    expr = makeASTFunction("arrayMap",
        makeASTLambda({postprocessor_lambda_arg}, std::move(expr)),
        std::move(tokens_ast));

    NamesAndTypesList source_columns{{col_name, col_type}};
    return buildActionsDAGFromAST(std::move(expr), source_columns);
}
}
