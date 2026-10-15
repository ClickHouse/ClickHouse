#include <Storages/MergeTree/MergeTreeIndexTextPostprocessor.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Core/Field.h>
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

bool isEmptyStringLiteral(const ASTPtr & ast)
{
    const auto * literal = ast->as<ASTLiteral>();
    return literal && literal->value.getType() == Field::Types::String && literal->value.safeGet<String>().empty();
}

bool isTokenIdentifier(const ASTPtr & ast, std::string_view token_name)
{
    const auto * identifier = ast->as<ASTIdentifier>();
    return identifier && identifier->name() == token_name;
}

/// Collects string literals from an `IN` right-hand side (literal / tuple / array); false if any isn't a string.
bool collectStringLiterals(const ASTPtr & ast, std::vector<String> & out)
{
    auto collect_from_container = [&out](const auto & elements) -> bool
    {
        for (const auto & element : elements)
        {
            if (element.getType() != Field::Types::String)
                return false;
            out.push_back(element.template safeGet<String>());
        }
        return true;
    };

    if (const auto * literal = ast->as<ASTLiteral>())
    {
        const Field & value = literal->value;
        if (value.getType() == Field::Types::String)
        {
            out.push_back(value.safeGet<String>());
            return true;
        }
        if (value.getType() == Field::Types::Tuple)
            return collect_from_container(value.safeGet<Tuple>());
        if (value.getType() == Field::Types::Array)
            return collect_from_container(value.safeGet<Array>());
        return false;
    }

    if (const auto * function = ast->as<ASTFunction>())
    {
        if ((function->name != "tuple" && function->name != "array") || !function->arguments)
            return false;
        for (const auto & child : function->arguments->children)
        {
            if (!collectStringLiterals(child, out))
                return false;
        }
        return true;
    }

    return false;
}

/// Extracts the drop set from `if`/`multiIf(token IN/NOT IN (<literals>), '', token)`; else nullopt.
std::optional<MergeTreeIndexTextInlineFilter> tryExtractInlineFilter(const ASTPtr & ast, std::string_view token_name)
{
    const auto * function = ast->as<ASTFunction>();
    if (!function || !function->arguments || function->arguments->children.size() != 3)
        return {};

    /// `if` and single-WHEN `multiIf` share the same 3-arg (cond, then, else) shape.
    if (function->name != "if" && function->name != "multiIf")
        return {};

    const auto & condition = function->arguments->children[0];
    const auto & then_branch = function->arguments->children[1];
    const auto & else_branch = function->arguments->children[2];

    bool drop_on_condition = false;
    if (isEmptyStringLiteral(then_branch) && isTokenIdentifier(else_branch, token_name))
        drop_on_condition = true;
    else if (isTokenIdentifier(then_branch, token_name) && isEmptyStringLiteral(else_branch))
        drop_on_condition = false;
    else
        return {};

    const auto * in_function = condition->as<ASTFunction>();
    if (!in_function || !in_function->arguments || in_function->arguments->children.size() != 2)
        return {};

    bool is_not_in = false;
    if (in_function->name == "in" || in_function->name == "globalIn")
        is_not_in = false;
    else if (in_function->name == "notIn" || in_function->name == "globalNotIn")
        is_not_in = true;
    else
        return {};

    if (!isTokenIdentifier(in_function->arguments->children[0], token_name))
        return {};

    std::vector<String> literals;
    if (!collectStringLiterals(in_function->arguments->children[1], literals) || literals.empty())
        return {};

    MergeTreeIndexTextInlineFilter filter;
    /// `NOT IN`: invert so `shouldDrop` fires for tokens outside the set.
    filter.drop_on_match = is_not_in ? !drop_on_condition : drop_on_condition;
    filter.tokens.reserve(literals.size());
    for (auto & literal : literals)
        filter.tokens.insert(std::move(literal));
    return filter;
}

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

    /// Fast path: recognize IN/NOT IN filters so the granule builder can decide drops per distinct token.
    inline_filter = tryExtractInlineFilter(transformed_ast, postprocessor_token_name);
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
