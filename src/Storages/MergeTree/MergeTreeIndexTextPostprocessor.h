#pragma once

#include <optional>
#include <Core/NamesAndTypes.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct IndexDescription;

/// Postprocessor for text index tokens.
/// Applies a user-defined expression to each individual token after tokenization.
/// Unlike the preprocessor which operates on entire column values before tokenization,
/// the postprocessor is always applied to individual String tokens.
class MergeTreeIndexTextPostprocessor
{
public:
    MergeTreeIndexTextPostprocessor(ASTPtr expression_ast, const IndexDescription & index_description);

    /// Applies the postprocessor to all tokens in one batch execution.
    /// Tokens mapped to an empty string are removed.
    /// If no expression was provided, returns the tokens unchanged.
    std::vector<String> applyBatch(std::vector<String> tokens) const;

    /// Processes a flat ColumnString of tokens in one vectorized ExpressionActions execution.
    /// Returns a new column of transformed tokens. Preferred over processToken for
    /// indexing because it amortizes expression execution overhead over all tokens
    /// in a document rather than paying it once per token.
    ColumnPtr processTokensBatch(ColumnPtr tokens_column) const;

    /// Processes a ColumnArray(String) where each row is an array of tokens for one document.
    /// The postprocessor expression is applied to all elements across the whole column in a
    /// single execution. Tokens mapped to an empty string by the postprocessor are dropped from
    /// their respective arrays. Returns a new ColumnArray(String) with updated offsets.
    ColumnPtr processTokensArrayBatch(ColumnPtr tokens_array_column) const;

    bool hasActions() const { return actions.has_value(); }

private:
    std::optional<ExpressionActions> actions;
    /// Cached to avoid repeated make_shared<DataTypeString>() allocations.
    DataTypePtr string_type;
};

}
