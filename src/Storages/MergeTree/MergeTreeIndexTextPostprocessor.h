#pragma once

#include <optional>
#include <Columns/ColumnArray.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Columns/ColumnString.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct IndexDescription;

/// Postprocessor for text index tokens.
/// Applies a user-defined expression to each output token after tokenization.
/// Unlike the preprocessor which operates on entire column values before tokenization,
/// the postprocessor is always applied to output String tokens.
class MergeTreeIndexTextPostprocessor
{
public:
    MergeTreeIndexTextPostprocessor(ASTPtr expression_ast, const IndexDescription & index_description);

    /// Applies the postprocessor to all tokens in one batch execution.
    /// Tokens mapped to an empty string are removed.
    /// If no expression was provided, returns the tokens unchanged.
    VectorWithMemoryTracking<String> processTokens(VectorWithMemoryTracking<String> tokens) const;

    /// Processes a flat ColumnString in which each row holds a single token
    /// (tokens of all documents are concatenated into one column, in order).
    /// The postprocessor expression is applied to the whole column in a single
    /// vectorized ExpressionActions execution. Returns a new ColumnString of
    /// transformed tokens, one per row, in the same order as the input.
    /// Preferred over processTokens because it amortizes expression-execution
    /// overhead over all tokens rather than paying it once per token.
    ColumnPtr processTokensBatch(const ColumnString * tokens) const;

    /// Processes a ColumnArray(String) where each row is an array of tokens for one document.
    /// The postprocessor expression is applied to all elements across the whole column in a
    /// single execution. Tokens mapped to an empty string by the postprocessor are dropped from
    /// their respective arrays. Returns a new ColumnArray(String) with updated offsets.
    ColumnPtr processTokensArrayBatch(const ColumnArray * tokens) const;

    bool hasActions() const { return actions.has_value(); }

    /// True when the expression only ever maps a token to itself or to the empty string, i.e. it is a pure
    /// filter (drop some tokens, never change a token's bytes) - e.g. stop-word `if(token IN (...), '', token)`
    /// or length `if(lengthUTF8(token) < N, '', token)`. Such postprocessors can be applied to the distinct
    /// tokens after a plain streaming build instead of to every occurrence; see the granule builder fast path.
    bool isFilterOnly() const { return is_filter_only; }

    /// Returns an ActionsDAG rewriting a haystack column into the Array(String) of postprocessed tokens
    /// the index stores: arrayMap(x -> postprocessor(x), tokens(col, '<tokenizer>')). Array(String)
    /// index columns are mapped directly (elements are already tokens). Only call when hasActions().
    ActionsDAG getOriginalActionsDAG(const String & col_name, const DataTypePtr & col_type, const String & tokenizer_description) const;

private:
    std::optional<ExpressionActions> actions;
    ASTPtr original_expression_ast;   ///< original AST before token-placeholder substitution
    String index_column_name;         ///< name of the index column in the original expression
    /// Cached to avoid repeated make_shared<DataTypeString>() allocations.
    DataTypePtr string_type;
    /// True when the expression maps every token to itself or the empty string (a pure filter).
    bool is_filter_only = false;
};

}
