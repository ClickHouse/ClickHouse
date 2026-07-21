#pragma once

#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct ColumnWithTypeAndName;
struct IndexDescription;

class MergeTreeIndexTextPreprocessor
{
public:
    MergeTreeIndexTextPreprocessor(ASTPtr expression_ast, const IndexDescription & index_description);

    /// Processes n_rows rows of input column, starting at start_row.
    /// The transformation is only applied in the range [start_row, start_row + n_rows)
    /// If the expression is empty this functions is just a no-op.
    /// Returns a pair with the result column and the starting position where results were written.
    std::pair<ColumnPtr, size_t> processColumn(const ColumnWithTypeAndName & column, size_t start_row, size_t n_rows) const;

    /// Applies the preprocessor expression to a constant string.
    String processConstant(const String & input) const;

    bool hasActions() const { return !original_actions.getActions().empty(); }
    const ActionsDAG & getOriginalActionsDAG() const { return original_actions.getActionsDAG(); }

    bool isLowerOrUpper() const { return is_lower_or_upper; }

    /// True when the preprocessor can turn a non-NULL source value into NULL (e.g. nullIf(str, '')).
    /// Detected from the constant-input actions, which run on a plain non-nullable String, so a
    /// Nullable output there means the expression itself introduces NULLs rather than just
    /// propagating a Nullable source. The direct-read optimization keys its null map on the source
    /// column, so such a preprocessor is invisible to it and direct read must be disabled.
    bool canIntroduceNull() const { return introduces_null; }

    /// True when the preprocessor strips the source column's nullability, i.e. a Nullable source
    /// yields a non-Nullable effective haystack (e.g. ifNull(str, ''), coalesce(str, ''),
    /// assumeNotNull(str)). Detected from the original actions, which run on the real (possibly
    /// Nullable) source column. When this is true, the rewritten fallback predicate evaluates a
    /// source-NULL row to 0 rather than NULL, so the direct-read null-map wrapper (keyed on the
    /// source null map) must NOT reintroduce NULL for those rows.
    bool removesNull() const { return removes_null; }

private:
    /// True only when the preprocessor is exactly lower/lowerUTF8/upper/upperUTF8 applied
    /// directly to the index column (no nested transformations).
    bool is_lower_or_upper = false;
    /// True when applying the preprocessor to a non-nullable input yields a Nullable output.
    bool introduces_null = false;
    /// True when the source column is Nullable but the preprocessor's effective output is not,
    /// i.e. the preprocessor removes the source nullability.
    bool removes_null = false;
    /// The name of the column on which the index is defined.
    String index_column_name;
    /// The type of the column on which the index is defined.
    DataTypePtr index_column_type;
    /// The original expression actions that executes the preprocessor expression
    /// and the index expression from the required index column.
    ExpressionActions original_actions;
    /// The expression actions that executes the preprocessor expression on top the ready index column.
    ExpressionActions actions_for_index_column;
    /// The expression actions that executes the preprocessor expression on top the constant string with needles.
    ExpressionActions actions_for_constant;
};

}
