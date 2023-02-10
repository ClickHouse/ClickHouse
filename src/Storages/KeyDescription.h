#pragma once

#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{
/// Common structure for primary, partition and other storage keys
struct KeyDescription
{
    /// User defined AST in CREATE/ALTER query. This field may be empty, but key
    /// can exists because some of them maybe set implicitly (for example,
    /// primary key in merge tree can be part of sorting key)
    ASTPtr definition_ast;

    /// ASTExpressionList with key fields, example: (x, toStartOfMonth(date))).
    ASTPtr expression_list_ast;

    /// Expression from expression_list_ast created by ExpressionAnalyzer. Useful,
    /// when you need to get required columns for key, example: a, date, b.
    ExpressionActionsPtr expression;

    /// Sample block with key columns (names, types, empty column)
    Block sample_block;

    /// Column names in key definition, example: x, toStartOfMonth(date), a * b.
    Names column_names;

    /// Types from sample block ordered in columns order.
    DataTypes data_types;

    /// Additional key column added by storage type. Never changes after
    /// initialization with non empty value. Doesn't stored in definition_ast,
    /// but added to expression_list_ast and all its derivatives.
    std::optional<String> additional_column;

    /// Parse key structure from key definition. Requires all columns, available
    /// in storage.
    static KeyDescription getKeyFromAST(
        const ASTPtr & definition_ast,
        const ColumnsDescription & columns,
        ContextPtr context);

    /// Sorting key can contain additional column defined by storage type (like
    /// Version column in VersionedCollapsingMergeTree).
    static KeyDescription getSortingKeyFromAST(
        const ASTPtr & definition_ast,
        const ColumnsDescription & columns,
        ContextPtr context,
        const std::optional<String> & additional_column);

    /// Build an empty key description. It's different from the default constructor with some
    /// additional initializations.
    static KeyDescription buildEmptyKey();

    /// Recalculate all expressions and fields for key with new columns without
    /// changes in constant fields. Just wrapper for static methods.
    void recalculateWithNewColumns(
        const ColumnsDescription & new_columns,
        ContextPtr context);

    /// Recalculate all expressions and fields for key with new ast without
    /// changes in constant fields. Just wrapper for static methods.
    void recalculateWithNewAST(
        const ASTPtr & new_ast,
        const ColumnsDescription & columns,
        ContextPtr context);

    KeyDescription() = default;

    /// We need custom copy constructors because we don't want
    /// unintentionaly share AST variables and modify them.
    KeyDescription(const KeyDescription & other);
    KeyDescription & operator=(const KeyDescription & other);

    /// Substitute modulo with moduloLegacy. Used in KeyCondition to allow proper comparison with keys.
    static bool moduloToModuloLegacyRecursive(ASTPtr node_expr);

    /// Parse description from string
    static KeyDescription parse(const String & str, const ColumnsDescription & columns, ContextPtr context);
};

}
