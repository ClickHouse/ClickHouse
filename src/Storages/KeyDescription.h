#pragma once

#include <optional>
#include <vector>

#include <Parsers/IAST_fwd.h>

#include <Interpreters/ExpressionActions.h>

#include <Storages/ColumnsDescription.h>

namespace DB
{

/// Common structure for primary, partition and other storage keys
struct KeyDescription
{
    struct AdditionalSettings
    {
        /// Columns that will be added before and after key definition.
        /// Don't stored in definition_ast, but added to expression_list_ast and all its derivatives.
        std::optional<std::vector<String>> ext_columns_front;
        std::optional<std::vector<String>> ext_columns_back;
    };

    class Builder
    {
    public:
        explicit Builder(std::optional<AdditionalSettings> settings_ = {});

        /// settings extenders
        Builder & withExtFrontColumns(std::vector<String> ext_columns_front_);
        Builder & withExtBackColumns(std::vector<String> ext_columns_back_);

        /// Builds an empty key description.
        /// It's different from the default constructor with some additional initializations.
        KeyDescription buildEmpty();

        /// Parses key structure from key definition.
        /// Requires all columns, available in storage.
        KeyDescription buildFromAST(const ASTPtr & definition_ast_, const ColumnsDescription & columns_, ContextPtr context_);

        /// Parses description from string
        KeyDescription buildFromString(const String & str_, const ColumnsDescription & columns_, ContextPtr context_);

    private:
        std::optional<AdditionalSettings> settings;
    };

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

    /// Additional key columns settings added by storage type.
    /// Never changes after initialization with non empty value.
    std::optional<AdditionalSettings> additional_settings;

    /// Recalculate all expressions and fields for key with new columns without
    /// changes in constant fields. Just wrapper for static methods.
    void recalculateWithNewColumns(const ColumnsDescription & new_columns, ContextPtr context);

    /// Recalculate all expressions and fields for key with new ast without
    /// changes in constant fields. Just wrapper for static methods.
    void recalculateWithNewAST(const ASTPtr & new_ast, const ColumnsDescription & columns, ContextPtr context);

    KeyDescription() = default;

    /// We need custom copy constructors because we don't want
    /// unintentionaly share AST variables and modify them.
    KeyDescription(const KeyDescription & other);
    KeyDescription & operator=(const KeyDescription & other);

    /// Substitute modulo with moduloLegacy. Used in KeyCondition to allow proper comparison with keys.
    static bool moduloToModuloLegacyRecursive(ASTPtr node_expr);
};

}
