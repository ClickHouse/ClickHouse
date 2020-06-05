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

    /// Parse key structure from key definition. Requires all columns, available
    /// in storage.
    static KeyDescription getKeyFromAST(const ASTPtr & definition_ast, const ColumnsDescription & columns, const Context & context, ASTPtr additional_key_expression = nullptr);

    KeyDescription() = default;

    /// We need custom copy constructors because we don't want
    /// unintentionaly share AST variables and modify them.
    KeyDescription(const KeyDescription & other);
    KeyDescription & operator=(const KeyDescription & other);
};

}
