#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Parsers/IAST.h>

#include <utility>
#include <memory>

namespace DB
{

class Context;
class ASTSelectQuery;
struct DatabaseAndTableWithAlias;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct JoinedColumn
{
    /// Column will be joined to block.
    NameAndTypePair name_and_type;
    /// original column name from joined source.
    String original_name;

    JoinedColumn(NameAndTypePair name_and_type_, String original_name_)
            : name_and_type(std::move(name_and_type_)), original_name(std::move(original_name_)) {}

    bool operator==(const JoinedColumn & o) const
    {
        return name_and_type == o.name_and_type && original_name == o.original_name;
    }
};

using JoinedColumnsList = std::list<JoinedColumn>;

struct AnalyzedJoin
{

    /// NOTE: So far, only one JOIN per query is supported.

    /** Query of the form `SELECT expr(x) AS k FROM t1 ANY LEFT JOIN (SELECT expr(x) AS k FROM t2) USING k`
      * The join is made by column k.
      * During the JOIN,
      *  - in the "right" table, it will be available by alias `k`, since `Project` action for the subquery was executed.
      *  - in the "left" table, it will be accessible by the name `expr(x)`, since `Project` action has not been executed yet.
      * You must remember both of these options.
      *
      * Query of the form `SELECT ... from t1 ANY LEFT JOIN (SELECT ... from t2) ON expr(t1 columns) = expr(t2 columns)`
      *     to the subquery will be added expression `expr(t2 columns)`.
      * It's possible to use name `expr(t2 columns)`.
      */
    Names key_names_left;
    Names key_names_right; /// Duplicating names are qualified.
    ASTs key_asts_left;
    ASTs key_asts_right;

    /// All columns which can be read from joined table. Duplicating names are qualified.
    JoinedColumnsList columns_from_joined_table;
    /// Columns from joined table which may be added to block.
    /// It's columns_from_joined_table without duplicate columns and possibly modified types.
    JoinedColumnsList available_joined_columns;

    void addUsingKey(const ASTPtr & ast);
    void addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast);

    ExpressionActionsPtr createJoinedBlockActions(
        const JoinedColumnsList & columns_added_by_join, /// Subset of available_joined_columns.
        const ASTSelectQuery * select_query_with_join,
        const Context & context) const;

    Names getOriginalColumnNames(const NameSet & required_columns) const;

    void calculateColumnsFromJoinedTable(const NamesAndTypesList & columns, const Names & original_names);
    void calculateAvailableJoinedColumns(bool make_nullable);
};

struct ASTTableExpression;
NamesAndTypesList getNamesAndTypeListFromTableExpression(const ASTTableExpression & table_expression, const Context & context);

}
