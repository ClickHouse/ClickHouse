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

struct AnalyzedJoin
{
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

private:
    friend class SyntaxAnalyzer;
    friend class ExpressionAnalyzer;

    Names key_names_left;
    Names key_names_right; /// Duplicating names are qualified.
    ASTs key_asts_left;
    ASTs key_asts_right;
    bool with_using = true;

    /// All columns which can be read from joined table. Duplicating names are qualified.
    NamesAndTypesList columns_from_joined_table;
    /// Columns from joined table which may be added to block. It's columns_from_joined_table with possibly modified types.
    NamesAndTypesList available_joined_columns;
    /// Name -> original name. Names are the same as in columns_from_joined_table list.
    std::unordered_map<String, String> original_names;
    /// Original name -> name. Only ranamed columns.
    std::unordered_map<String, String> renames;

public:
    void addUsingKey(const ASTPtr & ast);
    void addOnKeys(ASTPtr & left_table_ast, ASTPtr & right_table_ast);

    ExpressionActionsPtr createJoinedBlockActions(
        const NamesAndTypesList & columns_added_by_join, /// Subset of available_joined_columns.
        const ASTSelectQuery * select_query_with_join,
        const Context & context) const;

    NameSet getQualifiedColumnsSet() const;
    NameSet getOriginalColumnsSet() const;
    std::unordered_map<String, String> getOriginalColumnsMap(const NameSet & required_columns) const;

    void deduplicateAndQualifyColumnNames(const NameSet & left_table_columns, const String & right_table_prefix);
    void calculateAvailableJoinedColumns(bool make_nullable);
    size_t rightKeyInclusion(const String & name) const;
};

struct ASTTableExpression;
NamesAndTypesList getNamesAndTypeListFromTableExpression(const ASTTableExpression & table_expression, const Context & context);

}
