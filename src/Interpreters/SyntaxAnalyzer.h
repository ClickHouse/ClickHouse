#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Aliases.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class ASTFunction;
class TableJoin;
class Context;
struct Settings;
struct SelectQueryOptions;
using Scalars = std::map<String, Block>;

struct SyntaxAnalyzerResult
{
    ConstStoragePtr storage;
    std::shared_ptr<TableJoin> analyzed_join;

    NamesAndTypesList source_columns;
    NameSet source_columns_set; /// Set of names of source_columns.
    /// Set of columns that are enough to read from the table to evaluate the expression. It does not include joined columns.
    NamesAndTypesList required_source_columns;

    Aliases aliases;
    std::vector<const ASTFunction *> aggregates;

    /// Which column is needed to be ARRAY-JOIN'ed to get the specified.
    /// For example, for `SELECT s.v ... ARRAY JOIN a AS s` will get "s.v" -> "a.v".
    NameToNameMap array_join_result_to_source;

    /// For the ARRAY JOIN section, mapping from the alias to the full column name.
    /// For example, for `ARRAY JOIN [1,2] AS b` "b" -> "array(1,2)" will enter here.
    /// Note: not used further.
    NameToNameMap array_join_alias_to_name;

    /// The backward mapping for array_join_alias_to_name.
    /// Note: not used further.
    NameToNameMap array_join_name_to_alias;

    /// Predicate optimizer overrides the sub queries
    bool rewrite_subqueries = false;

    /// Results of scalar sub queries
    Scalars scalars;

    bool maybe_optimize_trivial_count = false;

    SyntaxAnalyzerResult(const NamesAndTypesList & source_columns_, ConstStoragePtr storage_ = {}, bool add_special = true)
        : storage(storage_)
        , source_columns(source_columns_)
    {
        collectSourceColumns(add_special);
    }

    void collectSourceColumns(bool add_special);
    void collectUsedColumns(const ASTPtr & query);
    Names requiredSourceColumns() const { return required_source_columns.getNames(); }
    const Scalars & getScalars() const { return scalars; }
};

using SyntaxAnalyzerResultPtr = std::shared_ptr<const SyntaxAnalyzerResult>;

/// AST syntax analysis.
/// Optimises AST tree and collect information for further expression analysis.
/// Result AST has the following invariants:
///  * all aliases are substituted
///  * qualified names are translated
///  * scalar subqueries are executed replaced with constants
///  * unneeded columns are removed from SELECT clause
///  * duplicated columns are removed from ORDER BY, LIMIT BY, USING(...).
/// Motivation:
///  * group most of the AST-changing operations in single place
///  * avoid AST rewriting in ExpressionAnalyzer
///  * decompose ExpressionAnalyzer
class SyntaxAnalyzer
{
public:
    SyntaxAnalyzer(const Context & context_)
        : context(context_)
    {}

    /// Analyze and rewrite not select query
    SyntaxAnalyzerResultPtr analyze(ASTPtr & query, const NamesAndTypesList & source_columns_, ConstStoragePtr storage = {}, bool allow_aggregations = false) const;

    /// Analyze and rewrite select query
    SyntaxAnalyzerResultPtr analyzeSelect(
        ASTPtr & query,
        SyntaxAnalyzerResult && result,
        const SelectQueryOptions & select_options = {},
        const std::vector<TableWithColumnNamesAndTypes> & tables_with_columns = {},
        const Names & required_result_columns = {},
        std::shared_ptr<TableJoin> table_join = {}) const;

private:
    const Context & context;

    static void normalize(ASTPtr & query, Aliases & aliases, const Settings & settings);
};

}
