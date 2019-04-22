#pragma once

#include <Interpreters/AnalyzedJoin.h>
#include <Interpreters/Aliases.h>
#include <Interpreters/SelectQueryOptions.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

NameSet removeDuplicateColumns(NamesAndTypesList & columns);

struct SyntaxAnalyzerResult
{
    StoragePtr storage;

    NamesAndTypesList source_columns;

    Aliases aliases;

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

    AnalyzedJoin analyzed_join;

    /// Predicate optimizer overrides the sub queries
    bool rewrite_subqueries = false;
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
    SyntaxAnalyzer(const Context & context_, const SelectQueryOptions & select_options = {})
        : context(context_)
        , subquery_depth(select_options.subquery_depth)
        , remove_duplicates(select_options.remove_duplicates)
    {}

    SyntaxAnalyzerResultPtr analyze(
        ASTPtr & query,
        const NamesAndTypesList & source_columns_,
        const Names & required_result_columns = {},
        StoragePtr storage = {}) const;

private:
    const Context & context;
    size_t subquery_depth;
    bool remove_duplicates;
};

}
