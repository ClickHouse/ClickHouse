#pragma once

#include <Interpreters/AnalyzedJoin.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

struct SyntaxAnalyzerResult
{
    StoragePtr storage;

    /// Note: used only in tests.
    using Aliases = std::unordered_map<String, ASTPtr>;
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
    SyntaxAnalyzer(const Context & context, StoragePtr storage) : context(context), storage(std::move(storage)) {}

    SyntaxAnalyzerResultPtr analyze(
        ASTPtr & query,
        NamesAndTypesList & source_columns,
        const Names & required_result_columns = {},
        size_t subquery_depth = 0) const;

    const Context & context;
    StoragePtr storage;
};

}
