#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class ASTSelectQuery;
class ASTFunction;
struct ASTTablesInSelectQueryElement;
class TableJoin;

struct TemporaryTableHolder;
using TemporaryTablesMapping = std::map<String, std::shared_ptr<TemporaryTableHolder>>;

class PreparedSets;
using PreparedSetsPtr = std::shared_ptr<PreparedSets>;

class GlobalSubqueriesMatcher
{
public:
    struct Data : WithContext
    {
        size_t subquery_depth;
        bool is_remote;
        bool is_explain;
        TemporaryTablesMapping & external_tables;
        PreparedSetsPtr prepared_sets;
        bool & has_global_subqueries;
        TableJoin * table_join;

        Data(
            ContextPtr context_,
            size_t subquery_depth_,
            bool is_remote_,
            bool is_explain_,
            TemporaryTablesMapping & tables,
            PreparedSetsPtr prepared_sets_,
            bool & has_global_subqueries_,
            TableJoin * table_join_)
            : WithContext(context_)
            , subquery_depth(subquery_depth_)
            , is_remote(is_remote_)
            , is_explain(is_explain_)
            , external_tables(tables)
            , prepared_sets(prepared_sets_)
            , has_global_subqueries(has_global_subqueries_)
            , table_join(table_join_)
        {
        }

        void addExternalStorage(ASTPtr & ast, const Names & required_columns, bool set_alias = false);
    };

    static void visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = ast->as<ASTFunction>())
            visit(*t, ast, data);
        if (auto * t = ast->as<ASTTablesInSelectQueryElement>())
            visit(*t, ast, data);
    }

    static bool needChildVisit(ASTPtr &, const ASTPtr & child)
    {
        /// We do not go into subqueries.
        return !child->as<ASTSelectQuery>();
    }

private:
    /// GLOBAL IN
    static void visit(ASTFunction & func, ASTPtr &, Data & data);

    /// GLOBAL JOIN
    static void visit(ASTTablesInSelectQueryElement & table_elem, ASTPtr &, Data & data);
};

/// Converts GLOBAL subqueries to external tables; Puts them into the external_tables dictionary: name -> StoragePtr.
using GlobalSubqueriesVisitor = InDepthNodeVisitor<GlobalSubqueriesMatcher, false>;

}
