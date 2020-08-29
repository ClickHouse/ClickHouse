#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/interpretSubquery.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Databases/IDatabase.h>
#include <Storages/StorageMemory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


class GlobalSubqueriesMatcher
{
public:
    struct Data
    {
        const Context & context;
        size_t subquery_depth;
        bool is_remote;
        size_t external_table_id;
        TemporaryTablesMapping & external_tables;
        SubqueriesForSets & subqueries_for_sets;
        bool & has_global_subqueries;

        Data(const Context & context_, size_t subquery_depth_, bool is_remote_,
             TemporaryTablesMapping & tables, SubqueriesForSets & subqueries_for_sets_, bool & has_global_subqueries_)
        :   context(context_),
            subquery_depth(subquery_depth_),
            is_remote(is_remote_),
            external_table_id(1),
            external_tables(tables),
            subqueries_for_sets(subqueries_for_sets_),
            has_global_subqueries(has_global_subqueries_)
        {}

        void addExternalStorage(ASTPtr & ast, bool set_alias = false)
        {
            /// With nondistributed queries, creating temporary tables does not make sense.
            if (!is_remote)
                return;

            bool is_table = false;
            ASTPtr subquery_or_table_name = ast; /// ASTIdentifier | ASTSubquery | ASTTableExpression

            if (const auto * ast_table_expr = ast->as<ASTTableExpression>())
            {
                subquery_or_table_name = ast_table_expr->subquery;

                if (ast_table_expr->database_and_table_name)
                {
                    subquery_or_table_name = ast_table_expr->database_and_table_name;
                    is_table = true;
                }
            }
            else if (ast->as<ASTIdentifier>())
                is_table = true;

            if (!subquery_or_table_name)
                throw Exception("Logical error: unknown AST element passed to ExpressionAnalyzer::addExternalStorage method",
                                ErrorCodes::LOGICAL_ERROR);

            if (is_table)
            {
                /// If this is already an external table, you do not need to add anything. Just remember its presence.
                auto temporary_table_name = getIdentifierName(subquery_or_table_name);
                bool exists_in_local_map = external_tables.end() != external_tables.find(temporary_table_name);
                bool exists_in_context = context.tryResolveStorageID(StorageID("", temporary_table_name), Context::ResolveExternal);
                if (exists_in_local_map || exists_in_context)
                    return;
            }

            String external_table_name = subquery_or_table_name->tryGetAlias();
            if (external_table_name.empty())
            {
                /// Generate the name for the external table.
                external_table_name = "_data" + toString(external_table_id);
                while (external_tables.count(external_table_name))
                {
                    ++external_table_id;
                    external_table_name = "_data" + toString(external_table_id);
                }
            }

            auto interpreter = interpretSubquery(subquery_or_table_name, context, subquery_depth, {});

            Block sample = interpreter->getSampleBlock();
            NamesAndTypesList columns = sample.getNamesAndTypesList();

            auto external_storage_holder = std::make_shared<TemporaryTableHolder>(context, ColumnsDescription{columns}, ConstraintsDescription{});
            StoragePtr external_storage = external_storage_holder->getTable();

            /** We replace the subquery with the name of the temporary table.
                * It is in this form, the request will go to the remote server.
                * This temporary table will go to the remote server, and on its side,
                *  instead of doing a subquery, you just need to read it.
                */

            auto database_and_table_name = createTableIdentifier("", external_table_name);
            if (set_alias)
            {
                String alias = subquery_or_table_name->tryGetAlias();
                if (auto * table_name = subquery_or_table_name->as<ASTIdentifier>())
                    if (alias.empty())
                        alias = table_name->shortName();
                database_and_table_name->setAlias(alias);
            }

            if (auto * ast_table_expr = ast->as<ASTTableExpression>())
            {
                ast_table_expr->subquery.reset();
                ast_table_expr->database_and_table_name = database_and_table_name;

                ast_table_expr->children.clear();
                ast_table_expr->children.emplace_back(database_and_table_name);
            }
            else
                ast = database_and_table_name;

            external_tables[external_table_name] = external_storage_holder;
            subqueries_for_sets[external_table_name].source = interpreter->execute().getInputStream();
            subqueries_for_sets[external_table_name].table = external_storage;

            /** NOTE If it was written IN tmp_table - the existing temporary (but not external) table,
            *  then a new temporary table will be created (for example, _data1),
            *  and the data will then be copied to it.
            * Maybe this can be avoided.
            */
        }
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
        if (child->as<ASTSelectQuery>())
            return false;
        return true;
    }

private:
    /// GLOBAL IN
    static void visit(ASTFunction & func, ASTPtr &, Data & data)
    {
        if (func.name == "globalIn" || func.name == "globalNotIn")
        {
            ASTPtr & ast = func.arguments->children[1];

            /// Literal can use regular IN
            if (ast->as<ASTLiteral>())
            {
                if (func.name == "globalIn")
                    func.name = "in";
                else
                    func.name = "notIn";
                return;
            }

            data.addExternalStorage(ast);
            data.has_global_subqueries = true;
        }
    }

    /// GLOBAL JOIN
    static void visit(ASTTablesInSelectQueryElement & table_elem, ASTPtr &, Data & data)
    {
        if (table_elem.table_join && table_elem.table_join->as<ASTTableJoin &>().locality == ASTTableJoin::Locality::Global)
        {
            data.addExternalStorage(table_elem.table_expression, true);
            data.has_global_subqueries = true;
        }
    }
};

/// Converts GLOBAL subqueries to external tables; Puts them into the external_tables dictionary: name -> StoragePtr.
using GlobalSubqueriesVisitor = InDepthNodeVisitor<GlobalSubqueriesMatcher, false>;

}
