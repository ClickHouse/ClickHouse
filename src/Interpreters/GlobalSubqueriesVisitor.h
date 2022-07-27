#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Databases/IDatabase.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/interpretSubquery.h>
#include <Interpreters/PreparedSets.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/typeid_cast.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int WRONG_GLOBAL_SUBQUERY;
}

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

        Data(
            ContextPtr context_,
            size_t subquery_depth_,
            bool is_remote_,
            bool is_explain_,
            TemporaryTablesMapping & tables,
            PreparedSetsPtr prepared_sets_,
            bool & has_global_subqueries_)
            : WithContext(context_)
            , subquery_depth(subquery_depth_)
            , is_remote(is_remote_)
            , is_explain(is_explain_)
            , external_tables(tables)
            , prepared_sets(prepared_sets_)
            , has_global_subqueries(has_global_subqueries_)
        {
        }

        void addExternalStorage(ASTPtr & ast, bool set_alias = false)
        {
            /// With nondistributed queries, creating temporary tables does not make sense.
            if (!is_remote)
                return;

            bool is_table = false;
            ASTPtr subquery_or_table_name; /// ASTTableIdentifier | ASTSubquery | ASTTableExpression

            if (const auto * ast_table_expr = ast->as<ASTTableExpression>())
            {
                subquery_or_table_name = ast_table_expr->subquery;

                if (ast_table_expr->database_and_table_name)
                {
                    subquery_or_table_name = ast_table_expr->database_and_table_name;
                    is_table = true;
                }
            }
            else if (ast->as<ASTTableIdentifier>())
            {
                subquery_or_table_name = ast;
                is_table = true;
            }
            else if (ast->as<ASTSubquery>())
            {
                subquery_or_table_name = ast;
            }

            if (!subquery_or_table_name)
                throw Exception("Global subquery requires subquery or table name", ErrorCodes::WRONG_GLOBAL_SUBQUERY);

            if (is_table)
            {
                /// If this is already an external table, you do not need to add anything. Just remember its presence.
                auto temporary_table_name = getIdentifierName(subquery_or_table_name);
                bool exists_in_local_map = external_tables.contains(temporary_table_name);
                bool exists_in_context = static_cast<bool>(getContext()->tryResolveStorageID(
                    StorageID("", temporary_table_name), Context::ResolveExternal));
                if (exists_in_local_map || exists_in_context)
                    return;
            }

            String alias = subquery_or_table_name->tryGetAlias();
            String external_table_name;
            if (alias.empty())
            {
                auto hash = subquery_or_table_name->getTreeHash();
                external_table_name = fmt::format("_data_{}_{}", hash.first, hash.second);
            }
            else
                external_table_name = alias;

            /** We replace the subquery with the name of the temporary table.
                * It is in this form, the request will go to the remote server.
                * This temporary table will go to the remote server, and on its side,
                *  instead of doing a subquery, you just need to read it.
                *  TODO We can do better than using alias to name external tables
                */

            auto database_and_table_name = std::make_shared<ASTTableIdentifier>(external_table_name);
            if (set_alias)
            {
                if (auto * table_name = subquery_or_table_name->as<ASTTableIdentifier>())
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

            if (external_tables.contains(external_table_name))
                return;

            auto interpreter = interpretSubquery(subquery_or_table_name, getContext(), subquery_depth, {});

            Block sample = interpreter->getSampleBlock();
            NamesAndTypesList columns = sample.getNamesAndTypesList();

            auto external_storage_holder = std::make_shared<TemporaryTableHolder>(
                getContext(),
                ColumnsDescription{columns},
                ConstraintsDescription{},
                nullptr,
                /*create_for_global_subquery*/ true);
            StoragePtr external_storage = external_storage_holder->getTable();

            external_tables.emplace(external_table_name, external_storage_holder);

            /// We need to materialize external tables immediately because reading from distributed
            /// tables might generate local plans which can refer to external tables during index
            /// analysis. It's too late to populate the external table via CreatingSetsTransform.
            if (is_explain)
            {
                /// Do not materialize external tables if it's explain statement.
            }
            else if (getContext()->getSettingsRef().use_index_for_in_with_subqueries)
            {
                auto external_table = external_storage_holder->getTable();
                auto table_out = external_table->write({}, external_table->getInMemoryMetadataPtr(), getContext());
                auto io = interpreter->execute();
                io.pipeline.complete(std::move(table_out));
                CompletedPipelineExecutor executor(io.pipeline);
                executor.execute();
            }
            else
            {
                auto & subquery_for_set = prepared_sets->getSubquery(external_table_name);
                subquery_for_set.createSource(*interpreter, external_storage);
            }

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
        return !child->as<ASTSelectQuery>();
    }

private:
    /// GLOBAL IN
    static void visit(ASTFunction & func, ASTPtr &, Data & data)
    {
        if ((data.getContext()->getSettingsRef().prefer_global_in_and_join
             && (func.name == "in" || func.name == "notIn" || func.name == "nullIn" || func.name == "notNullIn"))
            || func.name == "globalIn" || func.name == "globalNotIn" || func.name == "globalNullIn" || func.name == "globalNotNullIn")
        {
            ASTPtr & ast = func.arguments->children[1];

            /// Literal or function can use regular IN.
            /// NOTE: We don't support passing table functions to IN.
            if (ast->as<ASTLiteral>() || ast->as<ASTFunction>())
            {
                if (func.name == "globalIn")
                    func.name = "in";
                else if (func.name == "globalNotIn")
                    func.name = "notIn";
                else if (func.name == "globalNullIn")
                    func.name = "nullIn";
                else if (func.name == "globalNotNullIn")
                    func.name = "notNullIn";
                return;
            }

            data.addExternalStorage(ast);
            data.has_global_subqueries = true;
        }
    }

    /// GLOBAL JOIN
    static void visit(ASTTablesInSelectQueryElement & table_elem, ASTPtr &, Data & data)
    {
        if (table_elem.table_join
            && (table_elem.table_join->as<ASTTableJoin &>().locality == ASTTableJoin::Locality::Global
                || data.getContext()->getSettingsRef().prefer_global_in_and_join))
        {
            data.addExternalStorage(table_elem.table_expression, true);
            data.has_global_subqueries = true;
        }
    }
};

/// Converts GLOBAL subqueries to external tables; Puts them into the external_tables dictionary: name -> StoragePtr.
using GlobalSubqueriesVisitor = InDepthNodeVisitor<GlobalSubqueriesMatcher, false>;

}
