#pragma once

namespace DB
{

/// Converts GLOBAL subqueries to external tables; Puts them into the external_tables dictionary: name -> StoragePtr.
class GlobalSubqueriesVisitor
{
public:
    GlobalSubqueriesVisitor(const Context & context_, size_t subquery_depth_, bool is_remote_,
                            Tables & tables, SubqueriesForSets & subqueries_for_sets_, bool & has_global_subqueries_)
    :   context(context_),
        subquery_depth(subquery_depth_),
        is_remote(is_remote_),
        external_table_id(1),
        external_tables(tables),
        subqueries_for_sets(subqueries_for_sets_),
        has_global_subqueries(has_global_subqueries_)
    {}

    void visit(ASTPtr & ast) const
    {
        /// Recursive calls. We do not go into subqueries.
        for (auto & child : ast->children)
            if (!typeid_cast<ASTSelectQuery *>(child.get()))
                visit(child);

        /// Bottom-up actions.
        if (tryVisit<ASTFunction>(ast) ||
            tryVisit<ASTTablesInSelectQueryElement>(ast))
        {}
    }

private:
    const Context & context;
    size_t subquery_depth;
    bool is_remote;
    mutable size_t external_table_id = 1;
    Tables & external_tables;
    SubqueriesForSets & subqueries_for_sets;
    bool & has_global_subqueries;

    /// GLOBAL IN
    void visit(ASTFunction & func, ASTPtr &) const
    {
        if (func.name == "globalIn" || func.name == "globalNotIn")
        {
            addExternalStorage(func.arguments->children.at(1));
            has_global_subqueries = true;
        }
    }

    /// GLOBAL JOIN
    void visit(ASTTablesInSelectQueryElement & table_elem, ASTPtr &) const
    {
        if (table_elem.table_join
            && static_cast<const ASTTableJoin &>(*table_elem.table_join).locality == ASTTableJoin::Locality::Global)
        {
            addExternalStorage(table_elem.table_expression);
            has_global_subqueries = true;
        }
    }

    template <typename T>
    bool tryVisit(ASTPtr & ast) const
    {
        if (T * t = typeid_cast<T *>(ast.get()))
        {
            visit(*t, ast);
            return true;
        }
        return false;
    }

    void addExternalStorage(ASTPtr & subquery_or_table_name_or_table_expression) const
    {
        /// With nondistributed queries, creating temporary tables does not make sense.
        if (!is_remote)
            return;

        ASTPtr subquery;
        ASTPtr table_name;
        ASTPtr subquery_or_table_name;

        if (typeid_cast<const ASTIdentifier *>(subquery_or_table_name_or_table_expression.get()))
        {
            table_name = subquery_or_table_name_or_table_expression;
            subquery_or_table_name = table_name;
        }
        else if (auto ast_table_expr = typeid_cast<const ASTTableExpression *>(subquery_or_table_name_or_table_expression.get()))
        {
            if (ast_table_expr->database_and_table_name)
            {
                table_name = ast_table_expr->database_and_table_name;
                subquery_or_table_name = table_name;
            }
            else if (ast_table_expr->subquery)
            {
                subquery = ast_table_expr->subquery;
                subquery_or_table_name = subquery;
            }
        }
        else if (typeid_cast<const ASTSubquery *>(subquery_or_table_name_or_table_expression.get()))
        {
            subquery = subquery_or_table_name_or_table_expression;
            subquery_or_table_name = subquery;
        }

        if (!subquery_or_table_name)
            throw Exception("Logical error: unknown AST element passed to ExpressionAnalyzer::addExternalStorage method",
                            ErrorCodes::LOGICAL_ERROR);

        if (table_name)
        {
            /// If this is already an external table, you do not need to add anything. Just remember its presence.
            if (external_tables.end() != external_tables.find(static_cast<const ASTIdentifier &>(*table_name).name))
                return;
        }

        /// Generate the name for the external table.
        String external_table_name = "_data" + toString(external_table_id);
        while (external_tables.count(external_table_name))
        {
            ++external_table_id;
            external_table_name = "_data" + toString(external_table_id);
        }

        auto interpreter = interpretSubquery(subquery_or_table_name, context, subquery_depth, {});

        Block sample = interpreter->getSampleBlock();
        NamesAndTypesList columns = sample.getNamesAndTypesList();

        StoragePtr external_storage = StorageMemory::create(external_table_name, ColumnsDescription{columns});
        external_storage->startup();

        /** We replace the subquery with the name of the temporary table.
            * It is in this form, the request will go to the remote server.
            * This temporary table will go to the remote server, and on its side,
            *  instead of doing a subquery, you just need to read it.
            */

        auto database_and_table_name = createDatabaseAndTableNode("", external_table_name);

        if (auto ast_table_expr = typeid_cast<ASTTableExpression *>(subquery_or_table_name_or_table_expression.get()))
        {
            ast_table_expr->subquery.reset();
            ast_table_expr->database_and_table_name = database_and_table_name;

            ast_table_expr->children.clear();
            ast_table_expr->children.emplace_back(database_and_table_name);
        }
        else
            subquery_or_table_name_or_table_expression = database_and_table_name;

        external_tables[external_table_name] = external_storage;
        subqueries_for_sets[external_table_name].source = interpreter->execute().in;
        subqueries_for_sets[external_table_name].table = external_storage;

        /** NOTE If it was written IN tmp_table - the existing temporary (but not external) table,
        *  then a new temporary table will be created (for example, _data1),
        *  and the data will then be copied to it.
        * Maybe this can be avoided.
        */
    }
};

}
