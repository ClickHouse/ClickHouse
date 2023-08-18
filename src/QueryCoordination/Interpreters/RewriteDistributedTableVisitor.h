#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ASTAlterQuery.h>
#include <Interpreters/misc.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Storages/StorageDistributed.h>
//#include <Storages/IStorage.h>


namespace DB
{

class IStorage;

using ConstStoragePtr = std::shared_ptr<const IStorage>;
using StoragePtr = std::shared_ptr<IStorage>;

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;


/// Visitors consist of functions with unified interface 'void visit(Casted & x, ASTPtr & y)', there x is y, successfully casted to Casted.
/// Both types and function could have const specifiers. The second argument is used by visitor to replaces AST node (y) if needed.

/// Visits AST nodes, add default database to tables if not set. There's different logic for DDLs and selects.
class RewriteDistributedTableVisitor
{
public:
    explicit RewriteDistributedTableVisitor(
        ContextPtr context_)
        : context(context_)
    {
    }


    void visit(ASTPtr & ast)
    {
        if (!tryVisit<ASTSelectQuery>(ast) &&
            !tryVisit<ASTSelectWithUnionQuery>(ast) &&
            !tryVisit<ASTFunction>(ast))
            visitChildren(*ast);
    }

    void visit(ASTSelectQuery & select)
    {
        ASTPtr unused;
        visit(select, unused);
    }

    void visit(ASTSelectWithUnionQuery & select)
    {
        ASTPtr unused;
        visit(select, unused);
    }

    void visit(ASTColumns & columns)
    {
        for (auto & child : columns.children)
            visit(child);
    }

    std::vector<StoragePtr> storages;

    std::vector<ClusterPtr> clusters;

    std::vector<String> sharding_key_columns;

    bool has_distributed_table = false;
    bool has_local_table = false;

private:

    ContextPtr context;

    void visit(ASTSelectWithUnionQuery & select, ASTPtr &)
    {
        for (auto & child : select.list_of_selects->children)
        {
            if (child->as<ASTSelectQuery>())
                tryVisit<ASTSelectQuery>(child);
            else if (child->as<ASTSelectIntersectExceptQuery>())
                tryVisit<ASTSelectIntersectExceptQuery>(child);
        }
    }

    void visit(ASTSelectQuery & select, ASTPtr &)
    {
        if (select.tables())
            tryVisit<ASTTablesInSelectQuery>(select.refTables());

        visitChildren(select);
    }

    void visit(ASTSelectIntersectExceptQuery & select, ASTPtr &)
    {
        for (auto & child : select.getListOfSelects())
        {
            if (child->as<ASTSelectQuery>())
                tryVisit<ASTSelectQuery>(child);
            else if (child->as<ASTSelectIntersectExceptQuery>())
                tryVisit<ASTSelectIntersectExceptQuery>(child);
            else if (child->as<ASTSelectWithUnionQuery>())
                tryVisit<ASTSelectWithUnionQuery>(child);
        }
    }

    void visit(ASTTablesInSelectQuery & tables, ASTPtr &)
    {
        for (auto & child : tables.children)
            tryVisit<ASTTablesInSelectQueryElement>(child);
    }

    void visit(ASTTablesInSelectQueryElement & tables_element, ASTPtr &)
    {
        if (tables_element.table_expression)
            tryVisit<ASTTableExpression>(tables_element.table_expression);
    }

    void visit(ASTTableExpression & table_expression, ASTPtr &)
    {
        if (table_expression.database_and_table_name)
            tryVisit<ASTTableIdentifier>(table_expression.database_and_table_name);
//        else if (table_expression.subquery)
//            tryVisit<ASTSubquery>(table_expression.subquery);
    }

    void visit(const ASTTableIdentifier & identifier, ASTPtr & ast)
    {
        StorageID table_id = identifier.getTableId();

        if (!table_id.hasDatabase())
        {
            table_id.database_name = context->getCurrentDatabase();
        }

        auto table = DatabaseCatalog::instance().getTable(table_id, context);
        if (auto * distributed_table = dynamic_cast<StorageDistributed *>(table.get()))
        {
            auto database_name = distributed_table->getRemoteDatabaseName();
            auto table_name = distributed_table->getRemoteTableName();

            auto qualified_identifier = std::make_shared<ASTTableIdentifier>(database_name, table_name);
            auto local_table = DatabaseCatalog::instance().getTable(qualified_identifier->getTableId(), context);

            if (!identifier.alias.empty())
                qualified_identifier->setAlias(identifier.alias);
            ast = qualified_identifier;

            storages.emplace_back(local_table);
            clusters.emplace_back(distributed_table->getCluster());
//            sharding_key_columns.emplace_back(distributed_table->sharding_key); TODO

            has_distributed_table = true;
        }
        else
        {
            has_local_table = true;
        }
    }

    void visit(ASTSubquery & subquery, ASTPtr &)
    {
        tryVisit<ASTSelectWithUnionQuery>(subquery.children[0]);
    }

    void visit(ASTFunction & function, ASTPtr &)
    {
        bool is_operator_in = functionIsInOrGlobalInOperator(function.name);
        bool is_dict_get = functionIsDictGet(function.name);

        for (auto & child : function.children)
        {
            if (child.get() == function.arguments.get())
            {
                for (size_t i = 0; i < child->children.size(); ++i)
                {
                    if (is_dict_get && i == 0)
                    {
                        if (auto * identifier = child->children[i]->as<ASTIdentifier>())
                        {
                            /// Identifier already qualified
                            if (identifier->compound())
                                continue;

                            auto qualified_dictionary_name = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(identifier->name(), context);
                            child->children[i] = std::make_shared<ASTIdentifier>(qualified_dictionary_name.getParts());
                        }
                        else if (auto * literal = child->children[i]->as<ASTLiteral>())
                        {
                            auto & literal_value = literal->value;

                            if (literal_value.getType() != Field::Types::String)
                                continue;

                            auto dictionary_name = literal_value.get<String>();
                            auto qualified_dictionary_name = context->getExternalDictionariesLoader().qualifyDictionaryNameWithDatabase(dictionary_name, context);
                            literal_value = qualified_dictionary_name.getFullName();
                        }
                    }
                    else if (is_operator_in && i == 1)
                    {
                        /// XXX: for some unknown reason this place assumes that argument can't be an alias,
                        ///      like in the similar code in `MarkTableIdentifierVisitor`.
                        if (auto * identifier = child->children[i]->as<ASTIdentifier>())
                        {
                            /// If identifier is broken then we can do nothing and get an exception
                            auto maybe_table_identifier = identifier->createTable();
                            if (maybe_table_identifier)
                                child->children[i] = maybe_table_identifier;
                        }

                        /// Second argument of the "in" function (or similar) may be a table name or a subselect.
                        /// Rewrite the table name or descend into subselect.
                        if (!tryVisit<ASTTableIdentifier>(child->children[i]))
                            visit(child->children[i]);
                    }
                    else
                    {
                        visit(child->children[i]);
                    }
                }
            }
            else
            {
                visit(child);
            }
        }
    }

    void visitChildren(IAST & ast)
    {
        for (auto & child : ast.children)
            visit(child);
    }

    template <typename T>
    bool tryVisit(ASTPtr & ast)
    {
        if (T * t = typeid_cast<T *>(ast.get()))
        {
            visit(*t, ast);
            return true;
        }
        return false;
    }

};

}

