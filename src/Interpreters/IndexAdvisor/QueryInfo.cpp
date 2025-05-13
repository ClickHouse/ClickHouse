#include "QueryInfo.h"
#include "CollectTablesMatcher.h"
#include <Core/Defines.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/parseQuery.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

QueryInfo::QueryInfo(const String & path, ContextMutablePtr context_)
    : context(context_)
{
    readQueries(path);
    for (const auto & query : queries)
    {
        // LOG_INFO(getLogger("QueryInfo"), "Processing query: {}", query);
        parseColumnsFromQuery(query);
    }
    for (const auto & q : drop_view_queries)
    {
        try
        {
            executeQuery(q, context, QueryFlags{ .internal = true });
            // LOG_INFO(getLogger("QueryInfo"), "Successfully executed DROP VIEW: {}", q);
        }
        catch (const Exception & e)
        {
            LOG_INFO(getLogger("QueryInfo"), "Failed to execute DROP VIEW: {}, error: {}", q, e.message());
        }
    }
}

void QueryInfo::readQueries(const String & path)
{
    queries.clear();
    views.clear();
    drop_view_queries.clear();
    ReadBufferFromFile in{path};
    String file_contents;
    readStringUntilEOF(file_contents, in);

    size_t pos = 0;
    std::vector<String> create_view_queries;

    while (pos < file_contents.size())
    {
        size_t next = file_contents.find(';', pos);
        if (next == String::npos)
            next = file_contents.size();

        String query = file_contents.substr(pos, next - pos);
        size_t size = query.size();
        while (size > 0 && (query[size - 1] == ' ' || query[size - 1] == '\t' || query[size - 1] == '\n' || query[size - 1] == '\r'))
            --size;

        query.resize(size);
        if (!query.empty())
        {
            String upper_query = query;
            std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);
            
            if (upper_query.find("CREATE VIEW") != String::npos)
            {
                // LOG_INFO(getLogger("QueryInfo"), "Found CREATE VIEW query: {}", query);
                create_view_queries.push_back(query);
                queries.push_back(query);  // Add to queries to parse its contents
            }
            else if (upper_query.find("DROP VIEW") != String::npos)
            {
                // LOG_INFO(getLogger("QueryInfo"), "Found DROP VIEW query: {}", query);
                drop_view_queries.push_back(query);
                queries.push_back(query);  // Add to queries to parse its contents
            }
            else
            {
                // LOG_INFO(getLogger("QueryInfo"), "Found regular query: {}", query);
                queries.push_back(query);
            }
        }
        pos = next + 1;
    }

    for (const auto & query : create_view_queries)
    {
        try
        {
            executeQuery(query, context, QueryFlags{ .internal = true });
            // LOG_INFO(getLogger("QueryInfo"), "Successfully executed CREATE VIEW: {}", query);
        }
        catch (const Exception & e)
        {
            LOG_INFO(getLogger("QueryInfo"), "Failed to execute CREATE VIEW: {}, error: {}", query, e.message());
        }
    }
}

void QueryInfo::parseColumnsFromQuery(const String & query)
{
    // LOG_INFO(getLogger("QueryInfo"), "Parsing query: {}", query);
    
    ParserQuery parser(query.data() + query.size());
    ASTPtr ast;
    try
    {
        ast = parseQuery(parser, query, "", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }
    catch (const Exception &)
    {
        LOG_INFO(getLogger("QueryInfo"), "Failed to parse query: {}", query);
        return;
    }

    if (!ast)
    {
        LOG_INFO(getLogger("QueryInfo"), "Failed to parse query: {}", query);
        return;
    }

    if (const auto * create_query = ast->as<ASTCreateQuery>())
    {
        if (create_query->isView())
        {
            String view_name = create_query->getTable();
            if (!view_name.empty())
            {
                views.push_back(view_name);
                LOG_INFO(getLogger("QueryInfo"), "Found view name: {}", view_name);
            }

            if (create_query->select)
            {
                CollectTablesMatcher::Data tables_data;
                ASTPtr select_ast = create_query->select->ptr();
                collectTables(select_ast, tables_data);
                
                processTablesAndColumns(tables_data, nullptr, select_ast);
            }
            return;
        }
    }

    if (const auto * select_with_union = ast->as<ASTSelectWithUnionQuery>())
    {
        std::set<String> cte_names;
        for (const auto & child : select_with_union->list_of_selects->children)
        {
            if (const auto * select_query = child->as<ASTSelectQuery>())
            {
                if (select_query->with())
                {
                    for (const auto & with_element : select_query->with()->children)
                    {
                        if (const auto * cte = with_element->as<ASTWithElement>())
                        {
                            cte_names.insert(cte->name);
                        }
                    }
                }
            }
        }

        CollectTablesMatcher::Data tables_data;
        ASTPtr query_ast = ast;
        collectTables(query_ast, tables_data);
        
        processTablesAndColumns(tables_data, &cte_names, query_ast);
    }
}

void QueryInfo::processTablesAndColumns(const CollectTablesMatcher::Data & tables_data, const std::set<String> * cte_names, const ASTPtr & ast)
{
    std::unordered_map<String, std::set<String>> table_columns;
    
    std::unordered_map<String, std::set<String>> table_metadata_cache;
    
    for (const auto & table : tables_data.tables)
    {
        if (cte_names && cte_names->contains(table))
            continue;

        // LOG_INFO(getLogger("QueryInfo"), "Processing table: {}", table);
        
        StoragePtr storage = DatabaseCatalog::instance().getTable({context->getCurrentDatabase(), table}, context);
        
        if (!storage)
        {
            auto databases = DatabaseCatalog::instance().getDatabases();
            for (const auto & [db_name, db] : databases)
            {
                if (db_name == context->getCurrentDatabase())
                    continue;
                    
                storage = DatabaseCatalog::instance().getTable({db_name, table}, context);
                if (storage)
                    break;
            }
        }

        if (!storage)
        {
            // LOG_INFO(getLogger("QueryInfo"), "Table {} not found in any database, skipping", table);
            continue;
        }

        if (storage->isView())
        {
            // LOG_INFO(getLogger("QueryInfo"), "Skipping view: {}", table);
            continue;
        }

        auto metadata = storage->getInMemoryMetadataPtr();
        auto columns = metadata->getColumns().getNamesOfPhysical();
        table_metadata_cache[table] = std::set<String>(columns.begin(), columns.end());
        
        table_columns[table] = {};
    }

    auto idents = IdentifiersCollector::collect(ast);
    
    for (const auto * ident : idents)
    {
        // LOG_INFO(getLogger("QueryInfo"), "Processing identifier: {}", ident->getColumnName());
        if (auto column_name = IdentifierSemantic::getColumnName(*ident))
        {
            for (const auto & [table, columns] : table_metadata_cache)
            {
                if (columns.contains(*column_name))
                {
                    table_columns[table].insert(*column_name);
                    // LOG_INFO(getLogger("QueryInfo"), "Found column {}.{}", table, *column_name);
                    break;
                }
            }
        }
    }

    for (const auto & [table, columns] : table_columns)
    {
        Strings cols_vec(columns.begin(), columns.end());
        tables_to_columns[table].insert(tables_to_columns[table].end(), cols_vec.begin(), cols_vec.end());
    }
}

}
