#include "QueryInfo.h"
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Core/Defines.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/IndexAdvisor/CollectTablesMatcher.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace
{

void parseQueriesFromText(
    const String & file_contents,
    std::vector<String> & queries)
{
    size_t pos = 0;

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
            queries.push_back(query);
        }
        pos = next + 1;
    }
}

bool isCreateViewQuery(const String & query)
{
    ParserQuery parser(query.data() + query.size());
    ASTPtr ast = parseQuery(parser, query, "", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    if (!ast)
        return false;

    if (const auto * create_query = ast->as<ASTCreateQuery>())
    {
        return create_query->isView();
    }
    return false;
}

bool isDropViewQuery(const String & query)
{
    ParserQuery parser(query.data() + query.size());
    ASTPtr ast = parseQuery(parser, query, "", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    if (!ast)
        return false;

    if (const auto * drop_query = ast->as<ASTDropQuery>())
    {
        return drop_query->is_view;
    }
    return false;
}

}

QueryInfo::QueryInfo(const String & path, ContextMutablePtr context_)
    : context(context_)
{
    readQueries(path);

    for (const auto & query : queries)
    {
        if (isCreateViewQuery(query))
        {
            create_view_queries.push_back(query);
            executeQuery(query, context, QueryFlags{.internal = true});
            parseColumnsFromQuery(query);
        }
        else if (isDropViewQuery(query))
        {
            drop_view_queries.push_back(query);
        }
        else
        {
            parseColumnsFromQuery(query);
        }
    }

    for (const auto & q : drop_view_queries)
    {
        executeQuery(q, context, QueryFlags{.internal = true});
    }
}

void QueryInfo::readQueries(const String & path)
{
    queries.clear();
    drop_view_queries.clear();
    ReadBufferFromFile in{path};
    String file_contents;
    readStringUntilEOF(file_contents, in);

    parseQueriesFromText(file_contents, queries);
}

void QueryInfo::parseColumnsFromQuery(const String & query)
{
    ParserQuery parser(query.data() + query.size());
    ASTPtr ast = parseQuery(parser, query, "", DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    if (!ast)
    {
        return;
    }

    if (const auto * create_query = ast->as<ASTCreateQuery>())
    {
        if (create_query->isView())
        {
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

void QueryInfo::processTablesAndColumns(
    const CollectTablesMatcher::Data & tables_data, const std::set<String> * cte_names, const ASTPtr & ast)
{
    std::unordered_map<String, std::set<String>> table_columns;
    std::unordered_map<String, std::set<String>> table_metadata_cache;

    for (const auto & table : tables_data.tables)
    {
        if (cte_names && cte_names->contains(table))
            continue;

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
            continue;
        }

        if (storage->isView())
        {
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
        const auto& column_name = ident->shortName();
        if (!column_name.empty())
        {
            for (const auto & [table, columns] : table_metadata_cache)
            {
                if (columns.contains(column_name))
                {
                    table_columns[table].insert(column_name);
                }
            }
        }
    }

    for (const auto & [table, columns] : table_columns)
    {
        for (const auto & column : columns)
        {
            tables_to_columns[table].insert(column);
        }
    }
}

}
