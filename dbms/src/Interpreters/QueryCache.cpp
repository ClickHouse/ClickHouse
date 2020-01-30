#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/QueryCache.h>
#include <Interpreters/Set.h>

#include <Storages/IStorage.h>

namespace DB
{

QueryResult::QueryResult() :
    blocks(std::make_shared<Blocks>())
{
}

void QueryResult::add(const std::shared_ptr<QueryResult> & res)
{
    assert(set == nullptr);
    blocks->insert(blocks->end(), res->blocks->begin(), res->blocks->end());
}

size_t QueryResult::operator()(const QueryResult & x) const
{
    return x.blocks->size() || x.set;
}

size_t QueryResult::size() const
{
    size_t bytes = 0;

    for (auto & block : *blocks)
        bytes += block.allocatedBytes();

    if (set)
        bytes += set->getTotalByteCount();

    return bytes;
}

String QueryCache::getKey(const IAST & ast, const UInt32 shard_num, const QueryProcessingStage::Enum processed_stage)
{
    std::ostringstream out_key;
    IAST::FormatSettings settings(out_key, true);
    settings.with_alias = false;

    ast.format(settings);
    out_key << "_" << shard_num << "_" << QueryProcessingStage::toString(processed_stage);
    auto key = out_key.str();
    LOG_DEBUG(&Logger::get("QueryCache"), "key: " << key);

    return key;
}

RefTablesPtr QueryCache::getRefTables(const IAST & ast, const Context & context)
{
    std::set<DatabaseAndTableWithAlias> tables;
    getTables(ast, tables, context.getCurrentDatabase());

    auto ref_tables = std::make_shared<RefTables>();
    for (auto it = tables.begin(); it != tables.end(); ++it)
    {
        auto storage = context.getTable(it->database, it->table);
        if (!storage)
        {
            return nullptr;
        }

        RefTable table;
        table.database = it->database;
        table.table = it->table;
        table.alias = it->alias;
        table.version = storage->getVersion();

        ref_tables->push_back(table);

        LOG_DEBUG(&Logger::get("QueryCache"), " reference table: " << table.database << "." << table.table << " AS " << table.alias << " VERSION " << table.version << ", ");
    }

    return ref_tables;
}

QueryResultPtr QueryCache::getCache(const Key & key, const Context & context)
{
    auto res = get(key);
    if (!res)
        return res;

    auto ref_tables = res->tables;

    for (auto table : *ref_tables)
    {
        auto storage = context.getTable(table.database, table.table);
        if (!storage)
            return nullptr;

        auto storage_version = storage->getVersion();
        if (table.version == 0 || table.version != storage_version)
        {
            LOG_DEBUG(&Logger::get("QueryCache"), "ignore cache: " << key << ", cver: " << table.version << ", storage_version: " << storage_version);
            return nullptr;
        }
        else
        {
            LOG_DEBUG(&Logger::get("QueryCache"), "matched cache: " << key << ", cver: " << table.version << ", storage_version: " << storage_version);
        }
    }

    return res;
}

void getTables(const IAST & ast, std::set<DatabaseAndTableWithAlias> & databasesAndTables, const String & current_database)
{
    if (auto * expr = ast.as<ASTTableExpression>())
    {
        ASTPtr table;
        if (expr->subquery)
            table = expr->subquery;
        else if (expr->table_function)
            table = expr->table_function;
        else if (expr->database_and_table_name)
            table = expr->database_and_table_name;

        databasesAndTables.insert(DatabaseAndTableWithAlias(*expr, current_database));
        getTables(*table, databasesAndTables, current_database);
    }

    for (auto child : ast.children)
    {
        getTables(*child, databasesAndTables, current_database);
    }
}

}
