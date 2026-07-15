#include <Access/Common/AccessFlags.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <base/find_symbols.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowTablesQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Macros.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int UNKNOWN_DATABASE;
}


InterpreterShowTablesQuery::InterpreterShowTablesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
{
}

CurrentDatabaseInfo InterpreterShowTablesQuery::getFromInfo() const
{
    const auto & query = query_ptr->as<ASTShowTablesQuery &>();

    /// `FROM db.ns`- first part is the database, the rest is namespace path
    if (const auto * identifier = query.from ? query.from->as<ASTIdentifier>() : nullptr;
        identifier && identifier->name_parts.size() > 1)
    {
        CurrentDatabaseInfo info;
        info.database = identifier->name_parts[0];
        info.table_prefix = identifier->name_parts[1];
        for (size_t i = 2; i < identifier->name_parts.size(); ++i)
            info.table_prefix += "." + identifier->name_parts[i];
        return info;
    }

    if (const auto from = query.getFrom(); !from.empty())
        return {from, ""};

    auto info = getContext()->getCurrentDatabaseInfo();
    if (info.database.empty())
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Default database is not selected");
    return info;
}

String InterpreterShowTablesQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowTablesQuery &>();

    /// SHOW DATABASES
    if (query.databases)
    {
        WriteBufferFromOwnString rewritten_query;
        rewritten_query << "SELECT name FROM system.databases";

        if (!query.like.empty())
        {
            rewritten_query
                << " WHERE name "
                << (query.not_like ? "NOT " : "")
                << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                << DB::quote << query.like;
        }

        /// (*)
        rewritten_query << " ORDER BY name";

        if (query.limit_length)
            rewritten_query << " LIMIT " << query.limit_length->formatWithSecretsOneLine();

        return rewritten_query.str();
    }

    /// SHOW CLUSTERS
    if (query.clusters)
    {
        WriteBufferFromOwnString rewritten_query;
        rewritten_query << "SELECT DISTINCT cluster FROM system.clusters";

        if (!query.like.empty())
        {
            rewritten_query
                << " WHERE cluster "
                << (query.not_like ? "NOT " : "")
                << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                << DB::quote << query.like;
        }

        /// (*)
        rewritten_query << " ORDER BY cluster";

        if (query.limit_length)
            rewritten_query << " LIMIT " << query.limit_length->formatWithSecretsOneLine();

        return rewritten_query.str();
    }

    /// SHOW CLUSTER
    if (query.cluster)
    {
        WriteBufferFromOwnString rewritten_query;
        rewritten_query
            << "SELECT cluster, shard_num, replica_num, host_name, host_address, port FROM system.clusters";

        auto cluster_name_expanded = getContext()->getMacros()->expand(query.cluster_str);

        rewritten_query << " WHERE cluster = " << DB::quote << cluster_name_expanded;

        /// (*)
        rewritten_query << " ORDER BY cluster, shard_num, replica_num, host_name, host_address, port";

        return rewritten_query.str();
    }

    /// SHOW SETTINGS
    if (query.m_settings)
    {
        WriteBufferFromOwnString rewritten_query;
        rewritten_query << "SELECT name, type, value FROM system.settings";

        if (query.changed)
            rewritten_query << " WHERE changed = 1";

        if (!query.like.empty())
        {
            rewritten_query
                << (query.changed ? " AND name " : " WHERE name ")
                << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                << DB::quote << query.like;
        }

        /// (*)
        rewritten_query << " ORDER BY name, type, value ";

        return rewritten_query.str();
    }

    /// SHOW MERGES
    if (query.merges)
    {
        WriteBufferFromOwnString rewritten_query;
        rewritten_query << R"(
            SELECT
                table,
                database,
                merges.progress > 0 ? round(merges.elapsed * (1 - merges.progress) / merges.progress, 2) : NULL AS estimate_complete,
                round(elapsed, 2) AS elapsed,
                round(progress * 100, 2) AS progress,
                is_mutation,
                formatReadableSize(total_size_bytes_compressed) AS size_compressed,
                formatReadableSize(memory_usage) AS memory_usage
            FROM system.merges
            )";

        if (!query.like.empty())
        {
            rewritten_query
                << " WHERE table "
                << (query.not_like ? "NOT " : "")
                << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                << DB::quote << query.like;
        }

        /// (*)
        rewritten_query << " ORDER BY elapsed desc";

        if (query.limit_length)
            rewritten_query << " LIMIT " << query.limit_length->formatWithSecretsOneLine();

        return rewritten_query.str();
    }

    if (query.temporary && !query.getFrom().empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "The `FROM` and `TEMPORARY` cannot be used together in `SHOW TABLES`");

    /// FROM may carry a namespace path, SHOW TABLES FROM db.namespace
    /// With no FROM, the session scope applies, including `USE db.namespace` prefix
    const auto database_info = getFromInfo();
    const String & database = database_info.database;
    const String & table_namespace = database_info.table_prefix;
    DatabaseCatalog::instance().assertDatabaseExists(database);

    /// dictionaries have no namespaces
    if (query.dictionaries && !table_namespace.empty())
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "There is no database {} to show dictionaries from",
            backQuoteIfNeed(query.getFrom()));

    /// validate explicit FROM names like USE does
    if (!query.getFrom().empty() && !table_namespace.empty())
    {
        Names namespace_parts;
        splitInto<'.'>(namespace_parts, table_namespace);
        DatabaseCatalog::instance().getDatabase(database)->validateTableNamespace(namespace_parts, getContext());
    }

    WriteBufferFromOwnString rewritten_query;

    /// inside a namespace show names relative to it, and only direct children;
    /// the projection is a subquery so LIKE and WHERE both see the relative name
    const bool scoped = !table_namespace.empty() && !query.dictionaries && !query.temporary;

    if (query.full)
    {
        rewritten_query << "SELECT name, engine FROM ";
    }
    else
    {
        rewritten_query << "SELECT name FROM ";
    }

    if (query.dictionaries)
        rewritten_query << "system.dictionaries";
    else
        rewritten_query << "system.tables";

    rewritten_query << " WHERE ";

    if (query.temporary)
    {
        if (query.dictionaries)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Temporary dictionaries are not possible.");
        rewritten_query << "is_temporary";
    }
    else
    {
        rewritten_query << "database = " << DB::quote << database;
        if (scoped)
        {
            /// tables of the namespace itself, not of nested namespaces
            rewritten_query << " AND startsWith(name, " << DB::quote << (table_namespace + ".")
                            << ") AND position(name, '.', " << (table_namespace.size() + 2) << ") = 0";
        }
    }

    if (!query.like.empty())
        rewritten_query
            << " AND name "
            << (query.not_like ? "NOT " : "")
            << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
            << DB::quote << query.like;
    else if (query.where_expression)
        rewritten_query << " AND (" << query.where_expression->formatWithSecretsOneLine() << ")";

    /// (*)
    rewritten_query << " ORDER BY name ";

    if (query.limit_length)
        rewritten_query << " LIMIT " << query.limit_length->formatWithSecretsOneLine();

    return rewritten_query.str();
}


BlockIO InterpreterShowTablesQuery::execute()
{
    const auto & query = query_ptr->as<ASTShowTablesQuery &>();
    if (query.caches)
    {
        getContext()->checkAccess(AccessType::SHOW_FILESYSTEM_CACHES);

        Block sample_block{ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "Caches")};
        MutableColumns res_columns = sample_block.cloneEmptyColumns();
        auto caches = FileCacheFactory::instance().getAll();
        for (const auto & [name, _] : caches)
            res_columns[0]->insert(name);
        BlockIO res;
        size_t num_rows = res_columns[0]->size();
        auto source = std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(sample_block)), Chunk(std::move(res_columns), num_rows));
        res.pipeline = QueryPipeline(std::move(source));

        return res;
    }
    auto rewritten_query = getRewrittenQuery();
    String database = getFromInfo().database;
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");
    if (DatabaseCatalog::instance().isDatalakeCatalog(database))
    {
        /// Explicit `SHOW TABLES` should include tables from the requested data lake catalog.
        /// `system.databases` already shows all databases unconditionally, so no override is needed for `SHOW DATABASES`.
        query_context->setSetting("show_data_lake_catalogs_in_system_tables", true);
    }
    if (DatabaseCatalog::instance().isRemoteDatabase(database))
    {
        /// Explicit `SHOW TABLES` should include tables from the requested remote database.
        /// `system.databases` already shows all databases unconditionally, so no override is needed for `SHOW DATABASES`.
        query_context->setSetting("show_remote_databases_in_system_tables", true);
    }
    return executeQuery(rewritten_query, std::move(query_context), QueryFlags{ .internal = true }).second;
}

/// (*) Sorting is strictly speaking not necessary but 1. it is convenient for users, 2. SQL currently does not allow to
///     sort the output of SHOW <INFO> otherwise (SELECT * FROM (SHOW <INFO> ...) ORDER BY ...) is rejected) and 3. some
///     SQL tests can take advantage of this.


void registerInterpreterShowTablesQuery(InterpreterFactory & factory);
void registerInterpreterShowTablesQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowTablesQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowTablesQuery", create_fn, /*supports_table_namespace_scope*/ true);
}

}
