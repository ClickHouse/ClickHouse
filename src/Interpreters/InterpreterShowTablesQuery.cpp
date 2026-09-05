#include <Access/Common/AccessFlags.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowTablesQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/ColumnsDescription.h>
#include <Common/Macros.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>

#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}


InterpreterShowTablesQuery::InterpreterShowTablesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , query_ptr(query_ptr_)
{
}

/// The sources of the tables listed for a hierarchical database name (see `DatabaseCatalog`), as conditions on
/// `system.tables` and expressions of the listed name. Empty for an ordinary database name, which is only itself.
std::vector<InterpreterShowTablesQuery::HierarchicalDatabaseSource> InterpreterShowTablesQuery::getHierarchicalDatabaseSources(const String & database)
{
    const auto & catalog = DatabaseCatalog::instance();

    std::vector<String> parts = DatabaseCatalog::splitHierarchicalName(database);

    /// The databases `a.b.*`, when there are such.
    bool has_nested_databases = false;
    String nested_database_prefix = database + '.';
    auto databases = catalog.getDatabases(GetDatabasesOptions{.with_datalake_catalogs = true, .with_remote_databases = true});
    if (auto it = databases.lower_bound(nested_database_prefix); it != databases.end() && it->first.starts_with(nested_database_prefix))
        has_nested_databases = true;

    if (parts.size() < 2 && !has_nested_databases)
        return {};

    std::vector<HierarchicalDatabaseSource> sources;
    auto quoted = [](const String & value) { WriteBufferFromOwnString buf; buf << DB::quote << value; return buf.str(); };

    /// The database itself.
    sources.push_back({.condition = "database = " + quoted(database), .name_expression = "name"});

    if (has_nested_databases)
        sources.push_back({
            .condition = "startsWith(database, " + quoted(nested_database_prefix) + ")",
            .name_expression = fmt::format("concat(substring(database, {}), '.', name)", nested_database_prefix.size() + 1)});

    /// The tables `b.*` of the database `a`, for every prefix `a` that is a database.
    for (size_t database_parts = parts.size() - 1; database_parts >= 1; --database_parts)
    {
        String database_name = fmt::format("{}", fmt::join(parts.begin(), parts.begin() + database_parts, "."));
        if (!catalog.isDatabaseExist(database_name))
            continue;

        String table_prefix = fmt::format("{}.", fmt::join(parts.begin() + database_parts, parts.end(), "."));
        sources.push_back({
            .condition = "(database = " + quoted(database_name) + " AND startsWith(name, " + quoted(table_prefix) + "))",
            .name_expression = fmt::format("substring(name, {})", table_prefix.size() + 1)});
    }

    return sources;
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

    String database = getContext()->resolveDatabase(query.getFrom());
    DatabaseCatalog::instance().assertDatabaseExists(database);

    WriteBufferFromOwnString rewritten_query;

    String system_table = query.dictionaries ? "system.dictionaries" : "system.tables";
    String engine_column = query.full ? ", engine" : "";

    if (query.temporary)
    {
        if (query.dictionaries)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Temporary dictionaries are not possible.");
        rewritten_query << "SELECT name" << engine_column << " FROM " << system_table << " WHERE is_temporary";
    }
    else
    {
        auto sources = getHierarchicalDatabaseSources(database);
        if (sources.empty())
        {
            rewritten_query << "SELECT name" << engine_column << " FROM " << system_table << " WHERE database = " << DB::quote << database;
        }
        else
        {
            /// A hierarchical database name (see `DatabaseCatalog`): the tables of the database `a.b`, of the databases
            /// `a.b.*` (listed as `c.table`), and the tables `b.*` of the database `a` (listed as `table`) - the names
            /// are relative to the name selected by `USE`. The relative name gets its own alias in the subquery,
            /// because an alias named `name` would shadow the column in the subquery's own `WHERE`.
            rewritten_query << "SELECT hierarchical_name AS name" << engine_column << " FROM (SELECT * EXCEPT (name), multiIf(";
            for (const auto & source : sources)
                rewritten_query << source.condition << ", " << source.name_expression << ", ";
            rewritten_query << "name) AS hierarchical_name FROM " << system_table << " WHERE ";
            for (size_t i = 0; i < sources.size(); ++i)
                rewritten_query << (i ? " OR " : "") << sources[i].condition;
            rewritten_query << ") WHERE 1";
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
    String database = getContext()->resolveDatabase(query.getFrom());
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");

    /// A hierarchical database name (`a.b`) may denote the tables `b.*` of the database `a`: the flags below are
    /// about that database then.
    auto resolved_database = DatabaseCatalog::instance().resolveHierarchicalDatabase(database);
    if (!resolved_database.database_name.empty())
        database = resolved_database.database_name;

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
    factory.registerInterpreter("InterpreterShowTablesQuery", create_fn);
}

}
