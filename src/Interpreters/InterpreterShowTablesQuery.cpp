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

#include <utility>


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

namespace
{

/// Escape characters that are wildcards in SQL LIKE (`%` and `_`) and the
/// escape character itself (`\`) so the value is matched literally.
String escapeForLikeLiteral(const String & s)
{
    String result;
    result.reserve(s.size());
    for (char c : s)
    {
        if (c == '%' || c == '_' || c == '\\')
            result += '\\';
        result += c;
    }
    return result;
}

/// Split a dotted FROM identifier like `mycatalog.ns1.ns2` into a `(database,
/// namespace_prefix)` pair by taking the longest prefix that is an existing
/// DataLake catalog. For ordinary databases this is a no-op so the existing
/// "database does not exist" error keeps surfacing for typos.
/// Returns `(resolved, "")` when no proper prefix matches.
std::pair<String, String> splitDatabaseAndNamespacePrefix(const String & resolved)
{
    auto & catalog = DatabaseCatalog::instance();
    if (catalog.isDatabaseExist(resolved))
        return {resolved, {}};

    /// Walk from the longest prefix to the shortest, preferring the most
    /// specific match (e.g. for `a.b.c` try `a.b` before `a`).
    size_t pos = std::string::npos;
    while (true)
    {
        pos = resolved.rfind('.', pos);
        if (pos == std::string::npos || pos == 0)
            break;

        String db_candidate = resolved.substr(0, pos);
        if (catalog.isDatalakeCatalog(db_candidate) && catalog.isDatabaseExist(db_candidate))
            return {std::move(db_candidate), resolved.substr(pos + 1)};

        --pos;
    }

    return {resolved, {}};
}

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

    String resolved = getContext()->resolveDatabase(query.getFrom());

    /// Allow `SHOW TABLES FROM \`<db>.<ns1>.<ns2>\`` for DataLake catalogs.
    /// The dotted suffix maps to a catalog namespace and becomes part of the
    /// `name LIKE` predicate, which the system.tables push-down then forwards
    /// to the catalog (see issue #105022).
    auto [database, namespace_prefix] = splitDatabaseAndNamespacePrefix(resolved);
    DatabaseCatalog::instance().assertDatabaseExists(database);

    WriteBufferFromOwnString rewritten_query;

    if (query.full)
    {
        rewritten_query << "SELECT name, engine FROM system.";
    }
    else
    {
        rewritten_query << "SELECT name FROM system.";
    }

    if (query.dictionaries)
        rewritten_query << "dictionaries ";
    else
        rewritten_query << "tables ";

    rewritten_query << "WHERE ";

    if (query.temporary)
    {
        if (query.dictionaries)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Temporary dictionaries are not possible.");
        rewritten_query << "is_temporary";
    }
    else
        rewritten_query << "database = " << DB::quote << database;

    /// Prefix used to anchor every name predicate (LIKE, ILIKE, or NOT ...) to
    /// the requested catalog namespace, so the optimizer can pick the
    /// namespace up via the system.tables push-down.
    const String namespace_like_prefix
        = namespace_prefix.empty() ? String{} : escapeForLikeLiteral(namespace_prefix) + ".";

    if (!query.like.empty())
    {
        rewritten_query
            << " AND name "
            << (query.not_like ? "NOT " : "")
            << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
            << DB::quote << (namespace_like_prefix + query.like);

        /// A NOT LIKE filter alone does not restrict the result to the
        /// namespace — add an explicit anchor when both are present.
        if (query.not_like && !namespace_prefix.empty())
            rewritten_query
                << " AND name LIKE "
                << DB::quote << (namespace_like_prefix + "%");
    }
    else if (query.where_expression)
    {
        rewritten_query << " AND (" << query.where_expression->formatWithSecretsOneLine() << ")";
        if (!namespace_prefix.empty())
            rewritten_query
                << " AND name LIKE "
                << DB::quote << (namespace_like_prefix + "%");
    }
    else if (!namespace_prefix.empty())
    {
        rewritten_query
            << " AND name LIKE "
            << DB::quote << (namespace_like_prefix + "%");
    }

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
    String resolved = getContext()->resolveDatabase(query.getFrom());
    /// Resolve the actual database name when the FROM clause embeds a namespace
    /// path (e.g. `SHOW TABLES FROM \`mycatalog.ns1.ns2\``), so the datalake
    /// override below still fires.
    auto [database, namespace_prefix] = splitDatabaseAndNamespacePrefix(resolved);
    auto query_context = Context::createCopy(getContext());
    query_context->makeQueryContext();
    query_context->setCurrentQueryId("");
    if (DatabaseCatalog::instance().isDatalakeCatalog(database))
    {
        /// HACK: force the setting so that system.tables includes tables from the requested data lake catalog.
        /// system.databases already shows all catalogs unconditionally, so no override is needed for SHOW DATABASES.
        query_context->setSetting("show_data_lake_catalogs_in_system_tables", true);
    }
    return executeQuery(rewritten_query, std::move(query_context), QueryFlags{ .internal = true }).second;
}

/// (*) Sorting is strictly speaking not necessary but 1. it is convenient for users, 2. SQL currently does not allow to
///     sort the output of SHOW <INFO> otherwise (SELECT * FROM (SHOW <INFO> ...) ORDER BY ...) is rejected) and 3. some
///     SQL tests can take advantage of this.


void registerInterpreterShowTablesQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowTablesQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterShowTablesQuery", create_fn);
}

}
