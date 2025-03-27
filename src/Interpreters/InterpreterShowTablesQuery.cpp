#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterShowTablesQuery.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Access/Common/AccessFlags.h>
#include <Common/typeid_cast.h>
#include <IO/Operators.h>


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
            rewritten_query << " LIMIT " << query.limit_length;

        return rewritten_query.str();
    }

    /// SHOW CLUSTER/CLUSTERS
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
            rewritten_query << " LIMIT " << query.limit_length;

        return rewritten_query.str();
    }
    if (query.cluster)
    {
        WriteBufferFromOwnString rewritten_query;
        rewritten_query << "SELECT * FROM system.clusters";

        rewritten_query << " WHERE cluster = " << DB::quote << query.cluster_str;

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
            rewritten_query << " LIMIT " << query.limit_length;

        return rewritten_query.str();
    }

    if (query.temporary && !query.getFrom().empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "The `FROM` and `TEMPORARY` cannot be used together in `SHOW TABLES`");

    String database = getContext()->resolveDatabase(query.getFrom());
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

    if (!query.like.empty())
        rewritten_query
            << " AND name "
            << (query.not_like ? "NOT " : "")
            << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
            << DB::quote << query.like;
    else if (query.where_expression)
        rewritten_query << " AND (" << query.where_expression << ")";

    /// (*)
    rewritten_query << " ORDER BY name ";

    if (query.limit_length)
        rewritten_query << " LIMIT " << query.limit_length;

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
        auto source = std::make_shared<SourceFromSingleChunk>(sample_block, Chunk(std::move(res_columns), num_rows));
        res.pipeline = QueryPipeline(std::move(source));

        return res;
    }

    return executeQuery(getRewrittenQuery(), getContext(), QueryFlags{ .internal = true }).second;
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
