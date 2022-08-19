#include "Suggest.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Core/Settings.h>
#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Common/Macros.h>
#include "Core/Protocol.h"
#include <IO/Operators.h>
#include <Functions/FunctionFactory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/StorageFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Client/Connection.h>
#include <Client/LocalConnection.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int DEADLOCK_AVOIDED;
}

Suggest::Suggest()
{
    /// Keywords may be not up to date with ClickHouse parser.
    addWords({
        "CREATE",       "DATABASE", "IF",     "NOT",       "EXISTS",   "TEMPORARY",   "TABLE",    "ON",          "CLUSTER", "DEFAULT",
        "MATERIALIZED", "ALIAS",    "ENGINE", "AS",        "VIEW",     "POPULATE",    "SETTINGS", "ATTACH",      "DETACH",  "DROP",
        "RENAME",       "TO",       "ALTER",  "ADD",       "MODIFY",   "CLEAR",       "COLUMN",   "AFTER",       "COPY",    "PROJECT",
        "PRIMARY",      "KEY",      "CHECK",  "PARTITION", "PART",     "FREEZE",      "FETCH",    "FROM",        "SHOW",    "INTO",
        "OUTFILE",      "FORMAT",   "TABLES", "DATABASES", "LIKE",     "PROCESSLIST", "CASE",     "WHEN",        "THEN",    "ELSE",
        "END",          "DESCRIBE", "DESC",   "USE",       "SET",      "OPTIMIZE",    "FINAL",    "DEDUPLICATE", "INSERT",  "VALUES",
        "SELECT",       "DISTINCT", "SAMPLE", "ARRAY",     "JOIN",     "GLOBAL",      "LOCAL",    "ANY",         "ALL",     "INNER",
        "LEFT",         "RIGHT",    "FULL",   "OUTER",     "CROSS",    "USING",       "PREWHERE", "WHERE",       "GROUP",   "BY",
        "WITH",         "TOTALS",   "HAVING", "ORDER",     "COLLATE",  "LIMIT",       "UNION",    "AND",         "OR",      "ASC",
        "IN",           "KILL",     "QUERY",  "SYNC",      "ASYNC",    "TEST",        "BETWEEN",  "TRUNCATE",    "USER",    "ROLE",
        "PROFILE",      "QUOTA",    "POLICY", "ROW",       "GRANT",    "REVOKE",      "OPTION",   "ADMIN",       "EXCEPT",  "REPLACE",
        "IDENTIFIED",   "HOST",     "NAME",   "READONLY",  "WRITABLE", "PERMISSIVE",  "FOR",      "RESTRICTIVE", "RANDOMIZED",
        "INTERVAL",     "LIMITS",   "ONLY",   "TRACKING",  "IP",       "REGEXP",      "ILIKE",
    });
}

static String getLoadSuggestionQuery(Int32 suggestion_limit, bool basic_suggestion)
{
    /// NOTE: Once you will update the completion list,
    /// do not forget to update 01676_clickhouse_client_autocomplete.sh
    String query;

    auto add_subquery = [&](std::string_view select, std::string_view result_column_name)
    {
        if (!query.empty())
            query += " UNION ALL ";
        query += fmt::format("SELECT * FROM viewIfPermitted({} ELSE null('{} String'))", select, result_column_name);
    };

    auto add_column = [&](std::string_view column_name, std::string_view table_name, bool distinct, std::optional<Int64> limit)
    {
        add_subquery(
            fmt::format(
                "SELECT {}{} FROM system.{}{}",
                (distinct ? "DISTINCT " : ""),
                column_name,
                table_name,
                (limit ? (" LIMIT " + std::to_string(*limit)) : "")),
            column_name);
    };

    add_column("name", "functions", false, {});
    add_column("name", "table_engines", false, {});
    add_column("name", "formats", false, {});
    add_column("name", "table_functions", false, {});
    add_column("name", "data_type_families", false, {});
    add_column("name", "merge_tree_settings", false, {});
    add_column("name", "settings", false, {});

    if (!basic_suggestion)
    {
        add_column("cluster", "clusters", false, {});
        add_column("macro", "macros", false, {});
        add_column("policy_name", "storage_policies", false, {});
    }

    add_subquery("SELECT concat(func.name, comb.name) AS x FROM system.functions AS func CROSS JOIN system.aggregate_function_combinators AS comb WHERE is_aggregate", "x");

    /// The user may disable loading of databases, tables, columns by setting suggestion_limit to zero.
    if (suggestion_limit > 0)
    {
        add_column("name", "databases", false, suggestion_limit);
        add_column("name", "tables", true, suggestion_limit);
        if (!basic_suggestion)
        {
            add_column("name", "dictionaries", true, suggestion_limit);
        }
        add_column("name", "columns", true, suggestion_limit);
    }

    query = "SELECT DISTINCT arrayJoin(extractAll(name, '[\\\\w_]{2,}')) AS res FROM (" + query + ") WHERE notEmpty(res)";
    return query;
}

template <typename ConnectionType>
void Suggest::load(ContextPtr context, const ConnectionParameters & connection_parameters, Int32 suggestion_limit)
{
    loading_thread = std::thread([context=Context::createCopy(context), connection_parameters, suggestion_limit, this]
    {
        ThreadStatus thread_status;
        for (size_t retry = 0; retry < 10; ++retry)
        {
            try
            {
                auto connection = ConnectionType::createConnection(connection_parameters, context);
                fetch(*connection, connection_parameters.timeouts, getLoadSuggestionQuery(suggestion_limit, std::is_same_v<ConnectionType, LocalConnection>));
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::DEADLOCK_AVOIDED)
                    continue;

                std::cerr << "Cannot load data for command line suggestions: " << getCurrentExceptionMessage(false, true) << "\n";
            }
            catch (...)
            {
                std::cerr << "Cannot load data for command line suggestions: " << getCurrentExceptionMessage(false, true) << "\n";
            }

            break;
        }

        /// Note that keyword suggestions are available even if we cannot load data from server.
    });
}

void Suggest::fetch(IServerConnection & connection, const ConnectionTimeouts & timeouts, const std::string & query)
{
    connection.sendQuery(timeouts, query, "" /* query_id */, QueryProcessingStage::Complete, nullptr, nullptr, false, {});

    while (true)
    {
        Packet packet = connection.receivePacket();
        switch (packet.type)
        {
            case Protocol::Server::Data:
                fillWordsFromBlock(packet.block);
                continue;

            case Protocol::Server::Progress:
            case Protocol::Server::ProfileInfo:
            case Protocol::Server::Totals:
            case Protocol::Server::Extremes:
            case Protocol::Server::Log:
            case Protocol::Server::ProfileEvents:
                continue;

            case Protocol::Server::Exception:
                packet.exception->rethrow();
                return;

            case Protocol::Server::EndOfStream:
                return;

            default:
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}",
                    packet.type, connection.getDescription());
        }
    }
}

void Suggest::fillWordsFromBlock(const Block & block)
{
    if (!block)
        return;

    if (block.columns() != 1)
        throw Exception("Wrong number of columns received for query to read words for suggestion", ErrorCodes::LOGICAL_ERROR);

    const ColumnString & column = typeid_cast<const ColumnString &>(*block.getByPosition(0).column);

    size_t rows = block.rows();

    Words new_words;
    new_words.reserve(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        new_words.emplace_back(column.getDataAt(i).toString());
    }
    addWords(std::move(new_words));
}

template
void Suggest::load<Connection>(ContextPtr context, const ConnectionParameters & connection_parameters, Int32 suggestion_limit);

template
void Suggest::load<LocalConnection>(ContextPtr context, const ConnectionParameters & connection_parameters, Int32 suggestion_limit);
}
