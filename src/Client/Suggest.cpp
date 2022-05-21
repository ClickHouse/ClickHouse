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
    WriteBufferFromOwnString query;
    query << "SELECT DISTINCT arrayJoin(extractAll(name, '[\\\\w_]{2,}')) AS res FROM ("
        "SELECT name FROM system.functions"
        " UNION ALL "
        "SELECT name FROM system.table_engines"
        " UNION ALL "
        "SELECT name FROM system.formats"
        " UNION ALL "
        "SELECT name FROM system.table_functions"
        " UNION ALL "
        "SELECT name FROM system.data_type_families"
        " UNION ALL "
        "SELECT name FROM system.merge_tree_settings"
        " UNION ALL "
        "SELECT name FROM system.settings"
        " UNION ALL ";
    if (!basic_suggestion)
    {
        query << "SELECT cluster FROM system.clusters"
                 " UNION ALL "
                 "SELECT macro FROM system.macros"
                 " UNION ALL "
                 "SELECT policy_name FROM system.storage_policies"
                 " UNION ALL ";
    }
    query << "SELECT concat(func.name, comb.name) FROM system.functions AS func CROSS JOIN system.aggregate_function_combinators AS comb WHERE is_aggregate";
    /// The user may disable loading of databases, tables, columns by setting suggestion_limit to zero.
    if (suggestion_limit > 0)
    {
        String limit_str = toString(suggestion_limit);
        query << " UNION ALL "
                 "SELECT name FROM system.databases LIMIT " << limit_str
              << " UNION ALL "
                 "SELECT DISTINCT name FROM system.tables LIMIT " << limit_str
              << " UNION ALL ";

        if (!basic_suggestion)
        {
            query << "SELECT DISTINCT name FROM system.dictionaries LIMIT " << limit_str
                  << " UNION ALL ";
        }
        query << "SELECT DISTINCT name FROM system.columns LIMIT " << limit_str;
    }
    query << ") WHERE notEmpty(res)";

    return query.str();
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
