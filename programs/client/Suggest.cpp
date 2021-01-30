#include "Suggest.h"

#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int DEADLOCK_AVOIDED;
}

void Suggest::load(const ConnectionParameters & connection_parameters, size_t suggestion_limit)
{
    loading_thread = std::thread([connection_parameters, suggestion_limit, this]
    {
        for (size_t retry = 0; retry < 10; ++retry)
        {
            try
            {
                Connection connection(
                    connection_parameters.host,
                    connection_parameters.port,
                    connection_parameters.default_database,
                    connection_parameters.user,
                    connection_parameters.password,
                    "" /* cluster */,
                    "" /* cluster_secret */,
                    "client",
                    connection_parameters.compression,
                    connection_parameters.security);

                loadImpl(connection, connection_parameters.timeouts, suggestion_limit);
            }
            catch (const Exception & e)
            {
                /// Retry when the server said "Client should retry".
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

        std::sort(words.begin(), words.end());
        words_no_case = words;
        std::sort(words_no_case.begin(), words_no_case.end(), [](const std::string & str1, const std::string & str2)
        {
            return std::lexicographical_compare(begin(str1), end(str1), begin(str2), end(str2), [](const char char1, const char char2)
            {
                return std::tolower(char1) < std::tolower(char2);
            });
        });

        ready = true;
    });
}

Suggest::Suggest()
{
    /// Keywords may be not up to date with ClickHouse parser.
    words = {"CREATE",       "DATABASE", "IF",     "NOT",       "EXISTS",   "TEMPORARY",   "TABLE",    "ON",          "CLUSTER", "DEFAULT",
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
             "INTERVAL",     "LIMITS",   "ONLY",   "TRACKING",  "IP",       "REGEXP",      "ILIKE"};
}

void Suggest::loadImpl(Connection & connection, const ConnectionTimeouts & timeouts, size_t suggestion_limit)
{
    std::stringstream query;        // STYLE_CHECK_ALLOW_STD_STRING_STREAM
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
        " UNION ALL "
        "SELECT cluster FROM system.clusters"
        " UNION ALL "
        "SELECT concat(func.name, comb.name) FROM system.functions AS func CROSS JOIN system.aggregate_function_combinators AS comb WHERE is_aggregate";

    /// The user may disable loading of databases, tables, columns by setting suggestion_limit to zero.
    if (suggestion_limit > 0)
    {
        String limit_str = toString(suggestion_limit);
        query <<
            " UNION ALL "
            "SELECT name FROM system.databases LIMIT " << limit_str
            << " UNION ALL "
            "SELECT DISTINCT name FROM system.tables LIMIT " << limit_str
            << " UNION ALL "
            "SELECT DISTINCT name FROM system.dictionaries LIMIT " << limit_str
            << " UNION ALL "
            "SELECT DISTINCT name FROM system.columns LIMIT " << limit_str;
    }

    query << ") WHERE notEmpty(res)";

    fetch(connection, timeouts, query.str());
}

void Suggest::fetch(Connection & connection, const ConnectionTimeouts & timeouts, const std::string & query)
{
    connection.sendQuery(timeouts, query);

    while (true)
    {
        Packet packet = connection.receivePacket();
        switch (packet.type)
        {
            case Protocol::Server::Data:
                fillWordsFromBlock(packet.block);
                continue;

            case Protocol::Server::Progress:
                continue;
            case Protocol::Server::ProfileInfo:
                continue;
            case Protocol::Server::Totals:
                continue;
            case Protocol::Server::Extremes:
                continue;
            case Protocol::Server::Log:
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
    for (size_t i = 0; i < rows; ++i)
        words.emplace_back(column.getDataAt(i).toString());
}

}
