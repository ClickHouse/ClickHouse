#pragma once

#include "ConnectionParameters.h"

#include <string>
#include <sstream>
#include <string.h>
#include <vector>
#include <algorithm>

#include <ext/singleton.h>
#include <common/readline_use.h>

#include <Common/typeid_cast.h>
#include <Columns/ColumnString.h>
#include <Client/Connection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_SERVER;
}

class Suggest : public ext::singleton<Suggest>
{
private:
    /// The vector will be filled with completion words from the server and sorted.
    using Words = std::vector<std::string>;

    /// Keywords may be not up to date with ClickHouse parser.
    Words words
    {
        "CREATE", "DATABASE", "IF", "NOT", "EXISTS", "TEMPORARY", "TABLE", "ON", "CLUSTER", "DEFAULT", "MATERIALIZED", "ALIAS", "ENGINE",
        "AS", "VIEW", "POPULATE", "SETTINGS", "ATTACH", "DETACH", "DROP", "RENAME", "TO", "ALTER", "ADD", "MODIFY", "CLEAR", "COLUMN", "AFTER",
        "COPY", "PROJECT", "PRIMARY", "KEY", "CHECK", "PARTITION", "PART", "FREEZE", "FETCH", "FROM", "SHOW", "INTO", "OUTFILE", "FORMAT", "TABLES",
        "DATABASES", "LIKE", "PROCESSLIST", "CASE", "WHEN", "THEN", "ELSE", "END", "DESCRIBE", "DESC", "USE", "SET", "OPTIMIZE", "FINAL", "DEDUPLICATE",
        "INSERT", "VALUES", "SELECT", "DISTINCT", "SAMPLE", "ARRAY", "JOIN", "GLOBAL", "LOCAL", "ANY", "ALL", "INNER", "LEFT", "RIGHT", "FULL", "OUTER",
        "CROSS", "USING", "PREWHERE", "WHERE", "GROUP", "BY", "WITH", "TOTALS", "HAVING", "ORDER", "COLLATE", "LIMIT", "UNION", "AND", "OR", "ASC", "IN",
        "KILL", "QUERY", "SYNC", "ASYNC", "TEST", "BETWEEN", "TRUNCATE"
    };

    /// Words are fetched asynchonously.
    std::thread loading_thread;
    std::atomic<bool> ready{false};

    /// Points to current word to suggest.
    Words::const_iterator pos;
    /// Points after the last possible match.
    Words::const_iterator end;

    /// Set iterators to the matched range of words if any.
    void findRange(const char * prefix, size_t prefix_length)
    {
        std::string prefix_str(prefix);
        std::tie(pos, end) = std::equal_range(words.begin(), words.end(), prefix_str,
            [prefix_length](const std::string & s, const std::string & prefix_searched) { return strncmp(s.c_str(), prefix_searched.c_str(), prefix_length) < 0; });
    }

    /// Iterates through matched range.
    char * nextMatch()
    {
        if (pos >= end)
            return nullptr;

        /// readline will free memory by itself.
        char * word = strdup(pos->c_str());
        ++pos;
        return word;
    }

    void loadImpl(Connection & connection, size_t suggestion_limit)
    {
        std::stringstream query;
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
            "SELECT name FROM system.settings"
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
                "SELECT DISTINCT name FROM system.columns LIMIT " << limit_str;
        }

        query << ") WHERE notEmpty(res)";

        fetch(connection, query.str());
    }

    void fetch(Connection & connection, const std::string & query)
    {
        connection.sendQuery(query);

        while (true)
        {
            Connection::Packet packet = connection.receivePacket();
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
                    throw Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
            }
        }
    }

    void fillWordsFromBlock(const Block & block)
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

public:
    /// More old server versions cannot execute the query above.
    static constexpr int MIN_SERVER_REVISION = 54406;

    void load(const ConnectionParameters & connection_parameters, size_t suggestion_limit)
    {
        loading_thread = std::thread([connection_parameters, suggestion_limit, this]
        {
            try
            {
                Connection connection(
                    connection_parameters.host,
                    connection_parameters.port,
                    connection_parameters.default_database,
                    connection_parameters.user,
                    connection_parameters.password,
                    connection_parameters.timeouts,
                    "client",
                    connection_parameters.compression,
                    connection_parameters.security);

                loadImpl(connection, suggestion_limit);
            }
            catch (...)
            {
                std::cerr << "Cannot load data for command line suggestions: " << getCurrentExceptionMessage(false, true) << "\n";
            }

            /// Note that keyword suggestions are available even if we cannot load data from server.

            std::sort(words.begin(), words.end());
            ready = true;
        });
    }

    void finalize()
    {
        if (loading_thread.joinable())
            loading_thread.join();
    }

    /// A function for readline.
    static char * generator(const char * text, int state)
    {
        Suggest & suggest = Suggest::instance();
        if (!suggest.ready)
            return nullptr;
        if (state == 0)
            suggest.findRange(text, strlen(text));

        /// Do not append whitespace after word. For unknown reason, rl_completion_append_character = '\0' does not work.
        rl_completion_suppress_append = 1;

        return suggest.nextMatch();
    }

    ~Suggest()
    {
        finalize();
    }
};

}
