#pragma once

#include "AutocompleteModel.h"
#include <Client/ConnectionParameters.h>
#include <Columns/ColumnString.h>
#include <Parsers/Lexer.h>
#include <Client/Connection.h>
#include <Client/IServerConnection.h>
#include <Client/LocalConnection.h>
#include <Client/LineReader.h>
#include <IO/ConnectionTimeouts.h>
#include <atomic>
#include <thread>
#include <vector>

/// TODO: remove everythin in cpp + static/const where possible


namespace DB
{

namespace ErrorCodes
{
    extern const int OK;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int DEADLOCK_AVOIDED;
    extern const int USER_SESSION_LIMIT_EXCEEDED;
}

class Autocomplete: public boost::noncopyable 
{

public:
    Autocomplete() = default;

    ~Autocomplete()
    {
        if (loading_thread.joinable())
            loading_thread.join();
    }

    int getLastError() const { return last_error.load(); }


    static bool isLastCharSpace(const String & prefix, const char * word_break_characters)
    {
    // Is it even safe to iterate until '\0'? 
    // We get const char * here, so there is no guarantee that it is a legit c-str with \0 at the end.
    // Originally it is char[], maybe it is better to pass char[] everywhere? This way we can get size
        if (prefix.empty()) {
            return false;
        }
        for (const auto *p = word_break_characters; *p != '\0'; ++p) {
            if (prefix.back() == *p) {
                return true;
            }
        }
        return false;
    }

    template <typename ContainerType>
    ContainerType getPossibleNextWords(const String & prefix, size_t, const char * word_break_characters);

    void addQuery(const String& query);

    void fetch(IServerConnection & connection, const ConnectionTimeouts & timeouts, const std::string & query, const ClientInfo & client_info);

    template <typename ConnectionType>
    void load(ContextPtr context, const ConnectionParameters & connection_parameters);

    void load(IServerConnection & connection,
                    const ConnectionTimeouts & timeouts,
                    const ClientInfo & client_info);


    void fillQueriesFromBlock(const Block & block);

private:
    AutocompleteModel model = AutocompleteModel();

    std::vector<std::string> history_queries TSA_GUARDED_BY(mutex);

    std::atomic<bool> loading_finished = false;

    std::thread loading_thread;

    std::mutex mutex;

    std::atomic<int> last_error { -1 };

    size_t query_history_limit = 700;

    const String history_query = fmt::format("SELECT query FROM (SELECT query, query_start_time FROM system.query_log WHERE is_generated_query = 0 AND is_initial_query = 1 AND type = 2 AND user IN (SELECT currentUser()) ORDER BY query_start_time DESC LIMIT {}) AS recent_queries ORDER BY query_start_time ASC;", query_history_limit);
};
}
