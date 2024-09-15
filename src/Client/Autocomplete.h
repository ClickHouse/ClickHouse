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
    auto getPossibleNextWords(const String & prefix, size_t, const char * word_break_characters)
    {
        if (!loading_finished) {
            return ContainerType{};
        }
        assert(isLastCharSpace(prefix, word_break_characters));
        Lexer lexer(prefix.data(), prefix.data() + prefix.size());
        auto result = model.predictNextWords(lexer);
        return ContainerType(result.begin(), result.end());
    }

    void addQuery(const String& query) {
        Lexer lexer(query.data(), query.data() + query.size());
        model.addQuery(lexer);
    }


    void fetch(IServerConnection & connection, const ConnectionTimeouts & timeouts, const std::string & query, const ClientInfo & client_info)
    {
        auto client_info_copy = client_info;
        client_info_copy.is_generated = true;
        connection.sendQuery(
            timeouts, query, {} /* query_parameters */, "" /* query_id */, QueryProcessingStage::Complete, nullptr, &client_info_copy, false, {});

        while (true)
        {
            Packet packet = connection.receivePacket();
            switch (packet.type)
            {
                case Protocol::Server::Data:
                    fillQueriesFromBlock(packet.block);
                    continue;

                case Protocol::Server::TimezoneUpdate:
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
                    last_error = ErrorCodes::OK;
                    return;

                default:
                    throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}",
                        packet.type, connection.getDescription());
            }
        }
    }


    template <typename ConnectionType>
    void load(ContextPtr context, const ConnectionParameters & connection_parameters)
    {
        loading_thread = std::thread([my_context = Context::createCopy(context), connection_parameters, this]
        {
            ThreadStatus thread_status;
            for (size_t retry = 0; retry < 10; ++retry)
            {
                try
                {
                    auto connection = ConnectionType::createConnection(connection_parameters, my_context);
                    fetch(*connection,
                        connection_parameters.timeouts,
                        history_query,
                        my_context->getClientInfo());
                }
                catch (const Exception & e)
                {
                    last_error = e.code();
                    if (e.code() == ErrorCodes::DEADLOCK_AVOIDED)
                        continue;
                    else if (e.code() != ErrorCodes::USER_SESSION_LIMIT_EXCEEDED)
                    {
                        /// We should not use std::cerr here, because this method works concurrently with the main thread.
                        /// WriteBufferFromFileDescriptor will write directly to the file descriptor, avoiding data race on std::cerr.
                        ///
                        /// USER_SESSION_LIMIT_EXCEEDED is ignored here. The client will try to receive
                        /// suggestions using the main connection later.
                        WriteBufferFromFileDescriptor out(STDERR_FILENO, 4096);
                        out << "Cannot load data for command line autocomplete: " << getCurrentExceptionMessage(false, true) << "\n";
                        out.next();
                    }
                }
                catch (...)
                {
                    last_error = getCurrentExceptionCode();
                    WriteBufferFromFileDescriptor out(STDERR_FILENO, 4096);
                    out << "Cannot load data for command line autocomplete: " << getCurrentExceptionMessage(false, true) << "\n";
                    out.next();
                }

                break;
            }

            /// Note that keyword suggestions are available even if we cannot load data from server.

            loading_finished = true;
            // std::lock_guard lock(mutex);
            // std::cout << "LOADING HISTORY FINISHED\n";
            // std::cout << "LOADED " << history_queries.size() << " suggestions\n";
            // std::cout << "FIRST FEW SUGGESTION IS: \n";
            // size_t cnt = 0;
            // for (const auto& query: history_queries) {
            //     if (cnt > 3) {break;}
            //     std::cout << query << "\n";
            //     cnt += 1;

            // }
        });

    }

    void load(IServerConnection & connection,
                    const ConnectionTimeouts & timeouts,
                    const ClientInfo & client_info)
    {
        try
        {
            fetch(connection, timeouts, history_query, client_info);
        }
        catch (...)
        {
            std::cerr << "Suggestions loading exception: " << getCurrentExceptionMessage(false, true) << std::endl;
            last_error = getCurrentExceptionCode();
        }
        // std::lock_guard lock(mutex);
        // std::cout << "LOADING HISTORY FINISHED\n";
        // std::cout << "LOADED " << history_queries.size() << " suggestions\n";
        // std::cout << "FIRST FEW SUGGESTION IS: \n";
        // size_t cnt = 0;
        // for (const auto& query: history_queries) {
        //     if (cnt > 3) {break;}
        //     std::cout << query << "\n";
        //     cnt += 1;

        // }
        loading_finished = true;
    }


    void fillQueriesFromBlock(const Block & block)
    {
        if (!block)
            return;

        if (block.columns() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of columns received for query to read words for suggestion");

        const ColumnString & column = typeid_cast<const ColumnString &>(*block.getByPosition(0).column);

        size_t rows = block.rows();

        std::vector<std::string> new_queries;
        new_queries.reserve(rows);
        std::lock_guard lock(mutex);
        for (size_t i = 0; i < rows; ++i) {
            history_queries.emplace_back(column[i].safeGet<String>());
            addQuery(history_queries.back());
        }   
    }




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
