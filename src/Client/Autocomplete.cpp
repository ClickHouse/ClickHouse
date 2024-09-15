#include "Autocomplete.h"
#include <replxx.hxx>


namespace DB {


template <>
replxx::Replxx::completions_t Autocomplete::getPossibleNextWords<replxx::Replxx::completions_t>(const String & prefix, size_t, const char * word_break_characters) {
    if (!loading_finished) {
        return replxx::Replxx::completions_t{};
    }
    assert(isLastCharSpace(prefix, word_break_characters));
    Lexer lexer(prefix.data(), prefix.data() + prefix.size());
    auto result = model.predictNextWords(lexer);
    return replxx::Replxx::completions_t(result.begin(), result.end());
}

template <>
replxx::Replxx::hints_t Autocomplete::getPossibleNextWords<replxx::Replxx::hints_t>(const String & prefix, size_t, const char * word_break_characters) {
    if (!loading_finished) {
        return replxx::Replxx::hints_t{};
    }
    assert(isLastCharSpace(prefix, word_break_characters));
    Lexer lexer(prefix.data(), prefix.data() + prefix.size());
    auto result = model.predictNextWords(lexer);
    return replxx::Replxx::hints_t(result.begin(), result.end());
}

void Autocomplete::addQuery(const String& query) {
    Lexer lexer(query.data(), query.data() + query.size());
    model.addQuery(lexer);
}

void Autocomplete::fetch(IServerConnection & connection, const ConnectionTimeouts & timeouts, const std::string & query, const ClientInfo & client_info)
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
void Autocomplete::load(ContextPtr context, const ConnectionParameters & connection_parameters)
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
        loading_finished = true;
    });

}

void Autocomplete::load(IServerConnection & connection,
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
    loading_finished = true;
}

void Autocomplete::fillQueriesFromBlock(const Block & block)
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



template
void Autocomplete::load<Connection>(ContextPtr context, const ConnectionParameters & connection_parameters);

template
void Autocomplete::load<LocalConnection>(ContextPtr context, const ConnectionParameters & connection_parameters);
}
