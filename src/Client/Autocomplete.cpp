#include <Client/Autocomplete.h>
#include <Interpreters/Context.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/Operators.h>
#include <base/defines.h>
namespace DB
{

namespace ErrorCodes
{
    extern const int OK;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int DEADLOCK_AVOIDED;
    extern const int USER_SESSION_LIMIT_EXCEEDED;
    extern const int UNKNOWN_TABLE;
}

std::vector<std::string> Autocomplete::predictNextTokens(const String & prefix)
{
    if (!loading_finished)
    {
        return {};
    }
    Lexer lexer(prefix.data(), prefix.data() + prefix.size());
    std::lock_guard lock(mutex);
    return model.predictNextWords(lexer);
}

void Autocomplete::addQuery(const String & query)
{
    std::lock_guard lock(mutex);
    addQueryToModel(query);
}

void Autocomplete::addQueryToModel(const String & query)
{
    Lexer lexer(query.data(), query.data() + query.size());
    model.addQuery(lexer);
}

void Autocomplete::fetch(
    IServerConnection & connection, const ConnectionTimeouts & timeouts, const std::string & query, const ClientInfo & client_info)
{
    connection.sendQuery(
        timeouts,
        query,
        {} /* query_parameters */,
        "" /* query_id */,
        QueryProcessingStage::Complete,
        nullptr,
        &client_info,
        false,
        {}, {});

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
                throw Exception(
                    ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}", packet.type, connection.getDescription());
        }
    }
}

template <typename ConnectionType>
void Autocomplete::load(ContextPtr context, const ConnectionParameters & connection_parameters)
{
    loading_thread = std::thread(
        [my_context = Context::createCopy(context), connection_parameters, this]
        {
            ThreadStatus thread_status;
            for (size_t retry = 0; retry < 10; ++retry)
            {
                try
                {
                    auto connection = ConnectionType::createConnection(connection_parameters, my_context);
                    fetch(*connection, connection_parameters.timeouts, history_query, my_context->getClientInfo());
                }
                catch (const Exception & e)
                {
                    last_error = e.code();
                    if (e.code() == ErrorCodes::DEADLOCK_AVOIDED)
                        continue;
                    /// Quietly keep the empty model (it still learns from this session) when history
                    /// is unavailable for an expected reason: a server without `system.user_query_log`
                    /// (an older version, or `query_log.enable_user_query_log = 0`) fails with
                    /// `UNKNOWN_TABLE`, and a server that allows only one session per user rejects
                    /// this second session with `USER_SESSION_LIMIT_EXCEEDED` (`ClientBase` then
                    /// retries the seeding through the main session). Reading `system.user_query_log`
                    /// itself requires no grants, so unlike a direct read of `system.query_log` there
                    /// is no expected `ACCESS_DENIED` to hide.
                    else if (e.code() != ErrorCodes::USER_SESSION_LIMIT_EXCEEDED && e.code() != ErrorCodes::UNKNOWN_TABLE)
                    {
                        WriteBufferFromFileDescriptor out(STDERR_FILENO, 4096);
                        out << "Cannot load data for command line autocomplete: " << getCurrentExceptionMessage(false, true) << "\n";
                        out.finalize();
                    }
                }
                catch (...)
                {
                    last_error = getCurrentExceptionCode();
                    WriteBufferFromFileDescriptor out(STDERR_FILENO, 4096);
                    out << "Cannot load data for command line autocomplete: " << getCurrentExceptionMessage(false, true) << "\n";
                    out.finalize();
                }

                break;
            }
            loading_finished = true;
        });
}

void Autocomplete::load(IServerConnection & connection, const ConnectionTimeouts & timeouts, const ClientInfo & client_info)
{
    try
    {
        fetch(connection, timeouts, history_query, client_info);
    }
    catch (...)
    {
        tryLogCurrentException("Autocomplete", __PRETTY_FUNCTION__);
        last_error = getCurrentExceptionCode();
    }
    loading_finished = true;
}

void Autocomplete::fillQueriesFromBlock(const Block & block)
{
    if (block.empty())
        return;

    if (block.columns() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of columns received for query to read words for suggestion");

    const ColumnString & column = typeid_cast<const ColumnString &>(*block.getByPosition(0).column);

    size_t rows = block.rows();

    std::lock_guard lock(mutex);
    for (size_t i = 0; i < rows; ++i)
        addQueryToModel(column[i].safeGet<String>());
}


template void Autocomplete::load<Connection>(ContextPtr context, const ConnectionParameters & connection_parameters);
template void Autocomplete::load<LocalConnection>(ContextPtr context, const ConnectionParameters & connection_parameters);
}
