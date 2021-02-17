#include <Client/ConnectionEstablisher.h>
#include <Common/quoteString.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
}


ConnectionEstablisher::ConnectionEstablisher(
    IConnectionPool * pool_,
    const ConnectionTimeouts * timeouts_,
    const Settings * settings_,
    const QualifiedTableName * table_to_check_)
    : pool(pool_), timeouts(timeouts_), settings(settings_), table_to_check(table_to_check_),
      stage(Stage::INITIAL), log(&Poco::Logger::get("ConnectionEstablisher"))
{
#if defined(OS_LINUX)
    epoll.add(receive_timeout.getDescriptor());
#endif
}

void ConnectionEstablisher::Routine::ReadCallback::operator()(int fd, const Poco::Timespan & timeout, const std::string &)
{
#if defined(OS_LINUX)
    if (connection_establisher.socket_fd != fd)
    {
        if (connection_establisher.socket_fd != -1)
            connection_establisher.epoll.remove(connection_establisher.socket_fd);

        connection_establisher.epoll.add(fd);
        connection_establisher.socket_fd = fd;
    }

    connection_establisher.receive_timeout.setRelative(timeout);
    fiber = std::move(fiber).resume();
    connection_establisher.receive_timeout.reset();
#endif
}

Fiber ConnectionEstablisher::Routine::operator()(Fiber && sink)
{
    try
    {
        connection_establisher.establishConnection(ReadCallback{connection_establisher, sink});
    }
    catch (const boost::context::detail::forced_unwind &)
    {
        /// This exception is thrown by fiber implementation in case if fiber is being deleted but hasn't exited
        /// It should not be caught or it will segfault.
        /// Other exceptions must be caught
        throw;
    }
    catch (...)
    {
        connection_establisher.exception = std::current_exception();
    }

    return std::move(sink);
}

void ConnectionEstablisher::resume()
{
    if (!fiber_created)
    {
        reset();
        fiber = boost::context::fiber(std::allocator_arg_t(), fiber_stack, Routine{*this});
        fiber_created = true;
        resumeFiber();
        return;
    }

#if defined(OS_LINUX)
    bool is_socket_ready = false;
    bool is_receive_timeout_alarmed = false;

    epoll_event events[2];
    events[0].data.fd = events[1].data.fd;
    size_t ready_count = epoll.getManyReady(2, events, true);
    for (size_t i = 0; i != ready_count; ++i)
    {
        if (events[i].data.fd == socket_fd)
            is_socket_ready = true;
        if (events[i].data.fd == receive_timeout.getDescriptor())
            is_receive_timeout_alarmed = true;
    }

    if (is_receive_timeout_alarmed && !is_socket_ready)
        processReceiveTimeout();
#endif

    resumeFiber();
}

void ConnectionEstablisher::cancel()
{
    destroyFiber();
    reset();
}

void ConnectionEstablisher::processReceiveTimeout()
{
#if defined(OS_LINUX)
    destroyFiber();
    stage = Stage::FAILED;
    fail_message = "Code: 209, e.displayText() = DB::NetException: Timeout exceeded while reading from socket (" + result.entry->getDescription() + ")";
    epoll.remove(socket_fd);
    resetResult();
#endif
}

void ConnectionEstablisher::resetResult()
{
    if (!result.entry.isNull())
    {
        result.entry->disconnect();
        result.reset();
    }
}

void ConnectionEstablisher::reset()
{
    stage = Stage::INITIAL;
    resetResult();
    fail_message.clear();
    socket_fd = -1;
}

void ConnectionEstablisher::resumeFiber()
{
    fiber = std::move(fiber).resume();

    if (exception)
        std::rethrow_exception(std::move(exception));

    if (stage == Stage::FAILED)
        destroyFiber();
}

void ConnectionEstablisher::destroyFiber()
{
    Fiber to_destroy = std::move(fiber);
    fiber_created = false;
}

void ConnectionEstablisher::establishConnection(AsyncCallback async_callback)
{
    try
    {
        stage = Stage::IN_PROCESS;
        result.entry = pool->get(*timeouts, settings, /* force_connected = */ false);
        AsyncCallbackSetter async_setter(&*result.entry, std::move(async_callback));

        UInt64 server_revision = 0;
        if (table_to_check)
            server_revision = result.entry->getServerRevision(*timeouts);

        if (!table_to_check || server_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        {
            result.entry->forceConnected(*timeouts);
            result.is_usable = true;
            result.is_up_to_date = true;
            stage = Stage::FINISHED;
            return;
        }

        /// Only status of the remote table corresponding to the Distributed table is taken into account.
        /// TODO: request status for joined tables also.
        TablesStatusRequest status_request;
        status_request.tables.emplace(*table_to_check);

        TablesStatusResponse status_response = result.entry->getTablesStatus(*timeouts, status_request);
        auto table_status_it = status_response.table_states_by_id.find(*table_to_check);
        if (table_status_it == status_response.table_states_by_id.end())
        {
            const char * message_pattern = "There is no table {}.{} on server: {}";
            fail_message = fmt::format(message_pattern, backQuote(table_to_check->database), backQuote(table_to_check->table), result.entry->getDescription());
            LOG_WARNING(log, fail_message);
            ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);

            stage = Stage::FINISHED;
            return;
        }

        result.is_usable = true;

        UInt64 max_allowed_delay = settings ? UInt64(settings->max_replica_delay_for_distributed_queries) : 0;
        if (!max_allowed_delay)
        {
            result.is_up_to_date = true;
            stage = Stage::FINISHED;
            return;
        }

        UInt32 delay = table_status_it->second.absolute_delay;

        if (delay < max_allowed_delay)
            result.is_up_to_date = true;
        else
        {
            result.is_up_to_date = false;
            result.staleness = delay;

            LOG_TRACE(log, "Server {} has unacceptable replica delay for table {}.{}: {}", result.entry->getDescription(), table_to_check->database, table_to_check->table, delay);
            ProfileEvents::increment(ProfileEvents::DistributedConnectionStaleReplica);
        }
        stage = Stage::FINISHED;
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT
            && e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throw;

        fail_message = getCurrentExceptionMessage(/* with_stacktrace = */ false);
        resetResult();
        stage = Stage::FAILED;
    }
}

}
