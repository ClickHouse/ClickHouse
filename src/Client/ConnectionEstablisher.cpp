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
    Poco::Logger * log_,
    const QualifiedTableName * table_to_check_)
    : pool(pool_), timeouts(timeouts_), settings(settings_), log(log_), table_to_check(table_to_check_), is_finished(false)
{
}

void ConnectionEstablisher::run(ConnectionEstablisher::TryResult & result, std::string & fail_message)
{
    is_finished = false;
    SCOPE_EXIT(is_finished = true);
    try
    {
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
            return;
        }

        result.is_usable = true;

        UInt64 max_allowed_delay = settings ? UInt64(settings->max_replica_delay_for_distributed_queries) : 0;
        if (!max_allowed_delay)
        {
            result.is_up_to_date = true;
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
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT
            && e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throw;

        fail_message = getCurrentExceptionMessage(/* with_stacktrace = */ false);

        if (!result.entry.isNull())
        {
            result.entry->disconnect();
            result.reset();
        }
    }
}

#if defined(OS_LINUX)

ConnectionEstablisherAsync::ConnectionEstablisherAsync(
    IConnectionPool * pool_,
    const ConnectionTimeouts * timeouts_,
    const Settings * settings_,
    Poco::Logger * log_,
    const QualifiedTableName * table_to_check_)
    : connection_establisher(pool_, timeouts_, settings_, log_, table_to_check_)
{
    epoll.add(receive_timeout.getDescriptor());
}

void ConnectionEstablisherAsync::Routine::ReadCallback::operator()(int fd, Poco::Timespan timeout, const std::string &)
{
    /// Check if it's the first time and we need to add socket fd to epoll.
    if (connection_establisher_async.socket_fd == -1)
    {
        connection_establisher_async.epoll.add(fd);
        connection_establisher_async.socket_fd = fd;
    }

    connection_establisher_async.receive_timeout.setRelative(timeout);
    fiber = std::move(fiber).resume();
    connection_establisher_async.receive_timeout.reset();
}

Fiber ConnectionEstablisherAsync::Routine::operator()(Fiber && sink)
{
    try
    {
        connection_establisher_async.connection_establisher.setAsyncCallback(ReadCallback{connection_establisher_async, sink});
        connection_establisher_async.connection_establisher.run(connection_establisher_async.result, connection_establisher_async.fail_message);
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
        connection_establisher_async.exception = std::current_exception();
    }

    return std::move(sink);
}

std::variant<int, ConnectionEstablisher::TryResult> ConnectionEstablisherAsync::resume()
{
    if (!fiber_created)
    {
        reset();
        fiber = boost::context::fiber(std::allocator_arg_t(), fiber_stack, Routine{*this});
        fiber_created = true;
    } else if (!checkReceiveTimeout())
        return result;

    fiber = std::move(fiber).resume();

    if (exception)
        std::rethrow_exception(std::move(exception));

    if (connection_establisher.isFinished())
    {
        destroyFiber();
        return result;
    }

    return epoll.getFileDescriptor();
}

bool ConnectionEstablisherAsync::checkReceiveTimeout()
{
    bool is_socket_ready = false;
    bool is_receive_timeout_alarmed = false;

    epoll_event events[2];
    events[0].data.fd = events[1].data.fd = -1;
    size_t ready_count = epoll.getManyReady(2, events, false);
    for (size_t i = 0; i != ready_count; ++i)
    {
        if (events[i].data.fd == socket_fd)
            is_socket_ready = true;
        if (events[i].data.fd == receive_timeout.getDescriptor())
            is_receive_timeout_alarmed = true;
    }

    if (is_receive_timeout_alarmed && !is_socket_ready)
    {
        destroyFiber();
        /// In not async case this exception would be thrown and caught in ConnectionEstablisher::run,
        /// but in async case we process timeout outside and cannot throw exception. So, we just save fail message.
        fail_message = "Timeout exceeded while reading from socket (" + result.entry->getDescription() + ")";
        epoll.remove(socket_fd);
        resetResult();
        return false;
    }

    return true;
}

void ConnectionEstablisherAsync::cancel()
{
    destroyFiber();
    reset();
}

void ConnectionEstablisherAsync::reset()
{
    resetResult();
    fail_message.clear();
    socket_fd = -1;
}

void ConnectionEstablisherAsync::resetResult()
{
    if (!result.entry.isNull())
    {
        result.entry->disconnect();
        result.reset();
    }
}

void ConnectionEstablisherAsync::destroyFiber()
{
    Fiber to_destroy = std::move(fiber);
    fiber_created = false;
}

#endif

}
