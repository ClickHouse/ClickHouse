#include <Client/ConnectionEstablisher.h>
#include <Common/quoteString.h>
#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>

namespace ProfileEvents
{
    extern const Event DistributedConnectionTries;
    extern const Event DistributedConnectionUsable;
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
    extern const Event DistributedConnectionFailTry;
    extern const Event DistributedConnectionReconnectCount;
}

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_replica_delay_for_distributed_queries;
}

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int DNS_ERROR;
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_WRITE_TO_SOCKET;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace FailPoints
{
    extern const char replicated_merge_tree_all_replicas_stale[];
    extern const char connection_stale_on_establish[];
}

ConnectionEstablisher::ConnectionEstablisher(
    ConnectionPoolPtr pool_,
    const ConnectionTimeouts * timeouts_,
    const Settings & settings_,
    LoggerPtr log_,
    const QualifiedTableName * table_to_check_)
    : pool(std::move(pool_)), timeouts(timeouts_), settings(settings_), log(log_), table_to_check(table_to_check_)
{
}

void ConnectionEstablisher::run(ConnectionEstablisher::TryResult & result, std::string & fail_message)
{
    /// A pooled connection is handed out without pinging it first - a ping would add an unnecessary
    /// Ping-Pong round trip when the connection is still alive. A connection that the server has
    /// closed while it was idle in the pool is detected by a zero-timeout poll and recovered by
    /// reconnecting (see Connection::forceConnected). If a pooled connection nevertheless fails its
    /// very first request (e.g. the server went away without closing it), in the synchronous case we
    /// reconnect once and retry, so that a single available replica is not dropped just because its
    /// pooled connection went stale. In the asynchronous case we do not retry here - stale
    /// connections are handled by retrying the next replica (see ConnectionPoolWithFailover).
    const bool can_reconnect = !async_callback;

    /// Whether the connection was taken from the pool already established. Only such a connection
    /// can be stale; a failure of a freshly established one is a genuine replica problem, and
    /// retrying it is left to the caller (with its error accounting).
    bool had_pooled_connection = false;

    auto try_establish = [&]()
    {
        ProfileEvents::increment(ProfileEvents::DistributedConnectionTries);

        had_pooled_connection = false;
        result.entry = pool->getUnchecked(*timeouts, settings);
        had_pooled_connection = result.entry->isConnected();

        AsyncCallbackSetter<Connection> async_setter(&*result.entry, std::move(async_callback));

        /// For tests: simulate a pooled connection that the server has closed while it was idle,
        /// so that the reconnect-and-retry path below is exercised.
        fiu_do_on(FailPoints::connection_stale_on_establish, {
            had_pooled_connection = true;
            throw Exception(ErrorCodes::NETWORK_ERROR, "Injected stale connection failure while establishing the connection");
        });

        /// Establish the connection, or revalidate a pooled one, under the async callback: with the
        /// callback installed, Connection::connect uses a non-blocking connect and yields on it, so
        /// a caller that multiplexes several connection attempts (e.g. hedged connections) can
        /// preempt a slow connect or handshake and switch to another replica. `result.entry` is
        /// assigned before connecting, so the connection being established is visible to the
        /// multi-address timeout handling of ConnectionEstablisherAsync.
        result.entry->forceConnected(*timeouts);

        UInt64 server_revision = 0;
        if (table_to_check)
            server_revision = result.entry->getServerRevision(*timeouts);

        if (!table_to_check || server_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        {
            ProfileEvents::increment(ProfileEvents::DistributedConnectionUsable);
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
            LOG_WARNING(LogToStr(fail_message, log), "There is no table {}.{} on server: {}",
                        backQuote(table_to_check->database), backQuote(table_to_check->table), result.entry->getDescription());
            ProfileEvents::increment(ProfileEvents::DistributedConnectionMissingTable);
            return;
        }

        ProfileEvents::increment(ProfileEvents::DistributedConnectionUsable);
        result.is_usable = true;

        if (table_status_it->second.is_readonly)
        {
            result.is_readonly = true;
            LOG_TRACE(log, "Table {}.{} is readonly on server {}", table_to_check->database, table_to_check->table, result.entry->getDescription());
        }

        const UInt64 max_allowed_delay = settings[Setting::max_replica_delay_for_distributed_queries];
        if (!max_allowed_delay)
        {
            result.is_up_to_date = true;
            return;
        }

        const UInt32 delay = table_status_it->second.absolute_delay;
        if (delay < max_allowed_delay)
        {
            result.is_up_to_date = true;

            fiu_do_on(FailPoints::replicated_merge_tree_all_replicas_stale,
            {
                result.delay = 1;
                result.is_up_to_date = false;
            });
        }
        else
        {
            result.is_up_to_date = false;
            result.delay = delay;

            LOG_TRACE(log, "Server {} has unacceptable replica delay for table {}.{}: {}", result.entry->getDescription(), table_to_check->database, table_to_check->table, delay);
            ProfileEvents::increment(ProfileEvents::DistributedConnectionStaleReplica);
        }
    };

    for (size_t tries = 0; ; ++tries)
    {
        try
        {
            try_establish();
            return;
        }
        catch (const Exception & e)
        {
            ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

            /// All of these mean the connection taken from the pool turned out to be unusable, which
            /// is expected: the pooled connection is used optimistically, without a preceding ping.
            /// `UNEXPECTED_PACKET_FROM_SERVER` covers a connection left out of sync by a previous
            /// query (e.g. a stale `ProfileInfo` read instead of the `TablesStatusResponse` we
            /// requested). Anything else is a genuine error and is rethrown.
            if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT
                && e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF && e.code() != ErrorCodes::DNS_ERROR
                && e.code() != ErrorCodes::CANNOT_READ_FROM_SOCKET && e.code() != ErrorCodes::CANNOT_WRITE_TO_SOCKET
                && e.code() != ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER)
                throw;

            fail_message = getCurrentExceptionMessage(/* with_stacktrace = */ false);

            if (!result.entry.isNull())
            {
                result.entry->disconnect();
                result.reset();
            }

            /// Reconnect and retry once if a previously established (pooled) connection went stale.
            /// The failed request is safe to repeat: it is either a read-only status request, or the
            /// connection was closed before the request reached a live server.
            if (can_reconnect && tries == 0 && had_pooled_connection)
            {
                ProfileEvents::increment(ProfileEvents::DistributedConnectionReconnectCount);
                continue;
            }

            /// Report a soft failure, so the caller can retry on another replica instead of failing
            /// the whole distributed query.
            return;
        }
    }
}

#if defined(OS_LINUX)

ConnectionEstablisherAsync::ConnectionEstablisherAsync(
    ConnectionPoolPtr pool_,
    const ConnectionTimeouts * timeouts_,
    const Settings & settings_,
    LoggerPtr log_,
    const QualifiedTableName * table_to_check_)
    : AsyncTaskExecutor(std::make_unique<Task>(*this))
    , connection_establisher(std::move(pool_), timeouts_, settings_, log_, table_to_check_)
{
    epoll.add(timeout_descriptor.getDescriptor());
}

void ConnectionEstablisherAsync::Task::run(AsyncCallback async_callback, SuspendCallback)
{
    connection_establisher_async.reset();
    connection_establisher_async.connection_establisher.setAsyncCallback(async_callback);
    connection_establisher_async.connection_establisher.run(connection_establisher_async.result,
        connection_establisher_async.fail_message);
    connection_establisher_async.is_finished = true;
}

void ConnectionEstablisherAsync::processAsyncEvent(int fd, Poco::Timespan socket_timeout, AsyncEventTimeoutType type, const std::string & description, uint32_t events)
{
    socket_fd = fd;
    socket_description = description;
    epoll.add(fd, events);
    timeout_descriptor.setRelative(socket_timeout);
    timeout = socket_timeout;
    timeout_type = type;
}

void ConnectionEstablisherAsync::clearAsyncEvent()
{
    timeout_descriptor.reset();
    if (socket_fd != -1)
    {
        epoll.remove(socket_fd);
        socket_fd = -1;
    }
}

bool ConnectionEstablisherAsync::checkBeforeTaskResume()
{
    /// If we just restarted the task, no need to check timeout.
    if (restarted)
    {
        restarted = false;
        return true;
    }

    return checkTimeout();
}

void ConnectionEstablisherAsync::cancelAfter()
{
    if (!is_finished)
        reset();
}

bool ConnectionEstablisherAsync::checkTimeout()
{
    bool is_socket_ready = false;
    bool is_timeout_alarmed = false;

    epoll_event events[2];
    events[0].data.fd = events[1].data.fd = -1;
    size_t ready_count = epoll.getManyReady(2, events, 0);
    for (size_t i = 0; i != ready_count; ++i)
    {
        if (events[i].data.fd == socket_fd)
            is_socket_ready = true;
        if (events[i].data.fd == timeout_descriptor.getDescriptor())
            is_timeout_alarmed = true;
    }

    if (is_timeout_alarmed && !is_socket_ready)
    {
        if (haveMoreAddressesToConnect())
        {
            /// There are more addresses to try. Set a flag on the Connection so that
            /// when the fiber resumes, it will throw a timeout exception and the
            /// Connection::connect() loop can try the next address.
            if (!result.entry.isNull())
                result.entry->setAddressConnectTimeoutExpired();
            /// Reset the timer and remove socket from epoll so we can try the next address.
            timeout_descriptor.reset();
            if (socket_fd != -1)
            {
                epoll.remove(socket_fd);
                socket_fd = -1;
            }
            /// Return true to resume the fiber, which will throw the timeout exception.
            return true;
        }

        /// No more addresses to try - fail the connection attempt.
        /// In not async case timeout exception would be thrown and caught in ConnectionEstablisher::run,
        /// but in async case we process timeout outside and cannot throw exception. So, we just save fail message.
        fail_message = getSocketTimeoutExceededMessageByTimeoutType(timeout_type, timeout, socket_description);

        if (socket_fd != -1)
        {
            epoll.remove(socket_fd);
            socket_fd = -1;
        }
        /// Restart task, so the connection process will start from the beginning in the next resume().
        restart();
        /// The result should be Null in case of timeout.
        resetResult();
        restarted = true;
        /// Mark that current connection process is finished.
        is_finished = true;
        return false;
    }

    return true;
}

void ConnectionEstablisherAsync::afterTaskResume()
{
    if (is_finished)
    {
        restart();
        restarted = true;
    }
}

void ConnectionEstablisherAsync::reset()
{
    resetResult();
    fail_message.clear();
    socket_fd = -1;
    is_finished = false;
}

void ConnectionEstablisherAsync::resetResult()
{
    if (!result.entry.isNull())
    {
        result.entry->disconnect();
        result.reset();
    }
}

bool ConnectionEstablisherAsync::haveMoreAddressesToConnect()
{
    return !result.entry.isNull() && result.entry->haveMoreAddressesToConnect();
}

#endif

}
