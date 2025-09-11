#include <Client/ConnectionEstablisher.h>
#include <Common/quoteString.h>
#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Core/Settings.h>

namespace ProfileEvents
{
    extern const Event DistributedConnectionTries;
    extern const Event DistributedConnectionUsable;
    extern const Event DistributedConnectionMissingTable;
    extern const Event DistributedConnectionStaleReplica;
    extern const Event DistributedConnectionFailTry;
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
}

namespace FailPoints
{
    extern const char replicated_merge_tree_all_replicas_stale[];
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

void ConnectionEstablisher::run(ConnectionEstablisher::TryResult & result, std::string & fail_message, bool force_connected)
{
    try
    {
        ProfileEvents::increment(ProfileEvents::DistributedConnectionTries);
        result.entry = pool->get(*timeouts, settings, force_connected);
        AsyncCallbackSetter async_setter(&*result.entry, std::move(async_callback));

        UInt64 server_revision = 0;
        if (table_to_check)
            server_revision = result.entry->getServerRevision(*timeouts);

        if (!table_to_check || server_revision < DBMS_MIN_REVISION_WITH_TABLES_STATUS)
        {
            if (!force_connected)
                result.entry->forceConnected(*timeouts);

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
    }
    catch (const Exception & e)
    {
        ProfileEvents::increment(ProfileEvents::DistributedConnectionFailTry);

        if (e.code() != ErrorCodes::NETWORK_ERROR && e.code() != ErrorCodes::SOCKET_TIMEOUT
            && e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF && e.code() != ErrorCodes::DNS_ERROR)
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
        connection_establisher_async.fail_message, connection_establisher_async.force_connected);
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
    epoll.remove(socket_fd);
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

    if (is_timeout_alarmed && !is_socket_ready && !haveMoreAddressesToConnect())
    {
        /// In not async case timeout exception would be thrown and caught in ConnectionEstablisher::run,
        /// but in async case we process timeout outside and cannot throw exception. So, we just save fail message.
        fail_message = getSocketTimeoutExceededMessageByTimeoutType(timeout_type, timeout, socket_description);

        epoll.remove(socket_fd);
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
