#include "MySQLBinlogEventsDispatcher.h"
#include <boost/algorithm/string/join.hpp>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
    extern const int TIMEOUT_EXCEEDED;
}

namespace DB::MySQLReplication
{

class BinlogFromDispatcher : public IBinlog
{
public:
    BinlogFromDispatcher(const String & name_, const NameSet & mysql_database_names_, size_t max_bytes_, UInt64 max_waiting_ms_)
        : name(name_)
        , mysql_database_names(mysql_database_names_)
        , max_bytes(max_bytes_)
        , max_waiting_ms(max_waiting_ms_)
        , logger(getLogger("BinlogFromDispatcher(" + name + ")"))
    {
    }

    ~BinlogFromDispatcher() override
    {
        stop();
    }

    void stop()
    {
        {
            std::lock_guard lock(mutex);
            if (is_cancelled)
                return;
            is_cancelled = true;
        }
        cv.notify_all();
    }

    std::string getName() const
    {
        return name;
    }

    bool tryReadEvent(BinlogEventPtr & to, UInt64 ms) override;
    Position getPosition() const override;
    void setPosition(const Position & initial, const Position & wait);
    void setException(const std::exception_ptr & exception_);
    void push(const BinlogEventsDispatcher::Buffer & buffer);
    BinlogEventsDispatcher::BinlogMetadata getBinlogMetadata() const;

private:
    const String name;
    const NameSet mysql_database_names;
    const size_t max_bytes = 0;
    const UInt64 max_waiting_ms = 0;

    Position position;
    GTIDSets gtid_sets_wait;

    BinlogEventsDispatcher::Buffer buffer;
    mutable std::mutex mutex;

    std::condition_variable cv;
    bool is_cancelled = false;
    LoggerPtr logger = nullptr;
    std::exception_ptr exception;
};

static String getBinlogNames(const std::vector<std::weak_ptr<BinlogFromDispatcher>> & binlogs)
{
    std::vector<String> names;
    for (const auto & it : binlogs)
    {
        if (auto binlog = it.lock())
            names.push_back(binlog->getName());
    }
    return boost::algorithm::join(names, ", ");
}

BinlogEventsDispatcher::BinlogEventsDispatcher(const String & logger_name_, size_t max_bytes_in_buffer_, UInt64 max_flush_ms_)
    : logger_name(logger_name_)
    , max_bytes_in_buffer(max_bytes_in_buffer_)
    , max_flush_ms(max_flush_ms_)
    , logger(getLogger("BinlogEventsDispatcher(" + logger_name + ")"))
    , dispatching_thread(std::make_unique<ThreadFromGlobalPool>([this]() { dispatchEvents(); }))
{
}

BinlogEventsDispatcher::~BinlogEventsDispatcher()
{
    {
        std::lock_guard lock(mutex);
        is_cancelled = true;
        auto exc = std::make_exception_ptr(Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Dispatcher {} has been already destroyed", logger_name));
        try
        {
            cleanupLocked([&](const auto & binlog)
            {
                /// Notify the binlogs that the dispatcher is already destroyed
                /// and it needs to recreate new binlogs if needed
                binlog->setException(exc);
            });
        }
        catch (const std::exception & exc)
        {
            LOG_ERROR(logger, "Unexpected exception: {}", exc.what());
        }
    }
    cv.notify_all();
    if (dispatching_thread)
        dispatching_thread->join();
}

static void flushTimers(Stopwatch & watch, UInt64 & total_time, UInt64 & size, float & size_per_sec, UInt64 & bytes, float & bytes_per_sec, float threshold_flush, float threshold_reset)
{
    total_time += watch.elapsedMicroseconds();
    const float elapsed_seconds = total_time * 1e-6f;
    if (elapsed_seconds >= threshold_flush)
    {
        size_per_sec = size / elapsed_seconds;
        bytes_per_sec = bytes / elapsed_seconds;
    }
    if (elapsed_seconds >= threshold_reset)
    {
        size = 0;
        bytes = 0;
        total_time = 0;
    }
}

void BinlogEventsDispatcher::flushBufferLocked()
{
    Stopwatch watch;
    if (buffer.bytes)
        cleanupLocked([&](const auto & b) { b->push(buffer); });
    events_flush += buffer.events.size();
    bytes_flush += buffer.bytes;
    flushTimers(watch, events_flush_total_time, events_flush, events_flush_per_sec, bytes_flush, bytes_flush_per_sec, 0.1f, 1.0);
    buffer = {};
}

static bool isDispatcherEventIgnored(const BinlogEventPtr & event)
{
    switch (event->header.type)
    {
        /// Sending to all databases:
        case GTID_EVENT:          /// Catch up requested executed gtid set, used only in BinlogFromDispatcher
        case ROTATE_EVENT:        /// Change binlog_checksum
        case XID_EVENT:           /// Commit transaction
        /// Sending to all attached binlogs without filtering on dispatcher thread
        /// to keep the connection as up-to-date as possible,
        /// but these events should be filtered on databases' threads
        /// and sent only to requested databases:
        case QUERY_EVENT:         /// Apply DDL
        case WRITE_ROWS_EVENT_V1: /// Apply DML
        case WRITE_ROWS_EVENT_V2:
        case DELETE_ROWS_EVENT_V1:
        case DELETE_ROWS_EVENT_V2:
        case UPDATE_ROWS_EVENT_V1:
        case UPDATE_ROWS_EVENT_V2:
            return false;
        default:
            break;
    }
    return true;
}

void BinlogEventsDispatcher::dispatchEvents()
{
    LOG_TRACE(logger, "{}: started", __FUNCTION__);
    BinlogEventPtr event;
    BinlogPtr binlog_;
    Stopwatch watch;
    UInt64 events_read = 0;
    UInt64 bytes_read = 0;
    UInt64 events_read_total_time = 0;
    Stopwatch watch_events_read;

    while (!is_cancelled)
    {
        try
        {
            {
                std::unique_lock lock(mutex);
                cv.wait(lock, [&] { return is_cancelled || (binlog_read_from && !binlogs.empty()); });
                if (is_cancelled)
                    break;

                for (auto it = sync_to.begin(); it != sync_to.end() && !binlogs.empty();)
                {
                    if (auto d = it->lock())
                    {
                        /// If we can catch up the position of a dispatcher we synced to,
                        /// need to move all binlogs out
                        if (trySyncLocked(d))
                        {
                            /// Don't keep connection longer than needed
                            stopLocked();
                            break;
                        }
                        ++it;
                    }
                    else
                    {
                        it = sync_to.erase(it);
                    }
                }

                if (binlog_read_from)
                    binlog_read_from->setChecksum(binlog_checksum);
                binlog_ = binlog_read_from;
                if (watch.elapsedMilliseconds() >= max_flush_ms || buffer.bytes >= max_bytes_in_buffer)
                {
                    flushBufferLocked();
                    watch.restart();
                }
            }

            watch_events_read.restart();
            if (!is_cancelled && binlog_ && binlog_->tryReadEvent(event, max_flush_ms) && event)
            {
                ++events_read;
                bytes_read += event->header.event_size;
                {
                    std::lock_guard lock(mutex);
                    flushTimers(watch_events_read, events_read_total_time, events_read, events_read_per_sec, bytes_read, bytes_read_per_sec, 1.0, 5.0);
                    BinlogParser::updatePosition(event, position);
                    /// Ignore meaningless events
                    if (isDispatcherEventIgnored(event))
                        continue;
                    buffer.events.push_back(event);
                    buffer.bytes += event->header.event_size;
                    buffer.position = position;
                    /// Deliver ROTATE event ASAP if there binlog_checksum should be changed
                    if (event->header.type == ROTATE_EVENT)
                        flushBufferLocked();
                }
            }
        }
        catch (const std::exception & exc)
        {
            std::lock_guard lock(mutex);
            LOG_ERROR(logger, "Exception: {}", exc.what());
            stopLocked();
            /// All attached binlogs should be recreated
            cleanupLocked([&](const auto & b) { b->setException(std::current_exception()); });
            binlogs.clear();
            buffer = {};
            position = {};
        }
    }
    LOG_TRACE(logger, "{}: finished", __FUNCTION__);
}

bool BinlogEventsDispatcher::cleanupLocked(const std::function<void(const std::shared_ptr<BinlogFromDispatcher> & binlog)> & fn)
{
    for (auto it = binlogs.begin(); it != binlogs.end();)
    {
        if (auto binlog = it->lock())
        {
            if (fn)
                fn(binlog);
            ++it;
        }
        else
        {
            it = binlogs.erase(it);
        }
    }

    return binlogs.empty();
}

bool BinlogEventsDispatcher::cleanupBinlogsAndStop()
{
    std::lock_guard lock(mutex);
    const bool is_empty = cleanupLocked();
    if (is_empty && binlog_read_from)
        stopLocked();
    return is_empty;
}

void BinlogEventsDispatcher::stopLocked()
{
    if (!binlog_read_from)
    {
        LOG_DEBUG(logger, "Could not stop. Already stopped");
        return;
    }

    cleanupLocked();
    binlog_read_from = nullptr;
    LOG_DEBUG(logger, "Stopped: {}:{}.{}: ({})", position.binlog_name, position.gtid_sets.toString(), position.binlog_pos, getBinlogNames(binlogs));
}

BinlogPtr BinlogEventsDispatcher::createBinlogLocked(const String & name_,
                                                     const NameSet & mysql_database_names,
                                                     size_t max_bytes,
                                                     UInt64 max_waiting_ms,
                                                     const Position & pos_initial,
                                                     const Position & pos_wait)
{
    static int client_cnt = 0;
    const String client_id = !name_.empty() ? name_ : "binlog_" + std::to_string(++client_cnt);
    auto binlog = std::make_shared<BinlogFromDispatcher>(client_id, mysql_database_names, max_bytes, max_waiting_ms);
    binlogs.push_back(binlog);
    binlog->setPosition(pos_initial, pos_wait);
    LOG_DEBUG(logger, "Created binlog: {} -> {}", name_, binlog->getPosition().gtid_sets.toString());
    return binlog;
}

BinlogPtr BinlogEventsDispatcher::start(const BinlogPtr & binlog_read_from_,
                                        const String & name_,
                                        const NameSet & mysql_database_names,
                                        size_t max_bytes,
                                        UInt64 max_waiting_ms)
{
    BinlogPtr ret;
    {
        std::lock_guard lock(mutex);
        if (is_started)
            return {};
        binlog_read_from = binlog_read_from_;
        /// It is used for catching up
        /// binlog_read_from should return position with requested executed GTID set: 1-N
        position = binlog_read_from->getPosition();
        ret = createBinlogLocked(name_, mysql_database_names, max_bytes, max_waiting_ms, position);
        is_started = true;
    }
    cv.notify_all();
    return ret;
}

BinlogPtr BinlogEventsDispatcher::attach(const String & executed_gtid_set,
                                         const String & name_,
                                         const NameSet & mysql_database_names,
                                         size_t max_bytes,
                                         UInt64 max_waiting_ms)
{
    BinlogPtr ret;
    {
        std::lock_guard lock(mutex);
        /// Check if binlog_read_from can be reused:
        /// Attach to only active dispatchers
        /// and if executed_gtid_set is higher value than current
        if (!binlog_read_from || !is_started || cleanupLocked() || executed_gtid_set.empty())
            return {};
        Position pos_wait;
        pos_wait.gtid_sets.parse(executed_gtid_set);
        if (!BinlogParser::isNew(position, pos_wait))
            return {};
        ret = createBinlogLocked(name_, mysql_database_names, max_bytes, max_waiting_ms, position, pos_wait);
    }
    cv.notify_all();
    return ret;
}

void BinlogEventsDispatcher::syncToLocked(const BinlogEventsDispatcherPtr & to)
{
    if (to && this != to.get())
    {
        std::vector<String> names;
        for (const auto & it : sync_to)
        {
            if (auto dispatcher = it.lock())
                names.push_back(dispatcher->logger_name);
        }
        LOG_DEBUG(logger, "Syncing -> ({}) + ({})", boost::algorithm::join(names, ", "), to->logger_name);
        sync_to.emplace_back(to);
    }
}

void BinlogEventsDispatcher::syncTo(const BinlogEventsDispatcherPtr & to)
{
    std::lock_guard lock(mutex);
    syncToLocked(to);
}

Position BinlogEventsDispatcher::getPosition() const
{
    std::lock_guard lock(mutex);
    return position;
}

bool BinlogEventsDispatcher::trySyncLocked(BinlogEventsDispatcherPtr & to)
{
    {
        std::lock_guard lock(to->mutex);
        /// Don't catch up if positions do not have GTIDs yet
        const auto & cur_sets = position.gtid_sets.sets;
        const auto & sets = to->position.gtid_sets.sets;
        /// Sync to only started dispatchers
        if (!to->binlog_read_from || (cur_sets.empty() && sets.empty()) || to->position != position)
            return false;

        flushBufferLocked();
        to->flushBufferLocked();
        LOG_DEBUG(logger, "Synced up: {} -> {}: {}:{}.{}: ({}) + ({})", logger_name, to->logger_name,
                          position.binlog_name, position.gtid_sets.toString(), position.binlog_pos, getBinlogNames(to->binlogs), getBinlogNames(binlogs));
        std::move(binlogs.begin(), binlogs.end(), std::back_inserter(to->binlogs));
    }

    /// Notify that new binlogs arrived
    to->cv.notify_all();
    return true;
}

void BinlogEventsDispatcher::setBinlogChecksum(const String & checksum)
{
    std::lock_guard lock(mutex);
    LOG_DEBUG(logger, "Setting binlog_checksum: {}", checksum);
    binlog_checksum = IBinlog::checksumFromString(checksum);
}

void BinlogFromDispatcher::push(const BinlogEventsDispatcher::Buffer & buffer_)
{
    std::unique_lock lock(mutex);
    cv.wait_for(lock, std::chrono::milliseconds(max_waiting_ms),
        [&]
        {
            bool ret = is_cancelled || exception || max_bytes == 0 || buffer.bytes < max_bytes;
            if (!ret)
                LOG_TRACE(logger, "Waiting: bytes: {} >= {}", buffer.bytes, max_bytes);
            return ret;
        });

    if (is_cancelled || exception)
        return;

    if (max_bytes != 0 && buffer.bytes >= max_bytes)
    {
        lock.unlock();
        setException(std::make_exception_ptr(
            Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                      "Timeout exceeded: Waiting: bytes: {} >= {}", buffer.bytes, max_bytes)));
        return;
    }

    auto it = buffer_.events.begin();
    size_t bytes = buffer_.bytes;
    if (!gtid_sets_wait.sets.empty())
    {
        if (!buffer_.position.gtid_sets.contains(gtid_sets_wait))
        {
            LOG_TRACE(logger, "(wait_until: {} / {}) Skipped bytes: {}",
                              gtid_sets_wait.toString(), buffer_.position.gtid_sets.toString(), buffer_.bytes);
            return;
        }

        std::vector<GTID> seqs;
        for (auto & s : gtid_sets_wait.sets)
        {
            GTID g;
            g.uuid = s.uuid;
            for (auto & in : s.intervals)
            {
                g.seq_no = in.end;
                seqs.push_back(g);
            }
        }
        for (; it != buffer_.events.end(); ++it)
        {
            const auto & event = *it;
            auto find_if_func = [&](auto & a)
            {
                return std::static_pointer_cast<GTIDEvent>(event)->gtid == a;
            };
            if (event->header.type != GTID_EVENT || std::find_if(seqs.begin(), seqs.end(), find_if_func) == seqs.end())
            {
                LOG_TRACE(logger, "(wait_until: {} / {}) Skipped {}",
                                  gtid_sets_wait.toString(), buffer_.position.gtid_sets.toString(), magic_enum::enum_name(event->header.type));
                bytes -= event->header.event_size;
                continue;
            }
            LOG_DEBUG(logger, "(wait_until: {} / {}) Starting {}: gtid seq_no: {}",
                                gtid_sets_wait.toString(), buffer_.position.gtid_sets.toString(), magic_enum::enum_name(event->header.type),
                                std::static_pointer_cast<GTIDEvent>(event)->gtid.seq_no);
            break;
        }
        gtid_sets_wait = {};
    }

    if (it != buffer_.events.end())
    {
        std::copy(it, buffer_.events.end(), std::back_inserter(buffer.events));
        buffer.bytes += bytes;
        buffer.position = buffer_.position;
    }
    lock.unlock();
    /// Notify that added some event
    cv.notify_all();
}

static void rethrowIfNeeded(const std::exception_ptr & exception, size_t events_size)
{
    try
    {
        std::rethrow_exception(exception);
    }
    catch (const Exception & e)
    {
        /// If timeout exceeded, it is safe to read all events before rethrowning
        if (e.code() == ErrorCodes::TIMEOUT_EXCEEDED && events_size > 0)
            return;
        throw;
    }
}

static bool isBinlogEventIgnored(const NameSet & mysql_database_names, const BinlogEventPtr & event)
{
    bool ret = false;
    switch (event->header.type)
    {
        case WRITE_ROWS_EVENT_V1:
        case WRITE_ROWS_EVENT_V2:
        case DELETE_ROWS_EVENT_V1:
        case DELETE_ROWS_EVENT_V2:
        case UPDATE_ROWS_EVENT_V1:
        case UPDATE_ROWS_EVENT_V2:
            ret = !mysql_database_names.empty() && !mysql_database_names.contains(std::static_pointer_cast<RowsEvent>(event)->schema);
            break;
        case QUERY_EVENT:
            if (event->type() != MYSQL_UNHANDLED_EVENT)
            {
                auto query_event = std::static_pointer_cast<QueryEvent>(event);
                ret = !mysql_database_names.empty() &&
                      !query_event->query_database_name.empty() &&
                      !mysql_database_names.contains(query_event->query_database_name);
            }
            break;
        default:
            break;
    }
    return ret;
}

bool BinlogFromDispatcher::tryReadEvent(BinlogEventPtr & to, UInt64 ms)
{
    auto wake_up_func = [&]
    {
        if (exception)
            rethrowIfNeeded(exception, buffer.events.size());
        return is_cancelled || !buffer.events.empty();
    };
    to = nullptr;
    std::unique_lock lock(mutex);
    if (!cv.wait_for(lock, std::chrono::milliseconds(ms), wake_up_func) || is_cancelled || buffer.events.empty())
        return false;
    to = buffer.events.front();
    buffer.events.pop_front();
    BinlogParser::updatePosition(to, position);
    buffer.bytes -= to->header.event_size;
    if (isBinlogEventIgnored(mysql_database_names, to))
        to = std::make_shared<DryRunEvent>(EventHeader(to->header));
    lock.unlock();
    /// Notify that removed some event
    cv.notify_all();
    return true;
}

Position BinlogFromDispatcher::getPosition() const
{
    std::lock_guard lock(mutex);
    return position;
}

void BinlogFromDispatcher::setPosition(const Position & initial, const Position & wait)
{
    std::lock_guard lock(mutex);
    if (wait.gtid_sets.sets.empty())
    {
        position = initial;
    }
    else
    {
        position = wait;
        gtid_sets_wait = wait.gtid_sets;
    }
}

void BinlogFromDispatcher::setException(const std::exception_ptr & exception_)
{
    {
        std::lock_guard lock(mutex);
        exception = exception_;
    }
    cv.notify_all();
}

BinlogEventsDispatcher::BinlogMetadata BinlogFromDispatcher::getBinlogMetadata() const
{
    std::lock_guard lock(mutex);
    BinlogEventsDispatcher::BinlogMetadata ret;
    ret.name = name;
    ret.position_write = buffer.position;
    ret.position_read = position;
    ret.size = buffer.events.size();
    ret.bytes = buffer.bytes;
    ret.max_bytes = max_bytes;
    ret.max_waiting_ms = max_waiting_ms;
    return ret;
}

BinlogEventsDispatcher::DispatcherMetadata BinlogEventsDispatcher::getDispatcherMetadata() const
{
    std::lock_guard lock(mutex);
    DispatcherMetadata ret;
    ret.name = logger_name;
    ret.position = position;
    ret.events_read_per_sec = events_read_per_sec;
    ret.bytes_read_per_sec = bytes_read_per_sec;
    ret.events_flush_per_sec = events_flush_per_sec;
    ret.bytes_flush_per_sec = bytes_flush_per_sec;

    for (const auto & it : binlogs)
    {
        if (auto binlog = it.lock())
            ret.binlogs.push_back(binlog->getBinlogMetadata());
    }
    return ret;
}

}
