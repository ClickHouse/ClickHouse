#pragma once

#include <Databases/MySQL/MySQLBinlog.h>
#include <Common/ThreadPool.h>
#include <Core/Names.h>
#include <base/unit.h>

namespace DB::MySQLReplication
{

class BinlogEventsDispatcher;
using BinlogEventsDispatcherPtr = std::shared_ptr<BinlogEventsDispatcher>;
class BinlogFromDispatcher;

/** Reads the binlog events from one source and dispatches them over consumers.
  * If it can catch up the position of the another dispatcher, it will move all consumers to this dispatcher.
  */
class BinlogEventsDispatcher final : boost::noncopyable
{
public:
    explicit BinlogEventsDispatcher(const String & logger_name_ = "BinlogDispatcher", size_t max_bytes_in_buffer_ = 1_MiB, UInt64 max_flush_ms_ = 1000);
    ~BinlogEventsDispatcher();

    /// Moves all IBinlog objects to \a to if it has the same position
    /// Supports syncing to multiple dispatchers
    void syncTo(const BinlogEventsDispatcherPtr & to);

    /** Creates a binlog and starts the dispatching
      * binlog_read_from Source binlog to read events from
      * name Identifies the binlog, could be not unique
      * mysql_database_names Returns events only from these databases
      * max_bytes Defines a limit in bytes for this binlog
      *           Note: Dispatching will be stopped for all binlogs if bytes in queue increases this limit
      * max_waiting_ms Max wait time when max_bytes exceeded
      */
    BinlogPtr start(const BinlogPtr & binlog_read_from_,
                    const String & name = {},
                    const NameSet & mysql_database_names = {},
                    size_t max_bytes = 0,
                    UInt64 max_waiting_ms = 0);

    /** Creates a binlog if the dispatcher is started
      * executed_gtid_set Can be higher value than current,
      *                   otherwise not possible to attach
      * name Identifies the binlog, could be not unique
      * mysql_database_names Returns events only from these databases
      * max_bytes Defines a limit in bytes for this binlog
      * max_waiting_ms Max wait time when max_bytes exceeded
      */
    BinlogPtr attach(const String & executed_gtid_set,
                     const String & name = {},
                     const NameSet & mysql_database_names = {},
                     size_t max_bytes = 0,
                     UInt64 max_waiting_ms = 0);

    /// Cleans the destroyed binlogs up and returns true if empty
    bool cleanupBinlogsAndStop();

    /// Changes binlog_checksum for binlog_read_from
    void setBinlogChecksum(const String & checksum);

    Position getPosition() const;

    struct BinlogMetadata
    {
        String name;
        /// Position that was written to
        Position position_write;
        /// Position that was read from
        Position position_read;
        size_t size = 0;
        size_t bytes = 0;
        size_t max_bytes = 0;
        UInt64 max_waiting_ms = 0;
    };
    struct DispatcherMetadata
    {
        String name;
        Position position;
        float events_read_per_sec = 0;
        float bytes_read_per_sec = 0;
        float events_flush_per_sec = 0;
        float bytes_flush_per_sec = 0;
        std::vector<BinlogMetadata> binlogs;
    };
    DispatcherMetadata getDispatcherMetadata() const;

    struct Buffer
    {
        std::deque<BinlogEventPtr> events;
        size_t bytes = 0;
        Position position;
    };

private:
    bool cleanupLocked(const std::function<void(const std::shared_ptr<BinlogFromDispatcher> & binlog)> & fn = {});
    bool startLocked(const String & executed_gtid_set);
    void stopLocked();
    BinlogPtr createBinlogLocked(const String & name = {},
                                 const NameSet & mysql_database_names = {},
                                 size_t max_bytes = 0,
                                 UInt64 max_waiting_ms = 0,
                                 const Position & pos_initial = {},
                                 const Position & pos_wait = {});
    void syncToLocked(const BinlogEventsDispatcherPtr & to);
    bool trySyncLocked(BinlogEventsDispatcherPtr & to);
    void flushBufferLocked();
    void dispatchEvents();

    const String logger_name;
    const size_t max_bytes_in_buffer = 0;
    const UInt64 max_flush_ms = 0;
    LoggerPtr logger = nullptr;

    BinlogPtr binlog_read_from;

    Position position;
    std::vector<std::weak_ptr<BinlogEventsDispatcher>> sync_to;
    std::vector<std::weak_ptr<BinlogFromDispatcher>> binlogs;
    std::atomic_bool is_cancelled{false};
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::unique_ptr<ThreadFromGlobalPool> dispatching_thread;
    IBinlog::Checksum binlog_checksum = IBinlog::CRC32;
    bool is_started = false;
    Buffer buffer;
    float events_read_per_sec = 0;
    float bytes_read_per_sec = 0;
    UInt64 events_flush = 0;
    UInt64 events_flush_total_time = 0;
    float events_flush_per_sec = 0;
    UInt64 bytes_flush = 0;
    float bytes_flush_per_sec = 0;
};

}
