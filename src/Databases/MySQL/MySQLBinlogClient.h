#pragma once

#include <Databases/MySQL/MySQLBinlog.h>
#include <Databases/MySQL/MySQLBinlogEventsDispatcher.h>

namespace DB::MySQLReplication
{

/** It is supposed to reduce the number of connections to remote MySQL binlog by reusing one connection between several consumers.
  * Such reusing of the connection makes the time of reading from the remote binlog independent to number of the consumers.
  * It tracks a list of BinlogEventsDispatcher instances for consumers with different binlog position.
  * The dispatchers with the same binlog position will be merged to one.
  */
class BinlogClient
{
public:
    explicit BinlogClient(const BinlogFactoryPtr & factory,
                 const String & name = {},
                 UInt64 max_bytes_in_buffer_ = DBMS_DEFAULT_BUFFER_SIZE,
                 UInt64 max_flush_ms_ = 1000);
    BinlogClient(const BinlogClient & other) = delete;
    ~BinlogClient() = default;
    BinlogClient & operator=(const BinlogClient & other) = delete;

    /// Creates a binlog to receive events
    BinlogPtr createBinlog(const String & executed_gtid_set = {},
                           const String & name = {},
                           const NameSet & mysql_database_names = {},
                           size_t max_bytes = 0,
                           UInt64 max_waiting_ms = 0);

    /// The binlog checksum is related to entire connection
    void setBinlogChecksum(const String & checksum);

    struct Metadata
    {
        String binlog_client_name;
        std::vector<BinlogEventsDispatcher::DispatcherMetadata> dispatchers;
    };
    /// Returns only not empty dispatchers
    Metadata getMetadata() const;

private:
    BinlogFactoryPtr factory;
    const String binlog_client_name;
    UInt64 max_bytes_in_buffer = 0;
    UInt64 max_flush_ms = 0;
    std::vector<BinlogEventsDispatcherPtr> dispatchers;
    String binlog_checksum;
    mutable std::mutex mutex;
    LoggerPtr logger = nullptr;
    int dispatchers_count = 0;
};

using BinlogClientPtr = std::shared_ptr<BinlogClient>;

}
