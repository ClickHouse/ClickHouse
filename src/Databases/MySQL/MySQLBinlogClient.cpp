#include "MySQLBinlogClient.h"
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::MySQLReplication
{

BinlogClient::BinlogClient(const BinlogFactoryPtr & factory_,
                           const String & name,
                           UInt64 max_bytes_in_buffer_,
                           UInt64 max_flush_ms_)
    : factory(factory_)
    , binlog_client_name(name)
    , max_bytes_in_buffer(max_bytes_in_buffer_)
    , max_flush_ms(max_flush_ms_)
    , logger(getLogger("BinlogClient(" + name + ")"))
{
}

BinlogPtr BinlogClient::createBinlog(const String & executed_gtid_set,
                                     const String & name,
                                     const NameSet & mysql_database_names,
                                     size_t max_bytes,
                                     UInt64 max_waiting_ms)
{
    std::lock_guard lock(mutex);
    BinlogPtr ret;
    for (auto it = dispatchers.begin(); it != dispatchers.end();)
    {
        auto & dispatcher = *it;
        if (!ret)
        {
            const auto metadata = dispatcher->getDispatcherMetadata();
            LOG_DEBUG(logger, "({} -> {}): Trying dispatcher: {}, size: {} -> {}:{}.{}",
                              name, executed_gtid_set, metadata.name, metadata.binlogs.size(),
                              metadata.position.binlog_name, metadata.position.gtid_sets.toString(), metadata.position.binlog_pos);
            ret = dispatcher->attach(executed_gtid_set, name, mysql_database_names, max_bytes, max_waiting_ms);
            if (ret)
                LOG_DEBUG(logger, "({} -> {}): Reused dispatcher: {}, size: {} -> {}:{}.{}",
                                  name, executed_gtid_set, metadata.name, metadata.binlogs.size(),
                                  metadata.position.binlog_name, metadata.position.gtid_sets.toString(), metadata.position.binlog_pos);
        }

        if (dispatcher->cleanupBinlogsAndStop())
        {
            const auto metadata = dispatcher->getDispatcherMetadata();
            LOG_DEBUG(logger, "({} -> {}): Deleting dispatcher: {}, size: {}, total dispatchers: {}",
                              name, executed_gtid_set, metadata.name, metadata.binlogs.size(), dispatchers.size());
            it = dispatchers.erase(it);
            continue;
        }
        ++it;
    }

    if (!ret)
    {
        String dispatcher_name = name + ":" + std::to_string(dispatchers_count++);
        LOG_DEBUG(logger, "({} -> {}): Creating dispatcher: {}, total dispatchers: {}",
                          name, executed_gtid_set, dispatcher_name, dispatchers.size());
        auto dispatcher = std::make_shared<BinlogEventsDispatcher>(dispatcher_name, max_bytes_in_buffer, max_flush_ms);
        if (!binlog_checksum.empty())
            dispatcher->setBinlogChecksum(binlog_checksum);
        for (const auto & it : dispatchers)
            dispatcher->syncTo(it);
        ret = dispatcher->start(factory->createBinlog(executed_gtid_set), name, mysql_database_names, max_bytes, max_waiting_ms);
        if (!ret)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not create binlog: {}", executed_gtid_set);
        dispatchers.push_back(dispatcher);
    }

    return ret;
}

BinlogClient::Metadata BinlogClient::getMetadata() const
{
    std::lock_guard lock(mutex);
    Metadata ret;
    ret.binlog_client_name = binlog_client_name;
    for (const auto & dispatcher : dispatchers)
    {
        auto metadata = dispatcher->getDispatcherMetadata();
        if (!metadata.binlogs.empty())
            ret.dispatchers.push_back(metadata);
    }
    return ret;
}

void BinlogClient::setBinlogChecksum(const String & checksum)
{
    std::lock_guard lock(mutex);
    if (binlog_checksum != checksum)
    {
        LOG_DEBUG(logger, "Setting binlog_checksum: {} -> {}, total dispatchers: {}", binlog_checksum, checksum, dispatchers.size());
        binlog_checksum = checksum;
        for (const auto & dispatcher : dispatchers)
            dispatcher->setBinlogChecksum(checksum);
    }
}

}
