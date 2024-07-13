#pragma once

#include <Databases/MySQL/MySQLBinlogClient.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

namespace DB::MySQLReplication
{

/** Global instance to create or reuse MySQL Binlog Clients.
  * If a binlog client already exists for specific params,
  * it will be returned and reused to read binlog events from MySQL.
  * Otherwise new instance will be created.
  */
class BinlogClientFactory final : boost::noncopyable
{
public:
    static BinlogClientFactory & instance();

    BinlogClientPtr getClient(const String & host,
                              UInt16 port,
                              const String & user,
                              const String & password,
                              UInt64 max_bytes_in_buffer,
                              UInt64 max_flush_ms);

    /// Returns info of all registered clients
    std::vector<BinlogClient::Metadata> getMetadata() const;

private:
    BinlogClientFactory() = default;

    // Keeps track of already destroyed clients
    std::unordered_map<String, std::weak_ptr<BinlogClient>> clients;
    mutable std::mutex mutex;
};

}
