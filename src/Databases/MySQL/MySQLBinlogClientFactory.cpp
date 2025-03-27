#include <Databases/MySQL/MySQLBinlogClientFactory.h>

namespace DB::MySQLReplication
{

BinlogClientFactory & BinlogClientFactory::instance()
{
    static BinlogClientFactory ret;
    return ret;
}

BinlogClientPtr BinlogClientFactory::getClient(const String & host, UInt16 port, const String & user, const String & password, UInt64 max_bytes_in_buffer, UInt64 max_flush_ms)
{
    std::lock_guard lock(mutex);
    String binlog_client_name;
    WriteBufferFromString stream(binlog_client_name);
    stream << user << "@" << host << ":" << port;
    stream.finalize();
    String binlog_client_key = binlog_client_name + ":" + password;
    auto it = clients.find(binlog_client_key);
    BinlogClientPtr ret = it != clients.end() ? it->second.lock() : nullptr;
    if (ret)
        return ret;
    auto factory = std::make_shared<BinlogFromSocketFactory>(host, port, user, password);
    auto client = std::make_shared<BinlogClient>(factory, binlog_client_name, max_bytes_in_buffer, max_flush_ms);
    clients[binlog_client_key] = client;
    return client;
}

std::vector<BinlogClient::Metadata> BinlogClientFactory::getMetadata() const
{
    std::lock_guard lock(mutex);
    std::vector<BinlogClient::Metadata> ret;
    for (const auto & it : clients)
    {
        if (auto c = it.second.lock())
        {
            auto metadata = c->getMetadata();
            if (!metadata.dispatchers.empty())
                ret.push_back(metadata);
        }
    }
    return ret;
}

}
