#pragma once
#include <Core/UUID.h>
#include <filesystem>

namespace fs = std::filesystem;
namespace Poco
{
    class Logger;
}

namespace DB
{

class ServerUUID
{
    inline static UUID server_uuid = UUIDHelpers::Nil;

public:
    /// Returns persistent UUID of current clickhouse-server or clickhouse-keeper instance.
    static UUID get() { return server_uuid; }

    /// Loads server UUID from file or creates new one. Should be called on daemon startup.
    static void load(const fs::path & server_uuid_file, Poco::Logger * log);
};

}
