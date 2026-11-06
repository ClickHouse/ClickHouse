#pragma once

#include <Core/UUID.h>
#include <Common/Logger.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

class ServerUUID
{
    inline static UUID server_uuid = UUIDHelpers::Nil;

public:
    /// Returns persistent UUID of current clickhouse-server or clickhouse-keeper instance.
    static UUID get();

    /// Loads server UUID from file or creates new one. Should be called on daemon startup.
    static void load(const fs::path & server_uuid_file, Poco::Logger * log);

    /// Sets specific server UUID.
    static void set(UUID & uuid);

    static void setRandomForUnitTests();
};

UUID loadServerUUID(const fs::path & server_uuid_file, Poco::Logger * log);

}
