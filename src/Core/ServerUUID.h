#pragma once

#include <Core/UUID.h>
#include <Common/Logger.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

class ServerUUID
{
    /// Defined out of line: a definition in the header gives every shared object its own copy.
    static UUID server_uuid;

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
