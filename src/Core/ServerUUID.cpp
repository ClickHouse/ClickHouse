#include <Core/ServerUUID.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_FILE;
    extern const int LOGICAL_ERROR;
}

UUID ServerUUID::get()
{
    if (server_uuid == UUIDHelpers::Nil &&
        (Context::getGlobalContextInstance()->getApplicationType() == Context::ApplicationType::SERVER ||
         Context::getGlobalContextInstance()->getApplicationType() == Context::ApplicationType::KEEPER))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ServerUUID is not initialized yet");
    return server_uuid;
}

void ServerUUID::load(const fs::path & server_uuid_file, Poco::Logger * log)
{
    server_uuid = loadServerUUID(server_uuid_file, log);
}

UUID loadServerUUID(const fs::path & server_uuid_file, Poco::Logger * log)
{
    /// Write a uuid file containing a unique uuid if the file doesn't already exist during server start.

    if (fs::exists(server_uuid_file))
    {
        try
        {
            UUID uuid;
            ReadBufferFromFile in(server_uuid_file);
            readUUIDText(uuid, in);
            assertEOF(in);
            return uuid;
        }
        catch (...)
        {
            /// As for now it's ok to just overwrite it, because persistency in not essential.
            LOG_ERROR(log, "Cannot read server UUID from file {}: {}. Will overwrite it",
                      server_uuid_file.string(), getCurrentExceptionMessage(true));
        }
    }

    try
    {
        UUID new_uuid = UUIDHelpers::generateV4();
        auto uuid_str = toString(new_uuid);
        WriteBufferFromFile out(server_uuid_file);
        out.write(uuid_str.data(), uuid_str.size());
        out.sync();
        out.finalize();
        return new_uuid;
    }
    catch (...)
    {
        throw Exception(ErrorCodes::CANNOT_CREATE_FILE, "Caught Exception {} while writing the Server UUID file {}",
                        getCurrentExceptionMessage(false), server_uuid_file.string());
    }
}

void ServerUUID::set(UUID & uuid)
{
    server_uuid = uuid;
}

void ServerUUID::setRandomForUnitTests()
{
    server_uuid = UUIDHelpers::generateV4();
}

}
