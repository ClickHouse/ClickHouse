#include <Disks/ObjectStorages/S3/checkBatchRemove.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Core/Types.h>
#include <Core/ServerUUID.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFile.h>

namespace DB
{

static String getServerUUID()
{
    UUID server_uuid = ServerUUID::get();
    if (server_uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Server UUID is not initialized");
    return toString(server_uuid);
}

bool checkBatchRemove(S3ObjectStorage & storage, const String & key_with_trailing_slash)
{
    /// NOTE: key_with_trailing_slash is the disk prefix, it is required
    /// because access is done via S3ObjectStorage not via IDisk interface
    /// (since we don't have disk yet).
    const String path = fmt::format("{}clickhouse_remove_objects_capability_{}", key_with_trailing_slash, getServerUUID());
    StoredObject object(path);
    try
    {
        auto file = storage.writeObject(object, WriteMode::Rewrite);
        file->write("test", 4);
        file->finalize();
    }
    catch (...)
    {
        try
        {
            storage.removeObject(object);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
        /// We don't have write access, therefore no information about batch remove.
        return true;
    }
    try
    {
        /// Uses `DeleteObjects` request (batch delete).
        storage.removeObjects({object});
        return true;
    }
    catch (const Exception &)
    {
        try
        {
            storage.removeObject(object);
        }
        catch (...) // NOLINT(bugprone-empty-catch)
        {
        }
        return false;
    }
}

}
