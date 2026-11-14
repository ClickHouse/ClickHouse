#pragma once
#include <memory>
#include <string>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class Field;
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

namespace DiskFromAST

{
    void ensureDiskIsNotCustom(const std::string & name, ContextPtr context);
    /// Rejects an inline `disk(...)` definition, for the settings that take only a disk name.
    void ensureDiskIsNotInline(const Field & value);
    /// Refuse, for a session without `s3_allow_server_credentials_in_user_queries`, a custom disk that uses the server's own
    /// credentials, or a storage policy with one. For a table definition selecting a disk or a policy by name.
    void ensureDiskIsAllowed(const DiskPtr & disk, ContextPtr context);
    void ensureStoragePolicyIsAllowed(const std::string & storage_policy_name, ContextPtr context);
    /// `stored_definition`: the definition was read from the metadata the server stored for the table (short `ATTACH`),
    /// so its credentials markers are kept as they are: they keep the identity of the disk, and are not honored unless `attach`.
    std::string createCustomDisk(
        const ASTPtr & disk_function, ContextPtr context, bool attach, bool for_system_database = false, bool stored_definition = false);

    /// Create the disk described by a disk function without registering it in the context: the disk
    /// lives only as long as the returned pointer, so it does not grow the global disk map. The same
    /// validation as `createCustomDisk` is applied. Used by queries that need a disk only for their
    /// own lifetime, such as the `mergeTreeParts` table function.
    DiskPtr createTransientDisk(const ASTPtr & disk_function, ContextPtr context);

    /// Run the validation of `createTransientDisk` without creating the disk, for queries that only
    /// need to know that the disk could be created, such as `DESCRIBE`.
    void validateTransientDisk(const ASTPtr & disk_function, ContextPtr context);
}

}
