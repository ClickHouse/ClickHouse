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
    /// credentials, or a storage policy with one. With `check_remote_hosts`, also refuse a custom disk with an endpoint that
    /// `remote_url_allow_hosts` does not allow. For a table definition selecting a disk or a policy by name.
    void ensureDiskIsAllowed(const DiskPtr & disk, ContextPtr context, bool check_remote_hosts);
    void ensureStoragePolicyIsAllowed(const std::string & storage_policy_name, ContextPtr context, bool check_remote_hosts);
    /// `stored_definition`: the definition was read from the metadata the server stored for the table (short `ATTACH`),
    /// so its credentials markers are kept as they are: they keep the identity of the disk, and are not honored unless `attach`.
    std::string createCustomDisk(
        const ASTPtr & disk_function, ContextPtr context, bool attach, bool for_system_database = false, bool stored_definition = false);
}

}
