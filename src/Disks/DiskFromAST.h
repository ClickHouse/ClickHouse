#pragma once
#include <memory>
#include <string>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

namespace DiskFromAST

{
    void ensureDiskIsNotCustom(const std::string & name, ContextPtr context);
    std::string createCustomDisk(const ASTPtr & disk_function, ContextPtr context, bool attach, bool for_system_database = false);

    /// Create the disk described by a disk function without registering it in the context: the disk
    /// lives only as long as the returned pointer, so it does not grow the global disk map. The same
    /// validation as `createCustomDisk` is applied. Used by queries that need a disk only for their
    /// own lifetime, such as the `mergeTreeParts` table function.
    DiskPtr createTransientDisk(const ASTPtr & disk_function, ContextPtr context);
}

}
