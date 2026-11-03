#pragma once
#include <string>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

namespace DiskFromAST

{
    void ensureDiskIsNotCustom(const std::string & name, ContextPtr context);
    /// `stored_definition`: the definition was read from the metadata the server stored for the table (short `ATTACH`),
    /// so its credentials markers are kept as they are: they keep the identity of the disk, and are not honored unless `attach`.
    std::string createCustomDisk(
        const ASTPtr & disk_function, ContextPtr context, bool attach, bool for_system_database = false, bool stored_definition = false);
}

}
