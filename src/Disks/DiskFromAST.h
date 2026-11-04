#pragma once
#include <string>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

namespace DiskFromAST

{
    void ensureDiskIsNotCustom(const std::string & name, ContextPtr context);
    /// `stored_definition`: the definition comes from stored metadata through a user short `ATTACH`, not from the query.
    std::string createCustomDisk(
        const ASTPtr & disk_function, ContextPtr context, bool attach, bool stored_definition, bool for_system_database = false);
}

}
