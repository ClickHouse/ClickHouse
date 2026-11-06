#pragma once
#include <string>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class AccessRightsElements;
class SettingsChanges;

namespace DiskFromAST

{
    void ensureDiskIsNotCustom(const std::string & name, ContextPtr context);
    std::string createCustomDisk(const ASTPtr & disk_function, ContextPtr context, bool attach, bool for_system_database = false);

    /// The source grants needed to define every `disk(...)` in the subtree, including nested ones and those in setting values.
    AccessRightsElements getSourceAccess(const ASTPtr & ast);
    /// The same for the `disk` setting in `changes`.
    AccessRightsElements getSourceAccessOfSettings(const SettingsChanges & changes);
}

}
