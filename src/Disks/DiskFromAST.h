#pragma once
#include <string>
#include <Disks/CustomDiskRegistration.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

struct CustomDiskFromAST
{
    /// Name of the disk described by the outermost `disk(...)` function.
    std::string disk_name;
    /// Registration for that disk, which owns the registrations of every disk nested in its
    /// definition. It has to be kept for as long as the table or database being created uses the
    /// disk. It is `nullptr` when the disk is not a custom one, that is, when the name belongs to a
    /// disk from the configuration.
    CustomDiskRegistrationPtr registration;
};

namespace DiskFromAST

{
    void ensureDiskIsNotCustom(const std::string & name, ContextPtr context);
    CustomDiskFromAST createCustomDisk(const ASTPtr & disk_function, ContextPtr context, bool attach, bool for_system_database = false);
}

}
