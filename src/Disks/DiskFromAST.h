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
    /// The name of a disk described by a `disk(...)` function without a `name` argument. `serialization` is the
    /// function formatted with `formatWithSecretsOneLine`, after its nested `disk(...)` functions were replaced
    /// with the names of the disks they describe.
    std::string getGeneratedDiskName(const std::string & serialization);

    struct CustomDiskNames
    {
        /// The name of the disk described by the `disk(...)` function, generated if it has no `name` argument.
        std::string name;
        /// The value of its `disk` argument: the name of the disk it wraps, or empty.
        std::string wrapped_disk_name;
    };
    /// Resolves the names of a `disk(...)` function by the same rules as `createCustomDisk` for a definition loaded from
    /// existing metadata (including numeric values, `from_env` and `from_zk`), without creating the disk. The nested
    /// `disk(...)` functions must already be replaced with the names of the disks they describe.
    CustomDiskNames resolveCustomDiskNames(const ASTPtr & disk_function, ContextPtr context);
    CustomDiskFromAST createCustomDisk(const ASTPtr & disk_function, ContextPtr context, bool attach, bool for_system_database = false);
}

}
