#include <Storages/createDiskFromDiskAST.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <Disks/getDiskConfigurationFromAST.h>
#include <Disks/DiskSelector.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

bool isDiskFunction(ASTPtr ast)
{
    if (!ast)
        return false;

    const auto * function = ast->as<ASTFunction>();
    return function && function->name == "disk" && function->arguments->as<ASTExpressionList>();
}

std::string createDiskFromDiskAST(const ASTFunction & function, ContextPtr context)
{
    /// We need a unique name for a created custom disk, but it needs to be the same
    /// after table is reattached or server is restarted, so take a hash of the disk
    /// configuration serialized ast as a disk name suffix.
    auto disk_setting_string = serializeAST(function, true);
    auto disk_name = DiskSelector::TMP_DISK_PREFIX
        + toString(sipHash128(disk_setting_string.data(), disk_setting_string.size()));
    LOG_TRACE(&Poco::Logger::get("createDiskFromDiskAST"), "Using disk name `{}` for custom disk {}", disk_name, disk_setting_string);

    context->getOrCreateDisk(disk_name, [&](const DisksMap & disks_map) -> DiskPtr {
        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function.arguments.get());
        const auto & function_args = function_args_expr->children;
        auto config = getDiskConfigurationFromAST(disk_name, function_args, context);
        auto disk = DiskFactory::instance().create(disk_name, *config, disk_name, context, disks_map);
        /// Mark that disk can be used without storage policy.
        disk->markDiskAsCustom();
        return disk;
    });

    return disk_name;
}

}
