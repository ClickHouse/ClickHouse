#include <Disks/getOrCreateDiskFromAST.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>
#include <Disks/getDiskConfigurationFromAST.h>
#include <Disks/DiskSelector.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/isDiskFunction.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::string getOrCreateDiskFromDiskAST(const ASTFunction & function, ContextPtr context)
{
    /// We need a unique name for a created custom disk, but it needs to be the same
    /// after table is reattached or server is restarted, so take a hash of the disk
    /// configuration serialized ast as a disk name suffix.
    auto disk_setting_string = serializeAST(function, true);
    auto disk_name = DiskSelector::TMP_INTERNAL_DISK_PREFIX
        + toString(sipHash128(disk_setting_string.data(), disk_setting_string.size()));

    LOG_TRACE(
        &Poco::Logger::get("getOrCreateDiskFromDiskAST"),
        "Using disk name `{}` for custom disk {}",
        disk_name, disk_setting_string);

    auto result_disk = context->getOrCreateDisk(disk_name, [&](const DisksMap & disks_map) -> DiskPtr {
        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function.arguments.get());
        const auto & function_args = function_args_expr->children;
        auto config = getDiskConfigurationFromAST(disk_name, function_args, context);
        auto disk = DiskFactory::instance().create(disk_name, *config, disk_name, context, disks_map);
        /// Mark that disk can be used without storage policy.
        disk->markDiskAsCustom();
        return disk;
    });

    if (!result_disk->isRemote())
    {
        static constexpr auto custom_disks_base_dir_in_config = "custom_local_disks_base_directory";
        auto disk_path_expected_prefix = context->getConfigRef().getString(custom_disks_base_dir_in_config, "");

        if (disk_path_expected_prefix.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Base path for custom local disks must be defined in config file by `{}`",
                custom_disks_base_dir_in_config);

        if (!pathStartsWith(result_disk->getPath(), disk_path_expected_prefix))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Path of the custom local disk must be inside `{}` directory",
                disk_path_expected_prefix);
    }

    return disk_name;
}

}
