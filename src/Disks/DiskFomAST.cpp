#include <Disks/DiskFomAST.h>
#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>
#include <Disks/getDiskConfigurationFromAST.h>
#include <Disks/DiskSelector.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/isDiskFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_DISK;
}

std::string getOrCreateCustomDisk(DiskConfigurationPtr config, const std::string & serialization, ContextPtr context, bool attach)
{
    Poco::Util::AbstractConfiguration::Keys disk_settings_keys;
    config->keys(disk_settings_keys);


    // Check that no settings are defined when disk from the config is referred.
    if (disk_settings_keys.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk function has no arguments. Invalid disk description.");

    if (disk_settings_keys.size() == 1 && disk_settings_keys.front() == "name" && !attach)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk function `{}` has to have the other arguments which describe the disk. Invalid disk description.",
            serialization);
    }

    std::string disk_name;
    if (config->has("name"))
    {
        disk_name = config->getString("name");
    }

    if (!disk_name.empty())
    {
        if (disk_name.starts_with(DiskSelector::CUSTOM_DISK_PREFIX))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Disk name `{}` could not start with `{}`",
                    disk_name, DiskSelector::CUSTOM_DISK_PREFIX);

        if (auto disk = context->tryGetDisk(disk_name))
        {
            /// the disk is defined by config
            if (disk->isCustomDisk())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Disk with name `{}` already exist as a custom disk but the name does not start with `{}`",
                    disk_name,
                    DiskSelector::CUSTOM_DISK_PREFIX);

            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The disk `{}` is already exist. It is impossible to redefine it.", disk_name);
        }
    }

    auto disk_settings_hash = sipHash128(serialization.data(), serialization.size());

    std::string custom_disk_name;
    if (disk_name.empty())
    {
        /// We need a unique name for a created custom disk, but it needs to be the same
        /// after table is reattached or server is restarted, so take a hash of the disk
        /// configuration serialized ast as a disk name suffix.
        custom_disk_name = toString(DiskSelector::CUSTOM_DISK_PREFIX) + "noname_" + toString(disk_settings_hash);
    }
    else
    {
        custom_disk_name = toString(DiskSelector::CUSTOM_DISK_PREFIX) + disk_name;
    }

    auto result_disk = context->getOrCreateDisk(custom_disk_name, [&](const DisksMap & disks_map) -> DiskPtr {
        auto disk = DiskFactory::instance().create(
            disk_name, *config, /* config_path */"", context, disks_map, /* attach */attach, /* custom_disk */true);
        /// Mark that disk can be used without storage policy.
        disk->markDiskAsCustom(disk_settings_hash);
        return disk;
    });

    if (!result_disk->isCustomDisk())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Disk with name `{}` expected to be custom disk", disk_name);

    if (result_disk->getCustomDiskSettings() != disk_settings_hash && !attach)
        throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The disk `{}` is already configured as a custom disk in another table. It can't be redefined with different settings.",
                disk_name);

    if (!attach && !result_disk->isRemote())
    {
        static constexpr auto custom_local_disks_base_dir_in_config = "custom_local_disks_base_directory";
        auto disk_path_expected_prefix = context->getConfigRef().getString(custom_local_disks_base_dir_in_config, "");

        if (disk_path_expected_prefix.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Base path for custom local disks must be defined in config file by `{}`",
                custom_local_disks_base_dir_in_config);

        if (!pathStartsWith(result_disk->getPath(), disk_path_expected_prefix))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Path of the custom local disk must be inside `{}` directory",
                disk_path_expected_prefix);
    }

    return custom_disk_name;
}

class DiskConfigurationFlattener
{
public:
    struct Data
    {
        ContextPtr context;
        bool attach;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (isDiskFunction(ast))
        {
            const auto * function = ast->as<ASTFunction>();
            const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function->arguments.get());
            const auto & function_args = function_args_expr->children;
            auto config = getDiskConfigurationFromAST(function_args, data.context);
            auto disk_setting_string = serializeAST(*function);
            auto disk_name = getOrCreateCustomDisk(config, disk_setting_string, data.context, data.attach);
            ast = std::make_shared<ASTLiteral>(disk_name);
        }
    }
};


std::string DiskFomAST::createCustomDisk(const ASTPtr & disk_function_ast, ContextPtr context, bool attach)
{
    if (!isDiskFunction(disk_function_ast))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a disk function");

    auto ast = disk_function_ast->clone();

    using FlattenDiskConfigurationVisitor = InDepthNodeVisitor<DiskConfigurationFlattener, false>;
    FlattenDiskConfigurationVisitor::Data data{context, attach};
    FlattenDiskConfigurationVisitor{data}.visit(ast);

    auto disk_name = assert_cast<const ASTLiteral &>(*ast).value.get<String>();
    return disk_name;
}

std::string DiskFomAST::getConfigDefinedDisk(const std::string &disk_name, ContextPtr context)
{
    if (disk_name.starts_with(DiskSelector::CUSTOM_DISK_PREFIX))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk name `{}` could not start with `{}`",
                disk_name, DiskSelector::CUSTOM_DISK_PREFIX);

    if (auto result = context->tryGetDisk(disk_name))
        return disk_name;

    std::string custom_disk_name = DiskSelector::CUSTOM_DISK_PREFIX + disk_name;
    if (auto result = context->tryGetDisk(custom_disk_name))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk name `{}` is a custom disk that is used in other table."
            "That disk could not be used by a reference. The custom disk should be fully specified with a disk function.",
            disk_name);

    throw Exception(ErrorCodes::UNKNOWN_DISK, "Unknown disk {}", disk_name);
}

}
