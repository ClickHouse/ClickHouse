#include <Disks/getOrCreateDiskFromAST.h>
#include <Common/logger_useful.h>
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
    extern const int BAD_ARGUMENTS;
}

namespace
{
    std::string getOrCreateDiskFromDiskAST(const ASTFunction & function, ContextPtr context)
    {
        std::string disk_name;
        if (function.name == "disk")
        {
            /// We need a unique name for a created custom disk, but it needs to be the same
            /// after table is reattached or server is restarted, so take a hash of the disk
            /// configuration serialized ast as a disk name suffix.
            auto disk_setting_string = serializeAST(function, true);
            disk_name = DiskSelector::TMP_INTERNAL_DISK_PREFIX
                + toString(sipHash128(disk_setting_string.data(), disk_setting_string.size()));
        }
        else
        {
            static constexpr std::string_view custom_disk_prefix = "disk_";

            if (function.name.size() <= custom_disk_prefix.size() || !function.name.starts_with(custom_disk_prefix))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid disk name: {}", function.name);

            disk_name = function.name.substr(custom_disk_prefix.size());
        }

        auto result_disk = context->getOrCreateDisk(disk_name, [&](const DisksMap & disks_map) -> DiskPtr {
            const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function.arguments.get());
            const auto & function_args = function_args_expr->children;
            auto config = getDiskConfigurationFromAST(disk_name, function_args, context);
            auto disk = DiskFactory::instance().create(disk_name, *config, disk_name, context, disks_map);
            /// Mark that disk can be used without storage policy.
            disk->markDiskAsCustom();
            return disk;
        });

        if (!result_disk->isCustomDisk())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk with name `{}` already exist", disk_name);

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

    class DiskConfigurationFlattener
    {
    public:
        struct Data
        {
            ContextPtr context;
        };

        static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

        static void visit(ASTPtr & ast, Data & data)
        {
            if (isDiskFunction(ast))
            {
                auto disk_name = getOrCreateDiskFromDiskAST(*ast->as<ASTFunction>(), data.context);
                ast = std::make_shared<ASTLiteral>(disk_name);
            }
        }
    };

    /// Visits children first.
    using FlattenDiskConfigurationVisitor = InDepthNodeVisitor<DiskConfigurationFlattener, false>;
}


std::string getOrCreateDiskFromDiskAST(const ASTPtr & disk_function, ContextPtr context)
{
    if (!isDiskFunction(disk_function))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a disk function");

    auto ast = disk_function->clone();

    FlattenDiskConfigurationVisitor::Data data{context};
    FlattenDiskConfigurationVisitor{data}.visit(ast);

    auto disk_name = assert_cast<const ASTLiteral &>(*ast).value.get<String>();
    LOG_TRACE(&Poco::Logger::get("getOrCreateDiskFromDiskAST"), "Result disk name: {}", disk_name);
    return disk_name;
}

}
