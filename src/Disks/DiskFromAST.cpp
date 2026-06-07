#include <Disks/DiskFromAST.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Common/SipHash.h>
#include <Common/Config/ConfigProcessor.h>
#include <Disks/getDiskConfigurationFromAST.h>
#include <Disks/DiskSelector.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/isDiskFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static std::string getOrCreateCustomDisk(
    const ASTs & disk_args,
    const std::string & serialization,
    ContextPtr context,
    bool attach,
    DiskFromAST::CustomDiskRegistrationScope * scope)
{
    std::string default_path = "/etc/metrika.xml";

    const auto & server_config = context->getConfigRef();
    std::string include_from_path;
    if (server_config.has("include_from"))
        include_from_path = server_config.getString("include_from");
    else if (fs::exists(default_path))
        include_from_path = default_path;

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    {
        auto xml_document = getDiskConfigurationFromASTImpl(disk_args, context, attach);

        Poco::AutoPtr<Poco::XML::NamePool> name_pool(new Poco::XML::NamePool());
        Poco::XML::DOMParser dom_parser(name_pool);

        std::vector<std::pair<std::string, std::string>> substitutions;
        zkutil::ZooKeeperNodeCache zk_node_cache([&]() { return context->getZooKeeper(); });

        ConfigProcessor::processIncludes(
            xml_document,
            substitutions,
            include_from_path,
            /* throw_on_bad_incl= */!attach,
            dom_parser,
            getLogger("getOrCreateCustomDisk"),
            /*contributing_zk_paths=*/ {},
            /*contributing_files=*/ {},
            &zk_node_cache);

        config->load(xml_document);
    }

    Poco::Util::AbstractConfiguration::Keys disk_settings_keys;
    config->keys(disk_settings_keys);
    /// Check that no settings are defined when disk from the config is referred.
    if (disk_settings_keys.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk function must have arguments. Invalid disk description.");

    if (disk_settings_keys.size() == 1 && disk_settings_keys.front() == "name" && !attach)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk function `{}` must have other arguments apart from `name`, which describe disk configuration. Invalid disk description.",
            serialization);

    auto disk_settings_hash = sipHash128(serialization.data(), serialization.size());

    std::string disk_name;
    if (config->has("name"))
    {
        disk_name = config->getString("name");
    }
    else
    {
        /// We need a unique name for a created custom disk, but it needs to be the same
        /// after table is reattached or server is restarted, so take a hash of the disk
        /// configuration serialized ast as a disk name suffix.
        disk_name = DiskSelector::TMP_INTERNAL_DISK_PREFIX + toString(disk_settings_hash);
    }

    bool newly_registered = false;
    auto disk = context->getOrCreateDisk(disk_name, [&](const DisksMap & disks_map) -> DiskPtr {
        auto result = DiskFactory::instance().create(
            disk_name, *config, /* config_path */"", context, disks_map, /* attach */attach, /* custom_disk */true);
        /// Mark that disk can be used without storage policy.
        result->markDiskAsCustom(disk_settings_hash);
        newly_registered = true;
        return result;
    });

    /// If we just added a new custom disk to the global selector AND the caller is running a
    /// dry-run / validation pass, record the name in the scope so it can be rolled back if
    /// the validation later throws (issue #63019: `MODIFY SETTING disk = disk(...)` rejected
    /// by the storage-policy migration guard would otherwise leak the disk name registration).
    if (scope && newly_registered)
        scope->track(disk_name);

    if (!disk->isCustomDisk())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Disk `{}` already exists and is described by the config."
            " It is impossible to redefine it.",
            disk_name);

    if (disk->getCustomDiskSettings() != disk_settings_hash && !attach)
        throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The disk `{}` is already configured as a custom disk in another table. It can't be redefined with different settings.",
                disk_name);

    if (!attach && !disk->isRemote() && disk->getName() != "backup")
    {
        static constexpr auto custom_local_disks_base_dir_in_config = "custom_local_disks_base_directory";
        auto disk_path_expected_prefix = context->getConfigRef().getString(custom_local_disks_base_dir_in_config, "");

        if (disk_path_expected_prefix.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Base path for custom local disks must be defined in config file by `{}`",
                custom_local_disks_base_dir_in_config);

        if (!pathStartsWith(disk->getPath(), disk_path_expected_prefix))
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
        bool attach;
        DiskFromAST::CustomDiskRegistrationScope * scope;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (isDiskFunction(ast))
        {
            const auto * function = ast->as<ASTFunction>();
            const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function->arguments.get());
            const auto & function_args = function_args_expr->children;
            auto disk_setting_string = function->formatWithSecretsOneLine();
            auto disk_name = getOrCreateCustomDisk(function_args, disk_setting_string, data.context, data.attach, data.scope);
            ast = make_intrusive<ASTLiteral>(disk_name);
        }
    }
};


std::string DiskFromAST::createCustomDisk(const ASTPtr & disk_function_ast, ContextPtr context, bool attach, CustomDiskRegistrationScope * scope)
{
    if (!isDiskFunction(disk_function_ast))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a disk function");

    auto ast = disk_function_ast->clone();

    using FlattenDiskConfigurationVisitor = InDepthNodeVisitor<DiskConfigurationFlattener, false>;
    FlattenDiskConfigurationVisitor::Data data{context, attach, scope};
    FlattenDiskConfigurationVisitor{data}.visit(ast);

    return assert_cast<const ASTLiteral &>(*ast).value.safeGet<String>();
}

void DiskFromAST::ensureDiskIsNotCustom(const std::string & disk_name, ContextPtr context)
{
    auto disk = context->getDisk(disk_name);

    if (disk->isCustomDisk())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Disk name `{}` is a custom disk that is used in other table. "
            "That disk could not be used by a reference by other tables. The custom disk should be fully specified with a disk function.",
            disk_name);
}

void DiskFromAST::convertCustomDiskField(Field & value, ContextPtr context, bool attach, CustomDiskRegistrationScope * scope)
{
    CustomType custom;
    ASTPtr value_as_custom_ast = nullptr;
    if (value.tryGet<CustomType>(custom) && 0 == strcmp(custom.getTypeName(), "AST"))
        value_as_custom_ast = dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast;

    if (value_as_custom_ast && isDiskFunction(value_as_custom_ast))
    {
        auto disk_name = createCustomDisk(value_as_custom_ast, context, attach, scope);
        LOG_DEBUG(getLogger("DiskFromAST"), "Created custom disk {}", disk_name);
        value = disk_name;
    }
    else
    {
        ensureDiskIsNotCustom(value.safeGet<String>(), context);
    }
}

void DiskFromAST::convertCustomDiskSettings(SettingsChanges & changes, ContextPtr context, bool attach, CustomDiskRegistrationScope * scope)
{
    for (auto & change : changes)
    {
        if (change.name == "disk")
            convertCustomDiskField(change.value, context, attach, scope);
    }
}

DiskFromAST::CustomDiskRegistrationScope::CustomDiskRegistrationScope(ContextPtr context_)
    : context(std::move(context_))
{
}

DiskFromAST::CustomDiskRegistrationScope::~CustomDiskRegistrationScope() noexcept
{
    if (committed || registered_disk_names.empty())
        return;
    /// Roll back any custom disk registrations performed during this scope. Any failure during
    /// removal is swallowed: this destructor must be `noexcept` because it runs during stack
    /// unwinding for exception cases. A best-effort log makes a real bug observable.
    for (const auto & disk_name : registered_disk_names)
    {
        try
        {
            context->removeCustomDiskAndStoragePolicy(disk_name);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("CustomDiskRegistrationScope"),
                fmt::format("Failed to roll back custom disk registration for {}", disk_name));
        }
    }
}

void DiskFromAST::CustomDiskRegistrationScope::track(const std::string & disk_name)
{
    registered_disk_names.push_back(disk_name);
}

}
