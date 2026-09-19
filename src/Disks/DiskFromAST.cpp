#include <Disks/DiskFromAST.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/filesystemHelpers.h>
#include <Common/SipHash.h>
#include <Common/Config/ConfigProcessor.h>
#include <Disks/getDiskConfigurationFromAST.h>
#include <Disks/DiskSelector.h>
#include <Disks/IDisk.h>
#include <Disks/DiskBackup.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/isDiskFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Every location on the local filesystem that a disk defined in SQL names has to be inside the
/// directory configured by this setting.
static constexpr auto custom_local_disks_base_dir_in_config = "custom_local_disks_base_directory";

static String getCustomLocalDisksBaseDirectory(const ContextPtr & context)
{
    auto base_directory = context->getConfigRef().getString(custom_local_disks_base_dir_in_config, "");

    if (base_directory.empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Base path for custom local disks must be defined in config file by `{}`",
            custom_local_disks_base_dir_in_config);

    return base_directory;
}

/// A location named by a disk definition has to be inside the base directory. It is compared as it
/// was given: every such location is used that way as well.
static void checkCustomDiskPathIsAllowed(const String & path, const ContextPtr & context)
{
    auto base_directory = getCustomLocalDisksBaseDirectory(context);

    if (!pathStartsWith(path, base_directory))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Path of the custom local disk must be inside `{}` directory",
            base_directory);
}

/// Whether the backend described at `prefix` keeps its data on the local filesystem, in the
/// directory named by `<prefix>path`. `DiskLocal` is spelled `local`, `local_blob_storage` is the
/// compatibility spelling of an object storage over the local filesystem, and the object storage
/// types backed by the local filesystem all start with `local` as well - so one prefix test covers
/// all of them. `object_storage_type` names the backend of a `type = object_storage` disk and of
/// every `locations.<name>` child, and `type` is the spelling that stands for it everywhere else,
/// which is the order `ObjectStorageFactory::create` reads them in.
static bool namesLocalFilesystem(const Poco::Util::AbstractConfiguration & config, const String & prefix)
{
    auto type = config.getString(prefix + "object_storage_type", "");
    if (type.empty())
        type = config.getString(prefix + "type", "");
    return type.starts_with("local");
}

/// The locations on the local filesystem that the disk definition names itself. A definition that
/// refers to a disk of the server configuration (`disk = '<name>'`, as a `cache` or an `encrypted`
/// disk does) inherits the location of that disk, which the administrator chose and the query did
/// not, so it is left alone. Every location is used exactly as it was given, so it is checked that
/// way too, and a relative one - which the filesystem resolves against the working directory of the
/// process, a directory that is not a property of the server - is inside the base directory only if
/// the server happens to run from the right place.
static void checkCustomDiskDefinitionPaths(const Poco::Util::AbstractConfiguration & config, const ContextPtr & context)
{
    /// The disk root, and every `locations.<name>` child: a multi-location `DiskObjectStorage`
    /// builds one object storage per child, each with its own backend and its own `path`, so an
    /// `include` can hide a local child behind a root that names a remote backend
    /// (see `RegisterDiskObjectStorage`).
    std::vector<String> prefixes{""};
    if (config.has("locations"))
    {
        Poco::Util::AbstractConfiguration::Keys locations;
        config.keys("locations", locations);
        for (const auto & location : locations)
            prefixes.push_back("locations." + location + ".");
    }

    for (const auto & prefix : prefixes)
        if (namesLocalFilesystem(config, prefix) && config.has(prefix + "path"))
            checkCustomDiskPathIsAllowed(config.getString(prefix + "path"), context);

    /// The metadata of a disk is written to the local filesystem whenever `metadata_path` is given.
    /// Its default, `<clickhouse path>/disks/<name>/`, needs no check of its own: the check of the
    /// disk name keeps it inside the directory that the server manages itself.
    if (config.has("metadata_path"))
        checkCustomDiskPathIsAllowed(config.getString("metadata_path"), context);
}

static std::string getOrCreateCustomDisk(
    const ASTs & disk_args,
    const std::string & serialization,
    ContextPtr context,
    bool attach,
    bool for_system_database)
{
    const auto & server_config = context->getConfigRef();
    std::string include_from_path = server_config.getString("include_from", "");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    {
        DynamicS3DiskCredentialInfo s3_disk_info;
        auto xml_document = getDiskConfigurationFromASTImpl(disk_args, context, attach, &s3_disk_info, for_system_database);

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

        /// Applied after `processIncludes`. If the pre-resolution check already decided the disk must load
        /// anonymously, enforce it now (an `include` cannot re-introduce server credentials). Otherwise
        /// re-validate the resolved config, so an `include` that injects an S3 backend with server-managed
        /// auth (past a literal non-S3 `type` in the AST) is still caught.
        if (s3_disk_info.load_anonymously)
            forceAnonymousS3DiskConfig(*config);
        else
            validateResolvedS3DiskCredentials(*config, context, attach, s3_disk_info);
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

        /// The name is used verbatim as a path component of the state the server keeps for the disk:
        /// the metadata storage builds `<clickhouse path>/disks/<name>/` and creates that directory
        /// (see `MetadataStorageFactory`). A name that contains a path separator or `..` therefore
        /// relocates server-managed state to any directory the server can write to - `user_scripts/`,
        /// `user_files/` and `format_schemas/` among them.
        if (disk_name.empty() || disk_name == "." || disk_name == ".."
            || disk_name.find_first_of("/\\") != std::string::npos || disk_name.contains('\0'))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Disk name `{}` is invalid: it must not be empty and must not contain a path separator or `..`",
                disk_name);
    }
    else
    {
        /// We need a unique name for a created custom disk, but it needs to be the same
        /// after table is reattached or server is restarted, so take a hash of the disk
        /// configuration serialized ast as a disk name suffix.
        disk_name = DiskSelector::TMP_INTERNAL_DISK_PREFIX + toString(disk_settings_hash);
    }

    /// Checked before the disk is created, so that a rejected definition creates no directory and
    /// leaves no disk behind for the statements that follow.
    if (!attach)
        checkCustomDiskDefinitionPaths(*config, context);

    auto disk = context->getOrCreateDisk(disk_name, [&](const DisksMap & disks_map) -> DiskPtr {
        auto result = DiskFactory::instance().create(
            disk_name, *config, /* config_path */"", context, disks_map, /* attach */attach, /* custom_disk */true);
        /// Mark that disk can be used without storage policy.
        result->markDiskAsCustom(disk_settings_hash);

        /// A backstop for the local disks whose directory the definition does not name outright -
        /// a `default` local disk answers with the path of the server, an `encrypted` disk with the
        /// path of the disk it wraps. `DiskBackup` serves the files of a backup rather than a
        /// directory, and is exempt by its type: its name, which the query chooses, says nothing.
        /// Checked here rather than after `getOrCreateDisk` returns, so that a rejected disk is not
        /// registered and usable by the statements that follow.
        if (!attach && !result->isRemote() && !typeid_cast<const DiskBackup *>(result.get()))
            checkCustomDiskPathIsAllowed(result->getPath(), context);

        return result;
    });

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

    return disk_name;
}

class DiskConfigurationFlattener
{
public:
    struct Data
    {
        ContextPtr context;
        bool attach;
        bool for_system_database;
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
            auto disk_name = getOrCreateCustomDisk(function_args, disk_setting_string, data.context, data.attach, data.for_system_database);
            ast = make_intrusive<ASTLiteral>(disk_name);
        }
    }
};

/// Persist the credential opt-in into the stored disk definition. For each leaf dynamic S3 disk that resolves
/// server-managed credentials and is currently allowed (the session opted in), add a `_server_credentials_allowed`
/// marker, so on reload the disk is not re-restricted (the marker is honored only when loading from metadata).
/// Operates on the original AST (not the flattening clone), so the marker reaches the stored metadata.
class ServerCredentialMarkerInjector
{
public:
    struct Data
    {
        ContextPtr context;
        bool for_system_database;
    };

    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (!isDiskFunction(ast))
            return;

        auto * function = ast->as<ASTFunction>();
        auto * args_expr = function->arguments ? function->arguments->as<ASTExpressionList>() : nullptr;
        if (!args_expr)
            return;
        auto & args = args_expr->children;

        auto is_marker = [](const ASTPtr & arg)
        {
            const auto * eq = arg->as<ASTFunction>();
            if (!eq || eq->name != "equals" || !eq->arguments || eq->arguments->children.size() != 2)
                return false;
            const auto * key = eq->arguments->children[0]->as<ASTIdentifier>();
            return key && key->name() == "_server_credentials_allowed";
        };

        /// Strip any marker the user put in the definition. This runs only on a fresh create (not on metadata
        /// load), so a server-written marker in stored metadata is never stripped. As a result a persisted
        /// marker is only ever one this injector wrote below for a disk that actually needed server credentials
        /// while the session had the opt-in -- a user cannot pre-seed it to bypass the restriction on reload.
        args.erase(std::remove_if(args.begin(), args.end(), is_marker), args.end());

        /// A wrapper disk (a nested `disk = disk(...)`) is not itself an S3 disk; the marker belongs on the
        /// inner leaf S3 disk, which the visitor reaches separately. (Markers were already stripped above.)
        for (const auto & arg : args)
        {
            const auto * eq = arg->as<ASTFunction>();
            if (eq && eq->name == "equals" && eq->arguments && eq->arguments->children.size() == 2
                && isDiskFunction(eq->arguments->children[1]))
                return;
        }

        DynamicS3DiskCredentialInfo info;
        getDiskConfigurationFromASTImpl(args, data.context, /* is_loading_from_existing_metadata */ false, &info, data.for_system_database);
        if (info.persist_server_credentials_allowance)
            args.push_back(makeASTFunction(
                "equals",
                make_intrusive<ASTIdentifier>("_server_credentials_allowed"),
                make_intrusive<ASTLiteral>(static_cast<UInt64>(1))));
    }
};


std::string DiskFromAST::createCustomDisk(const ASTPtr & disk_function_ast, ContextPtr context, bool attach, bool for_system_database)
{
    if (!isDiskFunction(disk_function_ast))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a disk function");

    /// On a fresh create, persist the credential opt-in into the definition (a `_server_credentials_allowed`
    /// marker) so the disk keeps working across restart. This must happen BEFORE flattening, because the custom
    /// disk's name is a hash of its definition: the marker has to be part of that definition so the disk
    /// resolves to the same name (and storage path) when reloaded from metadata. The marker itself is an unknown
    /// key to the disk factory and is ignored when building the client.
    if (!attach)
    {
        ASTPtr to_mark = disk_function_ast;
        using MarkerInjector = InDepthNodeVisitor<ServerCredentialMarkerInjector, false>;
        MarkerInjector::Data inject_data{context, for_system_database};
        MarkerInjector{inject_data}.visit(to_mark);
    }

    auto ast = disk_function_ast->clone();

    using FlattenDiskConfigurationVisitor = InDepthNodeVisitor<DiskConfigurationFlattener, false>;
    FlattenDiskConfigurationVisitor::Data data{context, attach, for_system_database};
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

}
