#include <Disks/DiskFromAST.h>
#include <Disks/getDiskConfigurationFromAST.h>
#include <Disks/DiskSelector.h>
#include <Common/assert_cast.h>
#include <Common/checkStackSize.h>
#include <Common/SipHash.h>
#include <Common/Config/ConfigProcessor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/isDiskFunction.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>
#include <Parsers/IAST.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>

#include <algorithm>
#include <memory>
#include <set>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
}

/// Resolve `include`, `from_env` and `from_zk` in a disk definition.
static Poco::AutoPtr<Poco::Util::XMLConfiguration> resolveDiskConfiguration(
    Poco::AutoPtr<Poco::XML::Document> xml_document, ContextPtr context, bool attach)
{
    Poco::AutoPtr<Poco::XML::NamePool> name_pool(new Poco::XML::NamePool());
    Poco::XML::DOMParser dom_parser(name_pool);

    std::vector<std::pair<std::string, std::string>> substitutions;
    zkutil::ZooKeeperNodeCache zk_node_cache([&]() { return context->getZooKeeper(); });

    ConfigProcessor::processIncludes(
        xml_document,
        substitutions,
        context->getConfigRef().getString("include_from", ""),
        /* throw_on_bad_incl= */!attach,
        dom_parser,
        getLogger("getOrCreateCustomDisk"),
        /*contributing_zk_paths=*/ {},
        /*contributing_files=*/ {},
        &zk_node_cache);

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config(new Poco::Util::XMLConfiguration());
    config->load(xml_document);
    return config;
}

static std::string getCredentialsMarker(const ASTPtr & arg)
{
    const auto * eq = arg->as<ASTFunction>();
    if (!eq || eq->name != "equals" || !eq->arguments || eq->arguments->children.size() < 2)
        return {};
    const auto * key = eq->arguments->children[0]->as<ASTIdentifier>();
    if (key && (key->name() == S3_SERVER_CREDENTIALS_MARKER || key->name() == AZURE_SERVER_CREDENTIALS_MARKER))
        return key->name();
    return {};
}

static bool hasCredentialsMarker(const ASTs & args, const std::string & marker)
{
    return std::ranges::any_of(args, [&](const ASTPtr & arg) { return getCredentialsMarker(arg) == marker; });
}

static bool isRegisteredCustomDisk(ContextPtr context, const std::string & disk_name, UInt128 disk_settings_hash)
{
    const auto disks = context->getDisksMap();
    const auto it = disks.find(disk_name);
    return it != disks.end() && it->second->isCustomDisk() && it->second->getCustomDiskSettings() == disk_settings_hash;
}

/// `serializations` holds the definition, then the same definition with other credentials markers; `adds_marker` tells
/// which of them add a marker. The returned `chosen` is the index of the one the disk was resolved by.
static std::string getOrCreateCustomDisk(
    const ASTs & disk_args,
    const std::vector<std::string> & serializations,
    const std::vector<bool> & adds_marker,
    size_t & chosen,
    ContextPtr context,
    bool attach,
    bool for_system_database,
    bool stored_definition)
{
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config;
    bool uses_server_credentials = false;
    bool restriction_exempt = false;
    {
        DynamicS3DiskCredentialInfo s3_disk_info;
        auto xml_document = getDiskConfigurationFromASTImpl(disk_args, context, attach, &s3_disk_info, for_system_database);
        config = resolveDiskConfiguration(xml_document, context, attach);

        /// Applied after `processIncludes`. If the pre-resolution check already decided the disk must load
        /// anonymously, enforce it now (an `include` cannot re-introduce server credentials). Otherwise
        /// re-validate the resolved config, so an `include` that injects an S3 backend with server-managed
        /// auth (past a literal non-S3 `type` in the AST) is still caught.
        if (s3_disk_info.load_anonymously)
            forceAnonymousS3DiskConfig(*config);
        else
            validateResolvedS3DiskCredentials(*config, context, attach, s3_disk_info);

        validateResolvedAzureDiskCredentials(*config, context, attach, for_system_database);

        uses_server_credentials = diskUsesServerCredentials(*config, s3_disk_info);
        restriction_exempt = s3_disk_info.restriction_exempt;
    }

    const bool restricted = !attach && !restriction_exempt && context->shouldRestrictUserQueryS3Credentials();

    /// The Azure marker was chosen from an earlier resolution of the definition (see `ServerCredentialMarkerInjector`).
    if (!attach && !stored_definition && !context->shouldRestrictUserQueryS3Credentials() && azureDiskUsesServerIdentity(*config)
        && !hasCredentialsMarker(disk_args, AZURE_SERVER_CREDENTIALS_MARKER))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The configuration of the disk `{}` changed while it was being created (`include` or `from_zk`). Retry the query.",
            serializations[0]);

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
            serializations[0]);

    auto get_disk_name = [&](UInt128 hash) -> std::string
    {
        if (config->has("name"))
            return config->getString("name");
        /// We need a unique name for a created custom disk, but it needs to be the same
        /// after table is reattached or server is restarted, so take a hash of the disk
        /// configuration serialized ast as a disk name suffix.
        return DiskSelector::TMP_INTERNAL_DISK_PREFIX + toString(hash);
    };

    chosen = 0;
    auto disk_settings_hash = sipHash128(serializations[0].data(), serializations[0].size());
    /// A disk declared by an older version is registered under the form of the definition that version stored. Reusing it
    /// is safe: this session passed the credential checks for its definition, the forms differ only in markers, a form
    /// that adds one is refused below without the opt-in, and the caller stores the form of the reused disk.
    if (!attach && !isRegisteredCustomDisk(context, get_disk_name(disk_settings_hash), disk_settings_hash))
    {
        for (size_t i = 1; i < serializations.size(); ++i)
        {
            const auto hash = sipHash128(serializations[i].data(), serializations[i].size());
            if (isRegisteredCustomDisk(context, get_disk_name(hash), hash))
            {
                chosen = i;
                disk_settings_hash = hash;
                break;
            }
        }
    }
    const auto disk_name = get_disk_name(disk_settings_hash);

    /// A marker is an allowance persisted for reload, which only a session with the opt-in may store.
    if (adds_marker[chosen] && restricted)
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "The disk `{}` was declared by a session with `s3_allow_server_credentials_in_user_queries`. Declare it again from such a session.",
            disk_name);

    if (!attach && (disk_name.empty() || disk_name == "." || disk_name == ".." || disk_name.contains('/')))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk name cannot be empty, `.`, `..` or contain `/` ({})", disk_name);

    auto disk = context->getOrCreateDisk(disk_name, [&](const DisksMap & disks_map) -> DiskPtr {
        /// A wrapper (`encrypted`, `cache`) names a disk another session may have created with the opt-in. It is looked up
        /// in the map the wrapper is created from, so that it cannot be registered in between.
        const auto wrapped = disks_map.find(config->getString(".disk", ""));
        const bool wraps_server_credentials = wrapped != disks_map.end() && wrapped->second->customDiskUsesServerCredentials();
        if (wraps_server_credentials && restricted)
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "A dynamic disk created from user SQL may not wrap the disk `{}`, which uses the server's own credentials."
#if !CLICKHOUSE_CLOUD
                " Enable the setting `s3_allow_server_credentials_in_user_queries` to allow it."
#endif
                , wrapped->first);

        auto result = DiskFactory::instance().create(
            disk_name, *config, /* config_path */"", context, disks_map, /* attach */attach, /* custom_disk */true);
        /// Mark that disk can be used without storage policy.
        result->markDiskAsCustom(disk_settings_hash, uses_server_credentials || wraps_server_credentials);
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

    /// The disk may have been created by a session with the opt-in, from a configuration that resolved differently then
    /// (`include` or `from_zk` changed since), so the check of this session's definition does not cover it.
    if (restricted && disk->customDiskUsesServerCredentials())
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "The disk `{}` uses the server's own credentials, which a dynamic disk created from user SQL may not use."
#if !CLICKHOUSE_CLOUD
            " Enable the setting `s3_allow_server_credentials_in_user_queries` to allow it."
#endif
            , disk_name);

    return disk_name;
}

template <typename Matcher>
static void visitBottomUp(ASTPtr & ast, typename Matcher::Data & data)
{
    checkStackSize();
    for (auto & child : ast->children)
        visitBottomUp<Matcher>(child, data);
    Matcher::visit(ast, data);
}


static ASTs & getDiskFunctionArguments(const ASTPtr & ast)
{
    return assert_cast<ASTExpressionList &>(*ast->as<ASTFunction &>().arguments).children;
}

static ASTPtr makeCredentialsMarker(const std::string & marker)
{
    return makeASTFunction("equals", make_intrusive<ASTIdentifier>(marker), make_intrusive<ASTLiteral>(static_cast<UInt64>(1)));
}


/// Whether a disk definition uses `include`, `from_env` or `from_zk`.
static bool usesIndirectValues(const ASTs & args)
{
    return std::ranges::any_of(args, [](const ASTPtr & arg)
    {
        const auto * eq = arg->as<ASTFunction>();
        if (!eq || eq->name != "equals" || !eq->arguments || eq->arguments->children.size() < 2)
            return false;
        if (const auto * key = eq->arguments->children[0]->as<ASTIdentifier>(); key && key->name() == "include")
            return true;
        String str;
        if (const auto * identifier = eq->arguments->children[1]->as<ASTIdentifier>())
            str = identifier->name();
        else if (const auto * literal = eq->arguments->children[1]->as<ASTLiteral>(); literal && literal->value.getType() == Field::Types::String)
            str = literal->value.safeGet<String>();
        return str.starts_with("from_env") || str.starts_with("from_zk");
    });
}

/// How a form of a disk definition stored by an older version differs from the current one.
struct CredentialsMarkersChange
{
    std::set<std::string> drop;
    bool add_azure = false;
};

static void applyCredentialsMarkersChange(ASTs & args, const CredentialsMarkersChange & change)
{
    args.erase(std::remove_if(args.begin(), args.end(), [&](const ASTPtr & arg) { return change.drop.contains(getCredentialsMarker(arg)); }), args.end());
    if (change.add_azure)
        args.push_back(makeCredentialsMarker(AZURE_SERVER_CREDENTIALS_MARKER));
}

struct DiskConfigurationFlattenerData
{
    ContextPtr context;
    bool attach;
    bool for_system_database;
    bool stored_definition;
};

/// Replace every disk function in `flat` (bottom-up) with the name of its disk. `original` is the AST `flat` was cloned
/// from, which is stored in metadata: it gets the markers of the form an existing disk was reused by.
static void flattenDiskConfiguration(ASTPtr & flat, const ASTPtr & original, const DiskConfigurationFlattenerData & data)
{
    checkStackSize();
    for (size_t i = 0; i < flat->children.size(); ++i)
        flattenDiskConfiguration(flat->children[i], original->children[i], data);

    if (!isDiskFunction(flat))
        return;

    const auto & args = getDiskFunctionArguments(flat);
    std::vector<CredentialsMarkersChange> changes{{}};
    if (!data.attach && !data.stored_definition)
    {
        /// Forms stored by older versions. 26.11 added the Azure marker to any definition with `include`, `from_env` or
        /// `from_zk` when the session had the opt-in. Tried for any session, so that a session without the opt-in is refused
        /// instead of getting a new disk.
        if (!hasCredentialsMarker(args, AZURE_SERVER_CREDENTIALS_MARKER) && usesIndirectValues(args))
            changes.push_back({.drop = {}, .add_azure = true});

        /// 26.7 to 26.10 had no Azure marker, and older versions no marker at all.
        for (std::set<std::string> drop : {std::set<std::string>{AZURE_SERVER_CREDENTIALS_MARKER},
                                           std::set<std::string>{AZURE_SERVER_CREDENTIALS_MARKER, S3_SERVER_CREDENTIALS_MARKER}})
        {
            std::erase_if(drop, [&](const std::string & marker) { return !hasCredentialsMarker(args, marker); });
            if (!drop.empty() && std::ranges::none_of(changes, [&](const auto & change) { return change.drop == drop; }))
                changes.push_back({.drop = std::move(drop), .add_azure = false});
        }
    }

    std::vector<std::string> serializations{flat->formatWithSecretsOneLine()};
    std::vector<bool> adds_marker{false};
    for (size_t i = 1; i < changes.size(); ++i)
    {
        auto candidate = flat->clone();
        applyCredentialsMarkersChange(getDiskFunctionArguments(candidate), changes[i]);
        serializations.push_back(candidate->formatWithSecretsOneLine());
        adds_marker.push_back(changes[i].add_azure);
    }

    size_t chosen = 0;
    auto disk_name = getOrCreateCustomDisk(
        args, serializations, adds_marker, chosen, data.context, data.attach, data.for_system_database, data.stored_definition);
    if (chosen)
        applyCredentialsMarkersChange(getDiskFunctionArguments(original), changes[chosen]);
    flat = make_intrusive<ASTLiteral>(disk_name);
}

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

    static void visit(ASTPtr & ast, Data & data)
    {
        if (!isDiskFunction(ast))
            return;

        auto * function = ast->as<ASTFunction>();
        auto * args_expr = function->arguments ? function->arguments->as<ASTExpressionList>() : nullptr;
        if (!args_expr)
            return;
        auto & args = args_expr->children;

        /// Strip any marker the user put in the definition. This runs only on a fresh create (not on metadata
        /// load), so a server-written marker in stored metadata is never stripped. As a result a persisted
        /// marker is only ever one this injector wrote below for a disk that actually needed server credentials
        /// while the session had the opt-in -- a user cannot pre-seed it to bypass the restriction on reload.
        applyCredentialsMarkersChange(args, {.drop = {S3_SERVER_CREDENTIALS_MARKER, AZURE_SERVER_CREDENTIALS_MARKER}, .add_azure = false});

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
        auto xml_document = getDiskConfigurationFromASTImpl(args, data.context, /* is_loading_from_existing_metadata */ false, &info, data.for_system_database);
        if (info.persist_server_credentials_allowance)
            args.push_back(makeCredentialsMarker(S3_SERVER_CREDENTIALS_MARKER));

        /// Decided on the resolved definition, so the marker is added exactly when a session without the opt-in would be
        /// refused the disk. Otherwise the same definition would get a different name depending on the session setting.
        if (!data.context->shouldRestrictUserQueryS3Credentials()
            && azureDiskUsesServerIdentity(*resolveDiskConfiguration(xml_document, data.context, /* attach */ false)))
            args.push_back(makeCredentialsMarker(AZURE_SERVER_CREDENTIALS_MARKER));
    }
};


std::string DiskFromAST::createCustomDisk(
    const ASTPtr & disk_function_ast, ContextPtr context, bool attach, bool for_system_database, bool stored_definition)
{
    if (!isDiskFunction(disk_function_ast))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a disk function");

    /// On a fresh create, persist the credential opt-in into the definition (a `_server_credentials_allowed`
    /// marker) so the disk keeps working across restart. This must happen BEFORE flattening, because the custom
    /// disk's name is a hash of its definition: the marker has to be part of that definition so the disk
    /// resolves to the same name (and storage path) when reloaded from metadata. The marker itself is an unknown
    /// key to the disk factory and is ignored when building the client.
    if (!attach && !stored_definition)
    {
        ASTPtr to_mark = disk_function_ast;
        ServerCredentialMarkerInjector::Data inject_data{context, for_system_database};
        visitBottomUp<ServerCredentialMarkerInjector>(to_mark, inject_data);
    }

    auto ast = disk_function_ast->clone();
    flattenDiskConfiguration(ast, disk_function_ast, {context, attach, for_system_database, stored_definition});

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
