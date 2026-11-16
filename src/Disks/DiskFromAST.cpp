#include <Disks/DiskFromAST.h>
#include <Disks/getDiskConfigurationFromAST.h>
#include <Disks/DiskSelector.h>
#include <Disks/IStoragePolicy.h>
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
    if (key && key->name() == S3_SERVER_CREDENTIALS_MARKER)
        return key->name();
    return {};
}

static bool hasCredentialsMarker(const ASTs & args, const std::string & marker)
{
    return std::ranges::any_of(args, [&](const ASTPtr & arg) { return getCredentialsMarker(arg) == marker; });
}

/// `serializations` holds the definition, then other forms of it (without the credentials marker, other disks below it).
/// Returns the indexes and names of the registered disks the forms match, the one used first. The others are returned
/// only if this session may use them. If none matches, the first form is created.
static std::vector<std::pair<size_t, std::string>> getOrCreateCustomDisk(
    const ASTs & disk_args,
    const std::vector<std::string> & serializations,
    ContextPtr context,
    bool attach,
    bool for_system_database)
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

        uses_server_credentials = diskUsesServerCredentials(*config, s3_disk_info);
        restriction_exempt = s3_disk_info.restriction_exempt;
    }

    const bool restricted = !attach && !restriction_exempt && context->shouldRestrictUserQueryS3Credentials();

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

    std::vector<UInt128> hashes;
    for (const auto & serialization : serializations)
        hashes.push_back(sipHash128(serialization.data(), serialization.size()));

    /// A disk declared by an older version is registered under the form of the definition that version stored. Reusing it
    /// is safe: this session passed the credential checks for its definition, the forms differ only in markers and in the
    /// equivalent disks below, and the caller stores the form of the reused disk.
    std::vector<std::pair<size_t, std::string>> matches;
    if (!attach)
    {
        const auto disks = context->getDisksMap();
        for (size_t i = 0; i < serializations.size(); ++i)
        {
            auto name = get_disk_name(hashes[i]);
            const auto it = disks.find(name);
            if (it == disks.end() || !it->second->isCustomDisk() || it->second->getCustomDiskSettings() != hashes[i])
                continue;
            if (!matches.empty() && restricted && it->second->customDiskUsesServerCredentials())
                continue;
            matches.emplace_back(i, std::move(name));
        }
    }
    if (matches.empty())
        matches.emplace_back(0, get_disk_name(hashes[0]));

    const auto disk_settings_hash = hashes[matches.front().first];
    const auto & disk_name = matches.front().second;

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

    return matches;
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


/// How a form of a disk definition stored by an older version differs from the current one.
struct CredentialsMarkersChange
{
    std::set<std::string> drop;
};

static void applyCredentialsMarkersChange(ASTs & args, const CredentialsMarkersChange & change)
{
    args.erase(std::remove_if(args.begin(), args.end(), [&](const ASTPtr & arg) { return change.drop.contains(getCredentialsMarker(arg)); }), args.end());
}

struct DiskConfigurationFlattenerData
{
    ContextPtr context;
    bool attach;
    bool for_system_database;
    bool stored_definition;
};

/// A registered disk a disk function may resolve to, and the changes of credentials markers in the stored definition
/// (of the function and of the disk functions below it) that give it.
struct DiskAlternative
{
    std::string name;
    std::vector<std::pair<ASTPtr, CredentialsMarkersChange>> edits;
};

/// A disk function already replaced by the name of its disk, and the disks it may resolve to, the used one first.
struct FlattenedDisk
{
    ASTPtr * literal;
    std::vector<DiskAlternative> alternatives;
};

/// Replace every disk function in `flat` (bottom-up) with the name of its disk. `original` is the AST `flat` was cloned
/// from, which is stored in metadata; the caller applies the edits of the alternative it uses to it. Returns the disk
/// functions directly in `flat`, or `flat` itself if it is one.
static std::vector<FlattenedDisk> flattenDiskConfiguration(ASTPtr & flat, const ASTPtr & original, const DiskConfigurationFlattenerData & data)
{
    checkStackSize();
    std::vector<FlattenedDisk> below;
    for (size_t i = 0; i < flat->children.size(); ++i)
        std::ranges::move(flattenDiskConfiguration(flat->children[i], original->children[i], data), std::back_inserter(below));

    if (!isDiskFunction(flat))
        return below;

    const auto & args = getDiskFunctionArguments(flat);
    std::vector<CredentialsMarkersChange> changes{{}};
    if (!data.attach && !data.stored_definition)
    {
        /// The form stored by older versions: versions before 26.7 had no marker.
        if (hasCredentialsMarker(args, S3_SERVER_CREDENTIALS_MARKER))
            changes.push_back({.drop = {S3_SERVER_CREDENTIALS_MARKER}});
    }

    /// Every combination of the disks below (an older wrapper names the disk of the older form), the used ones first.
    std::vector<std::vector<size_t>> combinations{{}};
    for (const auto & disk : below)
    {
        std::vector<std::vector<size_t>> extended;
        for (size_t alternative = 0; alternative < disk.alternatives.size(); ++alternative)
        {
            for (const auto & combination : combinations)
            {
                extended.push_back(combination);
                extended.back().push_back(alternative);
            }
        }
        combinations = std::move(extended);
    }

    auto set_disks_below = [&](const std::vector<size_t> & combination)
    {
        for (size_t i = 0; i < below.size(); ++i)
            *below[i].literal = make_intrusive<ASTLiteral>(below[i].alternatives[combination[i]].name);
    };

    std::vector<std::string> serializations;
    std::vector<DiskAlternative> forms;
    for (const auto & combination : combinations)
    {
        set_disks_below(combination);
        for (const auto & change : changes)
        {
            auto candidate = flat->clone();
            applyCredentialsMarkersChange(getDiskFunctionArguments(candidate), change);
            serializations.push_back(candidate->formatWithSecretsOneLine());

            DiskAlternative form{.name = {}, .edits = {}};
            if (!change.drop.empty())
                form.edits.emplace_back(original, change);
            for (size_t i = 0; i < below.size(); ++i)
                std::ranges::copy(below[i].alternatives[combination[i]].edits, std::back_inserter(form.edits));
            forms.push_back(std::move(form));
        }
    }
    /// The disk is created, if it is, from the disks used below.
    set_disks_below(combinations.front());

    const auto matches = getOrCreateCustomDisk(args, serializations, data.context, data.attach, data.for_system_database);

    std::vector<DiskAlternative> alternatives;
    for (const auto & [index, name] : matches)
    {
        alternatives.push_back(std::move(forms[index]));
        alternatives.back().name = name;
    }
    flat = make_intrusive<ASTLiteral>(alternatives.front().name);
    return {FlattenedDisk{.literal = &flat, .alternatives = std::move(alternatives)}};
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
        applyCredentialsMarkersChange(args, {.drop = {S3_SERVER_CREDENTIALS_MARKER}});

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
            args.push_back(makeCredentialsMarker(S3_SERVER_CREDENTIALS_MARKER));
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
    const auto disks = flattenDiskConfiguration(ast, disk_function_ast, {context, attach, for_system_database, stored_definition});
    for (const auto & [disk_function, change] : disks.front().alternatives.front().edits)
        applyCredentialsMarkersChange(getDiskFunctionArguments(disk_function), change);

    return assert_cast<const ASTLiteral &>(*ast).value.safeGet<String>();
}

void DiskFromAST::ensureDiskIsAllowed(const DiskPtr & disk, ContextPtr context)
{
    /// A custom disk can be named by another session than the one that created it.
    if (disk->customDiskUsesServerCredentials() && context->shouldRestrictUserQueryS3Credentials())
        throw Exception(
            ErrorCodes::ACCESS_DENIED,
            "The disk `{}` uses the server's own credentials, which may not be used from user SQL."
#if !CLICKHOUSE_CLOUD
            " Enable the setting `s3_allow_server_credentials_in_user_queries` to allow it."
#endif
            , disk->getName());
}

void DiskFromAST::ensureStoragePolicyIsAllowed(const std::string & storage_policy_name, ContextPtr context)
{
    /// `__<disk>` is the policy a table on a disk gets, so it names a custom disk as well.
    if (const auto storage_policy = context->tryGetStoragePolicy(storage_policy_name))
    {
        for (const auto & disk : storage_policy->getDisks())
            ensureDiskIsAllowed(disk, context);
    }
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
