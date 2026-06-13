#include <Disks/DiskFromAST.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Common/SipHash.h>
#include <Common/Config/ConfigProcessor.h>
#include <Disks/getDiskConfigurationFromAST.h>
#include <Disks/DiskSelector.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/isDiskFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Common/FailPoint.h>
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

namespace FailPoints
{
    extern const char disk_from_ast_pause_after_tentative_registration[];
    extern const char disk_from_ast_unscoped_observer_pause_after_sentinel[];
}

/// Current thread's ambient CREATE / ATTACH scope. Installed by `CustomDiskRegistrationScope`
/// when constructed with `install_as_ambient_create_scope = true` (from `InterpreterCreateQuery`),
/// so that inline `disk(...)` conversions happening deep inside `StorageFactory` /
/// `DatabaseFactory` (which have no scope parameter) are tracked under a caller-owned scope that
/// commits only after the outer metadata transition is durable. See issue #63019.
static thread_local DiskFromAST::CustomDiskRegistrationScope * t_ambient_create_scope = nullptr;

DiskFromAST::CustomDiskRegistrationScope * DiskFromAST::currentAmbientCreateScope()
{
    return t_ambient_create_scope;
}

static std::string getOrCreateCustomDisk(
    const ASTs & disk_args,
    const std::string & serialization,
    ContextPtr context,
    bool attach,
    DiskFromAST::CustomDiskRegistrationScope * scope)
{
    /// CREATE / ATTACH callers reach here without an explicit scope (the conversion happens deep
    /// inside `StorageFactory` / `DatabaseFactory`). `InterpreterCreateQuery` installs an ambient
    /// scope on this thread so those registrations are still owned by a caller-controlled scope
    /// that commits only after the outer metadata transition is durable. A non-null explicit
    /// `scope` (the ALTER path) always wins. See issue #63019, PR #103818.
    DiskFromAST::CustomDiskRegistrationScope * const effective_scope
        = scope ? scope : DiskFromAST::currentAmbientCreateScope();
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

    /// `Context::getOrCreateDisk` records `scope` (used as an opaque owner pointer) in the
    /// global pending-rollback table. Newly-registered disks are tracked under that scope's
    /// ownership; observers of an already-registered disk join the rollback chain so they
    /// cannot be silently rolled out from under each other. See
    /// `DiskFromAST::CustomDiskRegistrationScope` and issue #63019.
    auto disk = context->getOrCreateDisk(disk_name, [&](const DisksMap & disks_map) -> DiskPtr {
        /// `disk(type = cache, name = X, ...)` registers a `FileCache` keyed by `X` in the
        /// global `FileCacheFactory` BEFORE `RegisterDiskCache` reaches the
        /// `DiskObjectStorage` cast that may throw (`RegisterDiskCache.cpp:144`). If the
        /// creator throws after the cache is inserted, the disk is never added to
        /// `DiskSelector`, so the registration scope never gets a chance to track it.
        /// Snapshot `FileCacheFactory` state here and roll back any cache entry that
        /// appeared on the throw path. A pre-existing cache by the same name (same path)
        /// is not removed, because the snapshot proves the entry existed before us.
        const bool cache_pre_existed = FileCacheFactory::instance().tryGetByName(disk_name) != nullptr;
        try
        {
            auto result = DiskFactory::instance().create(
                disk_name, *config, /* config_path */"", context, disks_map, /* attach */attach, /* custom_disk */true);
            /// Mark that disk can be used without storage policy.
            result->markDiskAsCustom(disk_settings_hash);
            return result;
        }
        catch (...)
        {
            if (!cache_pre_existed && FileCacheFactory::instance().tryGetByName(disk_name) != nullptr)
                FileCacheFactory::instance().removeByName(disk_name);
            throw;
        }
    }, /* pending_rollback_owner */ effective_scope);

    /// Always remember the name on the scope. The atomic ownership check at rollback / commit
    /// time decides whether this scope's destructor removes the disk (still uniquely owned)
    /// or merely releases its claim (someone else has observed the registration in the
    /// meantime and may have committed against it). Tracking even when the disk pre-existed
    /// is a no-op at rollback because the pending-rollback table will not be ours.
    /// For CREATE / ATTACH (`scope == nullptr`) `effective_scope` is the ambient scope installed
    /// by `InterpreterCreateQuery`; tracking under it defers the rollback decision to the outer
    /// metadata commit, mirroring the ALTER path.
    if (effective_scope)
        effective_scope->track(disk_name);

    /// Test-only barrier: pause here so that 04152 can deterministically inject a
    /// concurrent CREATE TABLE while the tentative registration is in flight.
    FailPointInjection::pauseFailPoint(FailPoints::disk_from_ast_pause_after_tentative_registration);

    /// Test-only barrier: fires only on the CREATE / ATTACH path (no EXPLICIT `scope` parameter),
    /// AFTER `Context::getOrCreateDisk` has recorded this caller's reference but BEFORE validation
    /// runs. Gated on the explicit `scope` parameter (not `effective_scope`) so it pauses a CREATE /
    /// ATTACH observer but never the ALTER apply path. Used by 04154 / 04155 to drive the
    /// reverse-order and co-observer races deterministically: the observer pauses here while the
    /// scoped ALTER owner runs and its destructor fires, then resumes and exercises the
    /// last-owner-leaves rollback / commit path.
    if (!scope)
        FailPointInjection::pauseFailPoint(FailPoints::disk_from_ast_unscoped_observer_pause_after_sentinel);

    /// Validate the (possibly observed) disk against this caller's settings BEFORE flipping
    /// any tentative registration to committed. For unscoped callers with NO ambient scope
    /// (`effective_scope == nullptr`, e.g. a server-startup ATTACH that does not run through
    /// `InterpreterCreateQuery`), `Context::getOrCreateDisk` took an anonymous `unscoped_observers`
    /// reference to keep the disk pinned across this validation; we must drop that reference
    /// exactly once via either commit or release. When `effective_scope` is non-null (ALTER, or a
    /// CREATE / ATTACH with the ambient scope installed) the scope owns the rollback / commit
    /// lifecycle and no anonymous reference was taken. Wrapping the validation in try/catch handles
    /// every throw path (`isCustomDisk`, settings-hash mismatch, custom-local-disks-base-dir, or
    /// future additions).
    try
    {
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
    }
    catch (...)
    {
        /// Only the no-ambient-scope fallback took an anonymous `unscoped_observers` reference.
        /// Scoped callers (ALTER and ambient CREATE / ATTACH) release through the scope destructor.
        if (!effective_scope)
            context->releaseUnscopedDiskObservation(disk_name);
        throw;
    }

    /// All validations passed. For the no-ambient-scope fallback, drop the anonymous reference and
    /// flip the tentative entry (if any) to committed so a still-in-flight scope's destructor cannot
    /// roll the disk back after this caller's metadata commits.
    ///
    /// IMPORTANT (issue #63019 gap fix): when `effective_scope` is non-null we DO NOT commit here.
    /// The disk's permanence is decided by the scope's `commit()`, which the caller invokes only
    /// AFTER its outer metadata transition is durable:
    ///   - ALTER: `MergeTreeData::changeSettings`'s caller-owned scope commits after `alterTable`.
    ///   - CREATE / ATTACH: the ambient scope installed by `InterpreterCreateQuery` commits after
    ///     `database->createTable` / `writeMetadataFile`. Committing here instead would re-introduce
    ///     the leak where a later `doCreateTable` failure (validateStorage, replicated fault
    ///     injection, metadata write) leaves the disk reserved in `DiskSelector` / `FileCacheFactory`
    ///     until restart.
    if (!effective_scope)
        context->commitUnscopedDiskObservation(disk_name);

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

DiskFromAST::CustomDiskRegistrationScope::CustomDiskRegistrationScope(ContextPtr context_, bool install_as_ambient_create_scope)
    : context(std::move(context_))
{
    if (install_as_ambient_create_scope)
    {
        /// Install this scope as the current thread's ambient CREATE / ATTACH scope, saving any
        /// previous one (e.g. a nested CREATE OR REPLACE that recurses into another CREATE) so it
        /// can be restored on destruction. The thread-local is purely a stack of scopes; the actual
        /// registration bookkeeping lives in `Context::tentative_disk_registrations`.
        installed_as_ambient = true;
        previous_ambient_scope = t_ambient_create_scope;
        t_ambient_create_scope = this;
    }
}

DiskFromAST::CustomDiskRegistrationScope::~CustomDiskRegistrationScope() noexcept
{
    /// Restore the previous ambient scope first, so the rollback work below (which may itself
    /// touch disk registration) never runs with a dangling `this` installed as the ambient scope.
    if (installed_as_ambient)
        t_ambient_create_scope = previous_ambient_scope;

    if (committed || registered_disk_names.empty())
        return;
    /// For each tracked name, ask `Context::removePendingCustomDiskIfOwned` to atomically
    /// check the global pending-rollback table and remove the disk only if our scope is still
    /// the recorded owner. If another DDL has observed this registration in between (its
    /// `getOrCreateDisk` call cleared our marker), the disk stays in the selector; the leaked
    /// name is safe because the existing settings-hash check rejects redefinition with
    /// different settings.
    /// This destructor must be `noexcept` because it runs during stack unwinding; failures
    /// are best-effort logged.
    for (const auto & disk_name : registered_disk_names)
    {
        try
        {
            context->removePendingCustomDiskIfOwned(disk_name, this);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("CustomDiskRegistrationScope"),
                fmt::format("Failed to release pending custom disk registration for {}", disk_name));
        }
    }
}

void DiskFromAST::CustomDiskRegistrationScope::track(const std::string & disk_name)
{
    registered_disk_names.push_back(disk_name);
}

void DiskFromAST::CustomDiskRegistrationScope::commit() noexcept
{
    committed = true;
    /// Drop any pending-rollback markers we still own. The disks stay in the global
    /// selectors; they have just been committed by the caller's metadata transition and
    /// are no longer tentative. Markers left behind would prevent a future ALTER validation
    /// scope (with a fresh `this` pointer) from being correctly tracked.
    for (const auto & disk_name : registered_disk_names)
    {
        try
        {
            context->clearPendingCustomDiskRegistration(disk_name, this);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("CustomDiskRegistrationScope"),
                fmt::format("Failed to clear pending custom disk registration for {}", disk_name));
        }
    }
}

}
