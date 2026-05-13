#include <Interpreters/NamedScalars/NamedScalarsManager.h>

#include <Interpreters/NamedScalars/NamedScalarDefinitionParse.h>
#include <Interpreters/NamedScalars/NamedScalarDefinitionStoreLocal.h>
#include <Interpreters/NamedScalars/NamedScalarValueBackendLocal.h>
#include <Interpreters/NamedScalars/NamedScalarDefinitionStoreShared.h>
#include <Interpreters/NamedScalars/SharedNamedScalarsWatcher.h>
#include <Interpreters/NamedScalars/NamedScalarValueBackendShared.h>

#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <base/defines.h>

#include <filesystem>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NAMED_SCALAR_ALREADY_EXISTS;
    extern const int NAMED_SCALAR_NOT_FOUND;
    extern const int NAMED_SCALAR_NOT_REFRESHABLE;
    extern const int SHARED_NAMED_SCALARS_NOT_CONFIGURED;
}

void NamedScalarsManager::checkName(const String & name)
{
    static constexpr size_t max_name_len = 200;
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Named scalar name cannot be empty");
    if (name.size() > max_name_len)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Named scalar name is too long ({} bytes, max {} bytes)",
            name.size(), max_name_len);
    const auto escaped = escapeForFileName(name);
    if (escaped.size() > max_name_len)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Named scalar name escapes to too long a filename ({} bytes after escape, max {} bytes); avoid characters that require escaping",
            escaped.size(), max_name_len);
}

namespace
{
constexpr std::string_view value_suffix = ".bin";

void sweepOrphanLocalValueFiles(
    const String & dir_path,
    const std::vector<LoadedNamedScalarDefinition> & definitions,
    const ContextPtr & context,
    LoggerPtr log)
{
    std::unordered_set<String> live_value_files;
    for (const auto & definition : definitions)
    {
        auto uuid = getNamedScalarUUIDFromSerializedDefinition(definition.definition_blob, context, log);
        if (uuid)
            live_value_files.insert(escapeForFileName(*uuid) + String(value_suffix));
    }

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        const String & file_name = it.name();
        if (!file_name.ends_with(value_suffix) || live_value_files.contains(file_name))
            continue;
        try { std::filesystem::remove(dir_path + file_name); }
        catch (...) { tryLogCurrentException(log, fmt::format("while sweeping orphan value {}", file_name)); }
    }
}

}

NamedScalarsManager::NamedScalarsManager(
    const String & definitions_disk_path,
    const String & definitions_zookeeper_path,
    const String & local_cache_path,
    NamedScalarCacheKind default_cache_kind_,
    const ContextPtr & global_context)
    : default_cache_kind(default_cache_kind_)
    , local_value_backend(std::make_unique<NamedScalarValueBackendLocal>(
          local_cache_path,
          getLogger("NamedScalarsManager")))
{
    if (definitions_zookeeper_path.empty())
    {
        definition_store = std::make_shared<NamedScalarDefinitionStoreLocal>(
            definitions_disk_path,
            getLogger("NamedScalarsManager"));
    }
    else
    {
        auto keeper_store = std::make_shared<NamedScalarDefinitionStoreShared>(global_context, definitions_zookeeper_path);
        definition_store = keeper_store;
        watchable_definition_store = keeper_store;
        shared_value_backend = std::make_unique<NamedScalarValueBackendShared>(
            global_context,
            definitions_zookeeper_path,
            [this]
            {
                if (watcher)
                    watcher->pokeResyncAll();
            });
    }
}

NamedScalarsManager::~NamedScalarsManager() = default;

void NamedScalarsManager::initialize(const ContextPtr & global_context)
{
    local_value_backend->setGlobalContext(global_context);
    local_value_backend->initialize();
    definition_store->initialize();

    if (usesKeeperDefinitions())
    {
        chassert(watchable_definition_store);
        chassert(shared_value_backend);
        watcher = std::make_unique<SharedNamedScalarsWatcher>(
            global_context,
            watchable_definition_store,
            *this);
        watcher->start();
        return;
    }

    auto definitions = definition_store->loadAll();
    sweepOrphanLocalValueFiles(local_value_backend->directoryPath(), definitions, global_context, getLogger("NamedScalarsManager"));

    auto log = getLogger("NamedScalarsManager");
    for (auto & loaded : definitions)
    {
        auto cache_kind = cacheKindFromDefinitionBlob(loaded.definition_blob, global_context, log);
        if (cache_kind == NamedScalarCacheKind::Shared)
        {
            LOG_WARNING(
                log,
                "Ignoring disk definition for shared-cache named scalar '{}': shared cache requires Keeper definitions",
                loaded.name);
            continue;
        }

        auto parsed = parseAndValidateDefinition(
            loaded.name, loaded.definition_blob,
            std::chrono::system_clock::now(), global_context, log);
        if (!parsed)
            continue;

        auto scalar = std::make_shared<NamedScalar>(std::move(*parsed), valueBackendFor(cache_kind));
        scalar->loadValueFromBackend();
        installScalar(scalar, cache_kind);
        scalar->start(global_context);
    }
}

void NamedScalarsManager::shutdown()
{
    try
    {
        if (watcher)
            watcher->stop();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("NamedScalarsManager"), "while shutting down named scalars watcher");
    }

    try
    {
        shutdownScalars();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("NamedScalarsManager"), "while shutting down named scalars");
    }
}

INamedScalarValueBackend & NamedScalarsManager::valueBackendFor(NamedScalarCacheKind cache_kind) const
{
    if (cache_kind != NamedScalarCacheKind::Shared)
        return *local_value_backend;
    if (!shared_value_backend)
        throw Exception(
            ErrorCodes::SHARED_NAMED_SCALARS_NOT_CONFIGURED,
            "Shared named scalar cache requires Keeper-backed named scalar definitions "
            "(configure <named_scalar_definitions_zookeeper_path>)");
    return *shared_value_backend;
}

NamedScalarCacheKind NamedScalarsManager::cacheKindFromDefinitionBlob(const String & definition_blob, const ContextPtr & context, LoggerPtr log) const
{
    auto cache_kind = getNamedScalarCacheKindFromSerializedDefinition(definition_blob, context, log);
    if (!cache_kind)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot determine named scalar cache kind from persisted definition");
    return *cache_kind;
}

NamedScalarPtr NamedScalarsManager::tryGetScalar(const String & name) const
{
    std::shared_lock lock(scalars_mutex);
    auto it = scalars.find(name);
    if (it == scalars.end())
        return nullptr;
    return it->second.scalar;
}

std::optional<NamedScalarWithScope> NamedScalarsManager::tryGetScopedScalar(const String & name) const
{
    std::shared_lock lock(scalars_mutex);
    auto it = scalars.find(name);
    if (it == scalars.end())
        return std::nullopt;
    return it->second;
}

std::optional<NamedScalarCacheKind> NamedScalarsManager::getCacheKind(
    const String & name,
    const ContextPtr & context,
    LoggerPtr log) const
{
    if (auto scoped = tryGetScopedScalar(name))
        return scoped->cache_kind;

    String definition_blob;
    if (!definition_store->readDefinition(name, definition_blob))
        return std::nullopt;
    return getNamedScalarCacheKindFromSerializedDefinition(definition_blob, context, log);
}

std::vector<NamedScalarPtr> NamedScalarsManager::listAllScalars() const
{
    std::vector<NamedScalarPtr> out;
    std::shared_lock lock(scalars_mutex);
    out.reserve(scalars.size());
    for (const auto & [_, scoped] : scalars)
        out.push_back(scoped.scalar);
    return out;
}

std::vector<NamedScalarWithScope> NamedScalarsManager::listScalars() const
{
    std::vector<NamedScalarWithScope> out;
    std::shared_lock lock(scalars_mutex);
    out.reserve(scalars.size());
    for (const auto & [_, scoped] : scalars)
        out.push_back(scoped);
    return out;
}

void NamedScalarsManager::ensureCreatable(NamedScalarCacheKind cache_kind) const
{
    if (cache_kind == NamedScalarCacheKind::Shared && !usesKeeperDefinitions())
        throw Exception(
            ErrorCodes::SHARED_NAMED_SCALARS_NOT_CONFIGURED,
            "Shared named scalar cache requires Keeper-backed named scalar definitions "
            "(configure <named_scalar_definitions_zookeeper_path>)");
}

bool NamedScalarsManager::definitionExists(const String & name) const
{
    return definition_store->definitionExists(name);
}

NamedScalarPtr NamedScalarsManager::swapScalar(NamedScalarPtr scalar, NamedScalarCacheKind cache_kind)
{
    chassert(scalar);
    const String name = scalar->getName();
    NamedScalarPtr replaced;
    std::unique_lock lock(scalars_mutex);
    auto it = scalars.find(name);
    if (it == scalars.end())
    {
        scalars.emplace(name, NamedScalarWithScope{.cache_kind = cache_kind, .scalar = std::move(scalar)});
    }
    else
    {
        replaced = std::move(it->second.scalar);
        it->second = NamedScalarWithScope{.cache_kind = cache_kind, .scalar = std::move(scalar)};
    }
    return replaced;
}

void NamedScalarsManager::installScalar(NamedScalarPtr scalar, NamedScalarCacheKind cache_kind)
{
    auto replaced = swapScalar(std::move(scalar), cache_kind);
    if (replaced)
        replaced->shutdown();
}

bool NamedScalarsManager::dropScalar(const String & name)
{
    NamedScalarPtr removed;
    {
        std::unique_lock lock(scalars_mutex);
        auto it = scalars.find(name);
        if (it == scalars.end())
            return false;
        removed = std::move(it->second.scalar);
        scalars.erase(it);
    }
    removed->shutdown();
    return true;
}

void NamedScalarsManager::shutdownScalars()
{
    std::unordered_map<String, NamedScalarWithScope> drained;
    {
        std::unique_lock lock(scalars_mutex);
        drained = std::move(scalars);
        scalars.clear();
    }
    for (auto & [_, scoped] : drained)
        scoped.scalar->shutdown();
}

bool NamedScalarsManager::create(NamedScalarCreateRequest request, const ContextPtr & context)
{
    const auto cache_kind = request.cache_kind;
    const auto & name = request.name;
    ensureCreatable(cache_kind);

    const ContextPtr task_context = context->getGlobalContext();
    auto log = getLogger("NamedScalarsManager");

    /// Fast existence pre-check against the local map so IF NOT EXISTS
    /// short-circuits without parsing + evaluating the SELECT. Authoritative
    /// serialisation happens in publishDefinition; missing the local map
    /// (peer just CREATEd, watcher hasn't reconciled) only costs a wasted
    /// SELECT — publishDefinition will still serve IF NOT EXISTS correctly.
    if (!request.or_replace)
    {
        std::shared_lock lock(scalars_mutex);
        if (scalars.contains(name))
        {
            if (request.if_not_exists)
                return false;
            throw Exception(ErrorCodes::NAMED_SCALAR_ALREADY_EXISTS, "Named scalar '{}' already exists", name);
        }
    }

    /// Parse + build the in-memory ParsedDefinition. The interpreter has
    /// already materialised an explicit UUID into request.formatted_create_query.
    auto parsed = parseAndValidateDefinition(
        name, request.formatted_create_query,
        std::chrono::system_clock::now(), task_context, log);
    if (!parsed)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid serialized definition for named scalar '{}'", name);

    auto & backend = valueBackendFor(cache_kind);
    auto scalar = std::make_shared<NamedScalar>(*parsed, backend);

    /// Best-effort orphan-cleanup helper: any failure path between
    /// writing the value blob and publishing the definition leaves an
    /// orphan at parsed->uuid that we want to remove. removeValue may
    /// itself throw (transient backend error) - that's fine, log and
    /// move on; for the local backend the startup orphan-sweep is the
    /// long-term safety net.
    auto remove_orphan_value = [&](const String & reason) noexcept
    {
        try { backend.removeValue(parsed->uuid); }
        catch (...) { tryLogCurrentException(log, reason); }
    };

    /// Run the SELECT and write the value blob BEFORE we publish the
    /// definition. If evaluation throws, no durable definition exists
    /// (the caller sees the exception). On crash between this write and
    /// the definition publish, the value blob is orphaned at this UUID.
    try
    {
        scalar->evaluateAndStoreValue(task_context);
    }
    catch (...)
    {
        remove_orphan_value(fmt::format("removing orphaned value for failed CREATE '{}'", name));
        throw;
    }

    String old_uuid_to_remove;
    NamedScalarPtr replaced;

    try
    {
        std::lock_guard catalog_lock(create_drop_mutex);

        /// Cap check uses the local map, not the definition store: doing
        /// a `definitionExists` here would be a second Keeper round-trip
        /// for shared mode just to gate a soft guardrail. In the rare
        /// case where a peer just CREATEd a scalar but our watcher
        /// hasn't installed it locally yet, we may be off by one - that's
        /// acceptable for `max_named_scalars` (it's not a hard invariant).
        const UInt64 cap = context->getConfigRef().getUInt64("max_named_scalars", 500);
        bool name_exists_locally;
        size_t local_count;
        {
            std::shared_lock lock(scalars_mutex);
            name_exists_locally = scalars.contains(name);
            local_count = scalars.size();
        }
        if (!name_exists_locally && local_count >= cap)
        {
            remove_orphan_value("removing orphaned value after exceeding cap");
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Too many named scalars ({} already, max {}); raise <max_named_scalars> in the server config to grow",
                local_count, cap);
        }

        /// Capture the predecessor's UUID so we can clean up its value
        /// blob after a successful OR REPLACE. Same cache_kind is assumed
        /// (the interpreter rejects KIND changes via OR REPLACE).
        if (request.or_replace)
        {
            std::shared_lock lock(scalars_mutex);
            auto it = scalars.find(name);
            if (it != scalars.end() && it->second.scalar)
                old_uuid_to_remove = it->second.scalar->getUUID();
        }

        /// publishDefinition is the authoritative serialiser: it throws
        /// NAMED_SCALAR_ALREADY_EXISTS for non-IF-NOT-EXISTS races, and
        /// returns false for the legitimate IF-NOT-EXISTS-with-existing
        /// case. No manual re-check needed.
        const bool definition_published = definition_store->publishDefinition(
            name,
            request.formatted_create_query,
            request.if_not_exists,
            request.or_replace,
            context->getSettingsRef());
        if (!definition_published)
        {
            remove_orphan_value("removing orphaned value after publishDefinition declined");
            return false;
        }

        /// Swap in the new scalar (atomic map update) and start its task,
        /// but do NOT shut down the predecessor here: holding
        /// create_drop_mutex through `replaced->shutdown()` would
        /// serialise all DDL on the predecessor's in-flight refresh
        /// drain. The predecessor's `live` flag is already false from
        /// this point on; we just defer the cancel-and-join.
        replaced = swapScalar(scalar, cache_kind);
        scalar->start(task_context);
    }
    catch (...)
    {
        remove_orphan_value(
            fmt::format("removing orphaned value after publishDefinition or installScalar threw for '{}'", name));
        throw;
    }

    /// Slow path: predecessor shutdown JOINS an in-flight refresh body.
    /// Outside the catalog lock so concurrent DDL on other scalars is
    /// not blocked.
    if (replaced)
        replaced->shutdown();

    /// Best-effort: remove the predecessor's value blob now that the new
    /// scalar is live. Not catastrophic if it fails - leaves an orphan.
    if (!old_uuid_to_remove.empty() && old_uuid_to_remove != parsed->uuid)
    {
        try { backend.removeValue(old_uuid_to_remove); }
        catch (...) { tryLogCurrentException(log, fmt::format("removing prior value for OR REPLACE of '{}'", name)); }
    }

    if (watcher)
        watcher->pokeResyncAll();
    return true;
}

bool NamedScalarsManager::drop(const String & name, bool throw_if_not_exists)
{
    std::lock_guard catalog_lock(create_drop_mutex);

    NamedScalarCacheKind cache_kind = NamedScalarCacheKind::Local;
    String value_key;
    if (auto found = tryGetScopedScalar(name))
    {
        cache_kind = found->cache_kind;
        value_key = found->scalar->getUUID();
    }
    else
    {
        String definition_blob;
        if (definition_store->readDefinition(name, definition_blob))
        {
            cache_kind = cacheKindFromDefinitionBlob(definition_blob, Context::getGlobalContextInstance(), getLogger("NamedScalarsManager"));
            if (auto uuid = getNamedScalarUUIDFromSerializedDefinition(
                    definition_blob,
                    Context::getGlobalContextInstance(),
                    getLogger("NamedScalarsManager")))
                value_key = *uuid;
        }
    }

    const bool removed = definition_store->removeDefinition(name, throw_if_not_exists);
    if (!removed)
        return false;

    dropScalar(name);
    if (!value_key.empty())
    {
        try
        {
            valueBackendFor(cache_kind).removeValue(value_key);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("NamedScalarsManager"),
                fmt::format("removing value blob for dropped scalar '{}'", name));
        }
    }
    return true;
}

void NamedScalarsManager::refreshNow(const String & name)
{
    auto scalar = tryGetScalar(name);
    if (!scalar)
        throw Exception(ErrorCodes::NAMED_SCALAR_NOT_FOUND, "named scalar '{}' doesn't exist", name);
    scalar->requestRefreshNow();
}

void NamedScalarsManager::setRefreshPaused(const String & name, bool paused)
{
    auto scalar = tryGetScalar(name);
    if (!scalar)
        throw Exception(ErrorCodes::NAMED_SCALAR_NOT_FOUND, "named scalar '{}' doesn't exist", name);
    /// Symmetric with refreshNow's NAMED_SCALAR_NOT_REFRESHABLE: when an
    /// operator names a specific scalar, surface the mismatch instead of
    /// silently no-opping. The iterate-all path keeps silent-skip.
    if (!scalar->isRefreshable())
        throw Exception(
            ErrorCodes::NAMED_SCALAR_NOT_REFRESHABLE,
            "named scalar '{}' is not refreshable",
            name);
    scalar->setRefreshPaused(paused);
}

void NamedScalarsManager::setAllRefreshesPaused(bool paused)
{
    for (const auto & scalar : listAllScalars())
        scalar->setRefreshPaused(paused);
}

void NamedScalarsManager::installStoredDefinition(
    const ContextPtr & context,
    const String & name,
    const String & definition_blob,
    LoggerPtr log)
{
    auto cache_kind = cacheKindFromDefinitionBlob(definition_blob, context, log);

    auto parsed = parseAndValidateDefinition(
        name, definition_blob, std::chrono::system_clock::now(), context, log);
    if (!parsed)
        return;

    /// Same-UUID fast path: nothing to do. The watcher has re-armed its
    /// own data-watch as part of reading the blob; the scalar's value
    /// watch is independently armed by the scalar itself.
    ///
    /// This check is intentionally NOT serialised against `create_drop_mutex`:
    /// a local CREATE racing with this watcher reconcile may end up
    /// installing the same UUID twice (via both paths), in which case
    /// the second call to `installScalar` just swaps in an equivalent
    /// instance and shuts the first one down. End state is consistent;
    /// holding `create_drop_mutex` here would block local CREATE on
    /// every reconcile (which does I/O), and the wasted work is rare.
    {
        std::shared_lock lock(scalars_mutex);
        auto it = scalars.find(name);
        if (it != scalars.end()
            && it->second.scalar
            && it->second.scalar->getUUID() == parsed->uuid)
            return;
    }

    /// Different-UUID (peer CREATE / OR REPLACE / DROP-and-recreate):
    /// drop+create. Swap the new scalar in atomically, then shutdown the
    /// predecessor outside the swap so the watcher loop is not blocked
    /// by an in-flight refresh drain on the old scalar.
    auto scalar = std::make_shared<NamedScalar>(std::move(*parsed), valueBackendFor(cache_kind));
    scalar->loadValueFromBackend();
    auto replaced = swapScalar(scalar, cache_kind);
    scalar->start(context);
    if (replaced)
        replaced->shutdown();
}

}
