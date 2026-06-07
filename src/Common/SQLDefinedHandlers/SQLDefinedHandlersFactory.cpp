#include <Common/SQLDefinedHandlers/SQLDefinedHandlersFactory.h>
#include <Common/SQLDefinedHandlers/SQLDefinedHandlerFromAST.h>

#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ASTDropHandlerQuery.h>
#include <Common/StringUtils.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <base/sleep.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int HANDLER_ALREADY_EXISTS;
    extern const int HANDLER_DOESNT_EXIST;
    extern const int AMBIGUOUS_HANDLER;
    extern const int KEEPER_EXCEPTION;
}

/// How many times a replicated create/alter re-reads the set and retries when another replica changed it
/// concurrently. Contention is resolved in one or two rounds in practice; the bound only guards against a
/// pathological live-lock and is large enough never to reject a legitimate write.
static constexpr size_t max_replicated_write_attempts = 100;

SQLDefinedHandlersFactory & SQLDefinedHandlersFactory::instance()
{
    static SQLDefinedHandlersFactory instance;
    return instance;
}

SQLDefinedHandlersFactory::~SQLDefinedHandlersFactory()
{
    shutdown();
}

void SQLDefinedHandlersFactory::shutdown()
{
    shutdown_called = true;
    if (update_task)
        update_task->deactivate();
    metadata_storage.reset();
}

bool SQLDefinedHandlersFactory::loadIfNot(std::lock_guard<std::mutex> & lock)
{
    if (loaded)
        return false;

    auto context = Context::getGlobalContextInstance();
    metadata_storage = SQLDefinedHandlersMetadataStorage::create(context);

    loaded_handlers = metadata_storage->getAll();
    rebuildSnapshot(lock);

    if (metadata_storage->isReplicated())
    {
        update_task = context->getSchedulePool().createTask(StorageID::createEmpty(), "SQLDefinedHandlersUpdate", [this] { updateFunc(); });
        update_task->activate();
        update_task->schedule();
    }

    loaded = true;
    return true;
}

void SQLDefinedHandlersFactory::loadIfNot()
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
}

void SQLDefinedHandlersFactory::rebuildSnapshot(std::lock_guard<std::mutex> &)
{
    snapshot = std::make_shared<const SQLDefinedHandlers>(loaded_handlers);
}

SQLDefinedHandlersPtr SQLDefinedHandlersFactory::getAll()
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    return snapshot;
}

bool SQLDefinedHandlersFactory::exists(const std::string & handler_name)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    return loaded_handlers.contains(handler_name);
}

bool SQLDefinedHandlersFactory::isReplicated()
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
    return metadata_storage->isReplicated();
}

namespace
{

bool protocolsOverlap(const std::optional<String> & a, const std::optional<String> & b)
{
    /// A handler without a protocol matches all protocols, so it overlaps with anything.
    if (!a || !b)
        return true;
    return *a == *b;
}

bool methodsOverlap(const std::vector<String> & a, const std::vector<String> & b)
{
    for (const auto & m : a)
        if (std::find(b.begin(), b.end(), m) != b.end())
            return true;
    return false;
}

/// Whether two exact/prefix URL patterns can match a common path.
bool urlsOverlap(const SQLDefinedHandler & a, const SQLDefinedHandler & b)
{
    using T = SQLDefinedHandler::URLMatchType;

    /// Ambiguity cannot be statically checked when a regexp is involved.
    if (a.url_match_type == T::Regexp || b.url_match_type == T::Regexp)
        return false;

    const bool a_prefix = a.url_match_type == T::Prefix;
    const bool b_prefix = b.url_match_type == T::Prefix;

    if (!a_prefix && !b_prefix)
        return a.url == b.url;                               /// exact vs exact
    if (a_prefix && b_prefix)
        return startsWith(a.url, b.url) || startsWith(b.url, a.url);  /// prefix vs prefix
    if (a_prefix)
        return startsWith(b.url, a.url);                     /// prefix a vs exact b
    return startsWith(a.url, b.url);                         /// exact a vs prefix b
}

}

void SQLDefinedHandlersFactory::checkAmbiguity(const SQLDefinedHandler & candidate, const SQLDefinedHandlers & handlers)
{
    for (const auto & [name, existing] : handlers)
    {
        if (name == candidate.name)
            continue;

        if (protocolsOverlap(candidate.protocol, existing->protocol)
            && methodsOverlap(candidate.methods, existing->methods)
            && urlsOverlap(candidate, *existing))
        {
            throw Exception(ErrorCodes::AMBIGUOUS_HANDLER,
                "Handler `{}` is ambiguous with the existing handler `{}`: "
                "they can match the same HTTP request (URL, method and protocol overlap)",
                candidate.name, name);
        }
    }
}

void SQLDefinedHandlersFactory::createFromSQL(const ASTCreateHandlerQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    /// Build first (validates the query and computes the canonical statement), then check ambiguity, then persist.
    auto handler = makeSQLDefinedHandler(query);

    if (metadata_storage->isReplicated())
    {
        createReplicated(query, handler, lock);
        return;
    }

    if (loaded_handlers.contains(query.handler_name))
    {
        if (query.if_not_exists)
            return;
        throw Exception(ErrorCodes::HANDLER_ALREADY_EXISTS, "A handler `{}` already exists", query.handler_name);
    }

    checkAmbiguity(*handler, loaded_handlers);

    metadata_storage->store(query.handler_name, handler->create_statement, /* replace */ false);
    loaded_handlers.emplace(query.handler_name, handler);
    rebuildSnapshot(lock);
}

void SQLDefinedHandlersFactory::createReplicated(const ASTCreateHandlerQuery & query, const SQLDefinedHandlerPtr & handler, std::lock_guard<std::mutex> & lock)
{
    /// Re-read the whole set from Keeper at a known root version, check ambiguity against it, then persist
    /// the new handler conditionally on that version. If another replica changed the set in between, the
    /// write is rejected and we retry: this serializes the ambiguity check with the write across replicas,
    /// so two replicas cannot both commit handlers that overlap under different names.
    for (size_t attempt = 0; attempt < max_replicated_write_attempts; ++attempt)
    {
        Int32 version = 0;
        auto current = metadata_storage->getAll(version);

        if (current.contains(query.handler_name))
        {
            loaded_handlers = std::move(current);
            rebuildSnapshot(lock);
            if (query.if_not_exists)
                return;
            throw Exception(ErrorCodes::HANDLER_ALREADY_EXISTS, "A handler `{}` already exists", query.handler_name);
        }

        checkAmbiguity(*handler, current);

        if (metadata_storage->store(query.handler_name, handler->create_statement, /* replace */ false, version))
        {
            current.emplace(query.handler_name, handler);
            loaded_handlers = std::move(current);
            rebuildSnapshot(lock);
            return;
        }
        /// The set changed concurrently (root version mismatch, or the same name was just created on
        /// another replica): re-read and re-validate.
    }

    throw Exception(ErrorCodes::KEEPER_EXCEPTION,
        "Could not create handler `{}`: the set of handlers kept changing concurrently", query.handler_name);
}

void SQLDefinedHandlersFactory::removeFromSQL(const ASTDropHandlerQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    if (!loaded_handlers.contains(query.handler_name))
    {
        if (query.if_exists)
            return;
        throw Exception(ErrorCodes::HANDLER_DOESNT_EXIST, "Cannot drop handler `{}`, because it doesn't exist", query.handler_name);
    }

    /// Use removeIfExists so that a concurrent removal on another replica (Keeper storage) does not
    /// turn this into a hard error: the local snapshot above already enforced the IF EXISTS contract.
    metadata_storage->removeIfExists(query.handler_name);
    loaded_handlers.erase(query.handler_name);
    rebuildSnapshot(lock);
}

void SQLDefinedHandlersFactory::updateFromSQL(const ASTCreateHandlerQuery & alter_query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    if (metadata_storage->isReplicated())
    {
        updateReplicated(alter_query, lock);
        return;
    }

    if (!loaded_handlers.contains(alter_query.handler_name))
        throw Exception(ErrorCodes::HANDLER_DOESNT_EXIST, "Cannot alter handler `{}`, because it doesn't exist", alter_query.handler_name);

    /// Build the merged handler (no write yet), check ambiguity against the others, then persist.
    auto updated = metadata_storage->buildUpdatedHandler(alter_query);

    auto old = loaded_handlers.at(alter_query.handler_name);
    loaded_handlers.erase(alter_query.handler_name);
    /// Keep the ambiguity check and the persistent write under a single recovery scope: if either the
    /// check or `store` fails (e.g. Keeper returns an error or loses the connection), restore the old
    /// handler so that `loaded_handlers` is never left missing an entry that still exists in storage.
    try
    {
        checkAmbiguity(*updated, loaded_handlers);
        metadata_storage->store(alter_query.handler_name, updated->create_statement, true);
    }
    catch (...)
    {
        loaded_handlers.emplace(alter_query.handler_name, old);
        throw;
    }

    loaded_handlers.emplace(alter_query.handler_name, updated);
    rebuildSnapshot(lock);
}

void SQLDefinedHandlersFactory::updateReplicated(const ASTCreateHandlerQuery & alter_query, std::lock_guard<std::mutex> & lock)
{
    /// Like createReplicated: re-read the set at a known root version, merge the ALTER onto the
    /// version-consistent snapshot, check ambiguity, and persist conditionally on that version, retrying
    /// if another replica changed the set in between.
    for (size_t attempt = 0; attempt < max_replicated_write_attempts; ++attempt)
    {
        Int32 version = 0;
        auto current = metadata_storage->getAll(version);

        auto it = current.find(alter_query.handler_name);
        if (it == current.end())
        {
            loaded_handlers = std::move(current);
            rebuildSnapshot(lock);
            throw Exception(ErrorCodes::HANDLER_DOESNT_EXIST, "Cannot alter handler `{}`, because it doesn't exist", alter_query.handler_name);
        }

        /// Merge onto the statement from this same version-consistent snapshot (no extra Keeper read).
        auto updated = metadata_storage->buildUpdatedHandler(it->second->create_statement, alter_query);
        checkAmbiguity(*updated, current);

        if (metadata_storage->store(alter_query.handler_name, updated->create_statement, /* replace */ true, version))
        {
            current[alter_query.handler_name] = updated;
            loaded_handlers = std::move(current);
            rebuildSnapshot(lock);
            return;
        }
        /// The set changed concurrently: re-read and re-validate.
    }

    throw Exception(ErrorCodes::KEEPER_EXCEPTION,
        "Could not alter handler `{}`: the set of handlers kept changing concurrently", alter_query.handler_name);
}

void SQLDefinedHandlersFactory::reloadFromSQL()
{
    std::lock_guard lock(mutex);
    if (loadIfNot(lock))
        return;

    loaded_handlers = metadata_storage->getAll();
    rebuildSnapshot(lock);
}

void SQLDefinedHandlersFactory::updateFunc()
{
    LOG_TRACE(log, "SQL-defined handlers background updating thread started");

    while (!shutdown_called.load())
    {
        try
        {
            if (metadata_storage->waitUpdate())
                reloadFromSQL();
        }
        catch (const Coordination::Exception & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                LOG_INFO(log, "Lost ZooKeeper connection, will try to connect again: {}", DB::getCurrentExceptionMessage(true));
                sleepForSeconds(1);
            }
            else
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
                chassert(false);
            }
            continue;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
            chassert(false);
            continue;
        }
    }

    LOG_TRACE(log, "SQL-defined handlers background updating thread finished");
}

}
