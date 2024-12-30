#include <Functions/UserDefined/UserDefinedSQLObjectsZooKeeperStorage.h>

#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Interpreters/Context.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <base/sleep.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 max_parser_backtracks;
extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    std::string_view getNodePrefix(UserDefinedSQLObjectType object_type)
    {
        switch (object_type)
        {
            case UserDefinedSQLObjectType::Function:
                return "function_";
        }
    }

    constexpr std::string_view sql_extension = ".sql";

    String getNodePath(const String & root_path, UserDefinedSQLObjectType object_type, const String & object_name)
    {
        return root_path + "/" + String{getNodePrefix(object_type)} + escapeForFileName(object_name) + String{sql_extension};
    }
}


UserDefinedSQLObjectsZooKeeperStorage::UserDefinedSQLObjectsZooKeeperStorage(
    const ContextPtr & global_context_, const String & zookeeper_path_)
    : UserDefinedSQLObjectsStorageBase(global_context_)
    , zookeeper_getter{[global_context_]() { return global_context_->getZooKeeper(); }}
    , zookeeper_path{zookeeper_path_}
    , watch_queue{std::make_shared<ConcurrentBoundedQueue<std::pair<UserDefinedSQLObjectType, String>>>(std::numeric_limits<size_t>::max())}
    , log{getLogger("UserDefinedSQLObjectsLoaderFromZooKeeper")}
{
    if (zookeeper_path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ZooKeeper path must be non-empty");

    if (zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);

    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;
}

UserDefinedSQLObjectsZooKeeperStorage::~UserDefinedSQLObjectsZooKeeperStorage()
{
    SCOPE_EXIT_SAFE(stopWatchingThread());
}

void UserDefinedSQLObjectsZooKeeperStorage::startWatchingThread()
{
    if (!watching_flag.exchange(true))
    {
        watching_thread = ThreadFromGlobalPool(&UserDefinedSQLObjectsZooKeeperStorage::processWatchQueue, this);
    }
}

void UserDefinedSQLObjectsZooKeeperStorage::stopWatchingThread()
{
    if (watching_flag.exchange(false))
    {
        watch_queue->finish();
        if (watching_thread.joinable())
            watching_thread.join();
    }
}

zkutil::ZooKeeperPtr UserDefinedSQLObjectsZooKeeperStorage::getZooKeeper()
{
    auto [zookeeper, session_status] = zookeeper_getter.getZooKeeper();

    if (session_status == zkutil::ZooKeeperCachingGetter::SessionStatus::New)
    {
        /// It's possible that we connected to different [Zoo]Keeper instance
        /// so we may read a bit stale state.
        zookeeper->sync(zookeeper_path);

        createRootNodes(zookeeper);
        refreshAllObjects(zookeeper);
    }

    return zookeeper;
}

void UserDefinedSQLObjectsZooKeeperStorage::initZooKeeperIfNeeded()
{
    getZooKeeper();
}

void UserDefinedSQLObjectsZooKeeperStorage::resetAfterError()
{
    zookeeper_getter.resetCache();
}


void UserDefinedSQLObjectsZooKeeperStorage::loadObjects()
{
    /// loadObjects() is called at start from Server::main(), so it's better not to stop here on no connection to ZooKeeper or any other error.
    /// However the watching thread must be started anyway in case the connection will be established later.
    if (!objects_loaded)
    {
        try
        {
            reloadObjects();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to load user-defined objects");
        }
    }
    startWatchingThread();
}


void UserDefinedSQLObjectsZooKeeperStorage::processWatchQueue()
{
    LOG_DEBUG(log, "Started watching thread");
    setThreadName("UserDefObjWatch");

    while (watching_flag)
    {
        try
        {
            UserDefinedSQLObjectTypeAndName watched_object;

            /// Re-initialize ZooKeeper session if expired and refresh objects
            initZooKeeperIfNeeded();

            if (!watch_queue->tryPop(watched_object, /* timeout_ms: */ 10000))
                continue;

            auto zookeeper = getZooKeeper();
            const auto & [object_type, object_name] = watched_object;

            if (object_name.empty())
                syncObjects(zookeeper, object_type);
            else
                refreshObject(zookeeper, object_type, object_name);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Will try to restart watching thread after error");
            resetAfterError();
            sleepForSeconds(5);
        }
    }

    LOG_DEBUG(log, "Stopped watching thread");
}


void UserDefinedSQLObjectsZooKeeperStorage::stopWatching()
{
    stopWatchingThread();
}


void UserDefinedSQLObjectsZooKeeperStorage::reloadObjects()
{
    auto zookeeper = getZooKeeper();
    refreshAllObjects(zookeeper);
    startWatchingThread();
}


void UserDefinedSQLObjectsZooKeeperStorage::reloadObject(UserDefinedSQLObjectType object_type, const String & object_name)
{
    auto zookeeper = getZooKeeper();
    refreshObject(zookeeper, object_type, object_name);
}


void UserDefinedSQLObjectsZooKeeperStorage::createRootNodes(const zkutil::ZooKeeperPtr & zookeeper)
{
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
}

bool UserDefinedSQLObjectsZooKeeperStorage::storeObjectImpl(
    const ContextPtr & /*current_context*/,
    UserDefinedSQLObjectType object_type,
    const String & object_name,
    ASTPtr create_object_query,
    bool throw_if_exists,
    bool replace_if_exists,
    const Settings &)
{
    String path = getNodePath(zookeeper_path, object_type, object_name);
    LOG_DEBUG(log, "Storing user-defined object {} at zk path {}", backQuote(object_name), path);

    WriteBufferFromOwnString create_statement_buf;
    formatAST(*create_object_query, create_statement_buf, false);
    writeChar('\n', create_statement_buf);
    String create_statement = create_statement_buf.str();

    auto zookeeper = getZooKeeper();

    size_t num_attempts = 10;
    while (true)
    {
        auto code = zookeeper->tryCreate(path, create_statement, zkutil::CreateMode::Persistent);
        if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
            throw zkutil::KeeperException::fromPath(code, path);

        if (code == Coordination::Error::ZNODEEXISTS)
        {
            if (throw_if_exists)
                throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User-defined function '{}' already exists", object_name);
            if (!replace_if_exists)
                return false;

            code = zookeeper->trySet(path, create_statement);
            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNONODE))
                throw zkutil::KeeperException::fromPath(code, path);
        }

        if (code == Coordination::Error::ZOK)
            break;

        if (!--num_attempts)
            throw zkutil::KeeperException::fromPath(code, path);
    }
    LOG_DEBUG(log, "Object {} stored", backQuote(object_name));

    /// Refresh object and set watch for it. Because it can be replaced by another node after creation.
    refreshObject(zookeeper, object_type, object_name);

    return true;
}


bool UserDefinedSQLObjectsZooKeeperStorage::removeObjectImpl(
    const ContextPtr & /*current_context*/,
    UserDefinedSQLObjectType object_type,
    const String & object_name,
    bool throw_if_not_exists)
{
    String path = getNodePath(zookeeper_path, object_type, object_name);
    LOG_DEBUG(log, "Removing user-defined object {} at zk path {}", backQuote(object_name), path);

    auto zookeeper = getZooKeeper();

    auto code = zookeeper->tryRemove(path);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNONODE))
        throw zkutil::KeeperException::fromPath(code, path);

    if (code == Coordination::Error::ZNONODE)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined object '{}' doesn't exist", object_name);
        return false;
    }

    LOG_DEBUG(log, "Object {} removed", backQuote(object_name));
    return true;
}

bool UserDefinedSQLObjectsZooKeeperStorage::getObjectDataAndSetWatch(
    const zkutil::ZooKeeperPtr & zookeeper,
    String & data,
    const String & path,
    UserDefinedSQLObjectType object_type,
    const String & object_name)
{
    const auto object_watcher = [my_watch_queue = watch_queue, object_type, object_name](const Coordination::WatchResponse & response)
    {
        if (response.type == Coordination::Event::CHANGED)
        {
            [[maybe_unused]] bool inserted = my_watch_queue->emplace(object_type, object_name);
            /// `inserted` can be false if `watch_queue` was already finalized (which happens when stopWatching() is called).
        }
        /// Event::DELETED is processed as child event by getChildren watch
    };

    Coordination::Stat entity_stat;
    return zookeeper->tryGetWatch(path, data, &entity_stat, object_watcher);
}

ASTPtr UserDefinedSQLObjectsZooKeeperStorage::parseObjectData(const String & object_data, UserDefinedSQLObjectType object_type)
{
    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function: {
            ParserCreateFunctionQuery parser;
            ASTPtr ast = parseQuery(
                parser,
                object_data.data(),
                object_data.data() + object_data.size(),
                "",
                0,
                global_context->getSettingsRef()[Setting::max_parser_depth],
                global_context->getSettingsRef()[Setting::max_parser_backtracks]);
            return ast;
        }
    }
    UNREACHABLE();
}

ASTPtr UserDefinedSQLObjectsZooKeeperStorage::tryLoadObject(
    const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type, const String & object_name)
{
    String path = getNodePath(zookeeper_path, object_type, object_name);
    LOG_DEBUG(log, "Loading user defined object {} from zk path {}", backQuote(object_name), path);

    try
    {
        String object_data;
        bool exists = getObjectDataAndSetWatch(zookeeper, object_data, path, object_type, object_name);

        if (!exists)
        {
            LOG_INFO(log, "User-defined object '{}' can't be loaded from path {}, because it doesn't exist", backQuote(object_name), path);
            return nullptr;
        }

        return parseObjectData(object_data, object_type);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while loading user defined SQL object {}", backQuote(object_name)));
        return nullptr; /// Failed to load this sql object, will ignore it
    }
}

Strings UserDefinedSQLObjectsZooKeeperStorage::getObjectNamesAndSetWatch(
    const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type)
{
    auto object_list_watcher = [my_watch_queue = watch_queue, object_type](const Coordination::WatchResponse &)
    {
        [[maybe_unused]] bool inserted = my_watch_queue->emplace(object_type, "");
        /// `inserted` can be false if `watch_queue` was already finalized (which happens when stopWatching() is called).
    };

    Coordination::Stat stat;
    const auto node_names = zookeeper->getChildrenWatch(zookeeper_path, &stat, object_list_watcher);
    const auto prefix = getNodePrefix(object_type);

    Strings object_names;
    object_names.reserve(node_names.size());
    for (const auto & node_name : node_names)
    {
        if (node_name.starts_with(prefix) && node_name.ends_with(sql_extension))
        {
            String object_name = unescapeForFileName(node_name.substr(prefix.length(), node_name.length() - prefix.length() - sql_extension.length()));
            if (!object_name.empty())
                object_names.push_back(std::move(object_name));
        }
    }

    return object_names;
}

void UserDefinedSQLObjectsZooKeeperStorage::refreshAllObjects(const zkutil::ZooKeeperPtr & zookeeper)
{
    /// It doesn't make sense to keep the old watch events because we will reread everything in this function.
    watch_queue->clear();

    refreshObjects(zookeeper, UserDefinedSQLObjectType::Function);
    objects_loaded = true;
}

void UserDefinedSQLObjectsZooKeeperStorage::refreshObjects(const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type)
{
    LOG_DEBUG(log, "Refreshing all user-defined {} objects", object_type);
    Strings object_names = getObjectNamesAndSetWatch(zookeeper, object_type);

    /// Read & parse all SQL objects from ZooKeeper
    std::vector<std::pair<String, ASTPtr>> function_names_and_asts;
    for (const auto & function_name : object_names)
    {
        if (auto ast = tryLoadObject(zookeeper, UserDefinedSQLObjectType::Function, function_name))
            function_names_and_asts.emplace_back(function_name, ast);
    }

    setAllObjects(function_names_and_asts);

    LOG_DEBUG(log, "All user-defined {} objects refreshed", object_type);
}

void UserDefinedSQLObjectsZooKeeperStorage::syncObjects(const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type)
{
    LOG_DEBUG(log, "Syncing user-defined {} objects", object_type);
    Strings object_names = getObjectNamesAndSetWatch(zookeeper, object_type);

    auto lock = getLock();

    /// Remove stale objects
    removeAllObjectsExcept(object_names);
    /// Read & parse only new SQL objects from ZooKeeper
    for (const auto & function_name : object_names)
    {
        if (!UserDefinedSQLFunctionFactory::instance().has(function_name))
            refreshObject(zookeeper, UserDefinedSQLObjectType::Function, function_name);
    }

    LOG_DEBUG(log, "User-defined {} objects synced", object_type);
}

void UserDefinedSQLObjectsZooKeeperStorage::refreshObject(
    const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type, const String & object_name)
{
    auto ast = tryLoadObject(zookeeper, object_type, object_name);

    if (ast)
        setObject(object_name, *ast);
    else
        removeObject(object_name);
}

}
