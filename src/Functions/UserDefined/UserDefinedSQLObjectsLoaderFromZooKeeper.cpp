#include <Functions/UserDefined/UserDefinedSQLObjectsLoaderFromZooKeeper.h>

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


namespace DB
{

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
    extern const int NO_ZOOKEEPER;
    extern const int BAD_ARGUMENTS;
}


UserDefinedSQLObjectsLoaderFromZooKeeper::UserDefinedSQLObjectsLoaderFromZooKeeper(
    const ContextPtr & global_context_, const String & zookeeper_path_)
    : global_context(global_context_)
    , zookeeper_path{zookeeper_path_}
    , log{&Poco::Logger::get("UserDefinedSQLObjectsLoaderFromZooKeeper")}
    , watch_queue(std::make_shared<ConcurrentBoundedQueue<String>>(std::numeric_limits<size_t>::max()))
{
    if (zookeeper_path.empty())
        throw Exception("ZooKeeper path must be non-empty", ErrorCodes::BAD_ARGUMENTS);

    if (zookeeper_path.back() == '/')
        zookeeper_path.resize(zookeeper_path.size() - 1);

    /// If zookeeper chroot prefix is used, path should start with '/', because chroot concatenates without it.
    if (zookeeper_path.front() != '/')
        zookeeper_path = "/" + zookeeper_path;
}


UserDefinedSQLObjectsLoaderFromZooKeeper::~UserDefinedSQLObjectsLoaderFromZooKeeper()
{
    try
    {
        stopWatchingThread();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::loadObjects(bool ignore_network_errors, bool start_watching)
{
    try
    {
        getZooKeeper();
    }
    catch (...)
    {
        if (ignore_network_errors)
            tryLogCurrentException(log, "Failed to load user-defined objects");
        else
            throw;
    }

    if (start_watching)
        startWatchingThread();
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::stopWatching()
{
    stopWatchingThread();
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::reloadObjects()
{
    std::lock_guard lock{cached_zookeeper_ptr_mutex};
    cached_zookeeper_ptr = nullptr;
    getZooKeeperNoLock();
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::reloadObject(UserDefinedSQLObjectType object_type, const String & object_name)
{
    auto zookeeper = getZooKeeper();
    refreshObject(zookeeper, object_type, object_name);
}


zkutil::ZooKeeperPtr UserDefinedSQLObjectsLoaderFromZooKeeper::getZooKeeper()
{
    std::lock_guard lock{cached_zookeeper_ptr_mutex};
    return getZooKeeperNoLock();
}


zkutil::ZooKeeperPtr UserDefinedSQLObjectsLoaderFromZooKeeper::getZooKeeperNoLock()
{
    if (!cached_zookeeper_ptr || cached_zookeeper_ptr->expired())
    {
        auto zookeeper = global_context->getZooKeeper();
        if (!zookeeper)
            throw Exception("Can't use user-defined SQL objects stored in ZooKeeper without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

        /// It's possible that we connected to different [Zoo]Keeper instance
        /// so we may read a bit stale state.
        zookeeper->sync(zookeeper_path);

        createRootNodes(zookeeper);
        refreshObjects(zookeeper, /* force_refresh_all= */ true);
        cached_zookeeper_ptr = zookeeper;
    }
    return cached_zookeeper_ptr;
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::createRootNodes(const zkutil::ZooKeeperPtr & zookeeper)
{
    zookeeper->createAncestors(zookeeper_path);
    zookeeper->createIfNotExists(zookeeper_path, "");
    zookeeper->createIfNotExists(zookeeper_path + "/functions", "");
}


bool UserDefinedSQLObjectsLoaderFromZooKeeper::storeObject(UserDefinedSQLObjectType object_type, const String & object_name, const IAST & create_object_query, bool throw_if_exists, bool replace_if_exists, const Settings &)
{
    String path = getNodePath(object_type, object_name);
    LOG_DEBUG(log, "Storing user-defined object {} at zk path {}", backQuote(object_name), path);

    WriteBufferFromOwnString create_statement_buf;
    formatAST(create_object_query, create_statement_buf, false);
    writeChar('\n', create_statement_buf);
    String create_statement = create_statement_buf.str();

    auto zookeeper = getZooKeeper();

    size_t num_attempts = 10;
    while (true)
    {
        auto code = zookeeper->tryCreate(path, create_statement, zkutil::CreateMode::Persistent);
        if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
            throw zkutil::KeeperException(code, path);

        if (code == Coordination::Error::ZNODEEXISTS)
        {
            if (throw_if_exists)
                throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User-defined function '{}' already exists", object_name);
            else if (!replace_if_exists)
                return false;

            code = zookeeper->trySet(path, create_statement);
            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNONODE))
                throw zkutil::KeeperException(code, path);

        }

        if (code == Coordination::Error::ZOK)
            break;

        if (!--num_attempts)
            throw zkutil::KeeperException(code, path);
    }

    LOG_TRACE(log, "Object {} removed", backQuote(object_name));
    return true;
}


bool UserDefinedSQLObjectsLoaderFromZooKeeper::removeObject(UserDefinedSQLObjectType object_type, const String & object_name, bool throw_if_not_exists)
{
    String path = getNodePath(object_type, object_name);
    LOG_DEBUG(log, "Removing user-defined object {} at zk path {}", backQuote(object_name), path);

    auto zookeeper = getZooKeeper();

    auto code = zookeeper->tryRemove(path);
    if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNONODE))
        throw zkutil::KeeperException(code, path);

    if (code == Coordination::Error::ZNONODE)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", object_name);
        else
            return false;
    }

    LOG_TRACE(log, "Object {} removed", backQuote(object_name));
    return true;
}


ASTPtr UserDefinedSQLObjectsLoaderFromZooKeeper::tryLoadObject(
    const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type, const String & object_name)
{
    String path = getNodePath(object_type, object_name);
    LOG_DEBUG(log, "Loading user defined object {} from zk path {}", backQuote(object_name), path);

    try
    {
        const auto watch_entity = [watch_queue = watch_queue, object_name](const Coordination::WatchResponse & response)
        {
            if (response.type == Coordination::Event::CHANGED)
                [[maybe_unused]] bool push_result = watch_queue->push(object_name);
        };

        Coordination::Stat entity_stat;
        String object_create_query;
        bool exists = zookeeper->tryGetWatch(path, object_create_query, &entity_stat, watch_entity);

        if (!exists)
            return nullptr;

        switch (object_type)
        {
            case UserDefinedSQLObjectType::Function:
            {
                ParserCreateFunctionQuery parser;
                ASTPtr ast = parseQuery(
                    parser,
                    object_create_query.data(),
                    object_create_query.data() + object_create_query.size(),
                    "",
                    0,
                    global_context->getSettingsRef().max_parser_depth);
                return ast;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while loading user defined SQL object {}", backQuote(object_name)));
        return nullptr; /// Failed to load this sql object, will ignore it
    }
}


String UserDefinedSQLObjectsLoaderFromZooKeeper::getNodePath(UserDefinedSQLObjectType object_type, const String & object_name) const
{
    String node_path;
    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function:
        {
            node_path = zookeeper_path + "/function_" + escapeForFileName(object_name) + ".sql";
            break;
        }
    }
    return node_path;
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::startWatchingThread()
{
    bool prev_watching_flag = watching.exchange(true);
    if (!prev_watching_flag)
        watching_thread = ThreadFromGlobalPool(&UserDefinedSQLObjectsLoaderFromZooKeeper::runWatchingThread, this);
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::stopWatchingThread()
{
    bool prev_watching_flag = watching.exchange(false);
    if (prev_watching_flag)
    {
        watch_queue->finish();
        if (watching_thread.joinable())
            watching_thread.join();
    }
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::runWatchingThread()
{
    LOG_DEBUG(log, "Started watching thread");
    setThreadName("UserDefObjWatch");

    while (watching)
    {
        try
        {
            refresh();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Will try to restart watching thread after error");
            resetAfterError();
            sleepForSeconds(5);
            continue;
        }
    }
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::refresh()
{
    getZooKeeper();

    String object_name;
    if (!watch_queue->tryPop(object_name, /* timeout_ms: */ 10000))
        return;

    auto zookeeper = getZooKeeper();

    if (object_name.empty())
        refreshObjects(zookeeper, /* force_refresh_all= */ false);
    else
        refreshObject(zookeeper, UserDefinedSQLObjectType::Function, object_name);
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::refreshObjects(const zkutil::ZooKeeperPtr & zookeeper, bool force_refresh_all)
{
    LOG_DEBUG(log, "Refreshing user-defined objects");

    if (force_refresh_all)
    {
        /// It doesn't make sense to keep the queue because we will reread everything in this function.
        watch_queue->clear();
    }

    auto watch_entities_list = [watch_queue = watch_queue](const Coordination::WatchResponse &)
    {
        [[maybe_unused]] bool push_result = watch_queue->push("");
    };

    Coordination::Stat stat;
    const auto node_names = zookeeper->getChildrenWatch(zookeeper_path, &stat, watch_entities_list);

    Strings function_names;
    function_names.reserve(node_names.size());
    for (const auto & node_name : node_names)
    {
        if (!startsWith(node_name, "function_") || !endsWith(node_name, ".sql"))
            continue;

        size_t prefix_length = strlen("function_");
        size_t suffix_length = strlen(".sql");
        String function_name = unescapeForFileName(node_name.substr(prefix_length, node_name.length() - prefix_length - suffix_length));

        if (function_name.empty())
            continue;

        function_names.emplace_back(std::move(function_name));
    }

    auto & factory = UserDefinedSQLFunctionFactory::instance();

    if (force_refresh_all)
    {
        /// force_refresh_all=true means we read & parse all SQL objects from ZooKeeper.
        std::vector<std::pair<String, ASTPtr>> function_names_and_asts;
        for (const auto & function_name : function_names)
        {
            if (auto ast = tryLoadObject(zookeeper, UserDefinedSQLObjectType::Function, function_name))
                function_names_and_asts.emplace_back(function_name, ast);
        }
        factory.setAllFunctions(function_names_and_asts);
    }
    else
    {
        /// force_refresh_all=false means we read & parse only new SQL objects from ZooKeeper.
        auto lock = factory.getLock();
        factory.removeAllFunctionsExcept(function_names);
        for (const auto & function_name : function_names)
        {
            if (!UserDefinedSQLFunctionFactory::instance().has(function_name))
                refreshObject(zookeeper, UserDefinedSQLObjectType::Function, function_name);
        }
    }

    LOG_TRACE(log, "Objects refreshed");
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::refreshObject(const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type, const String & object_name)
{
    auto ast = tryLoadObject(zookeeper, object_type, object_name);
    auto & factory = UserDefinedSQLFunctionFactory::instance();
    if (ast)
        factory.setFunction(object_name, *ast);
    else
        factory.removeFunction(object_name);
}


void UserDefinedSQLObjectsLoaderFromZooKeeper::resetAfterError()
{
    /// Make watching thread reinitialize ZooKeeper and reread everything.
    std::lock_guard lock{cached_zookeeper_ptr_mutex};
    cached_zookeeper_ptr = nullptr;
}

}
