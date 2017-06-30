#include <functional>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>


namespace ProfileEvents
{
    extern const Event ZooKeeperInit;
    extern const Event ZooKeeperTransactions;
    extern const Event ZooKeeperCreate;
    extern const Event ZooKeeperRemove;
    extern const Event ZooKeeperExists;
    extern const Event ZooKeeperMulti;
    extern const Event ZooKeeperGet;
    extern const Event ZooKeeperSet;
    extern const Event ZooKeeperGetChildren;
}

namespace CurrentMetrics
{
    extern const Metric ZooKeeperWatch;
}


namespace zkutil
{

const int CreateMode::Persistent = 0;
const int CreateMode::Ephemeral = ZOO_EPHEMERAL;
const int CreateMode::EphemeralSequential = ZOO_EPHEMERAL | ZOO_SEQUENCE;
const int CreateMode::PersistentSequential = ZOO_SEQUENCE;

void check(int32_t code, const std::string path = "")
{
    if (code != ZOK)
    {
        if (path.size())
            throw KeeperException(code, path);
        else
            throw KeeperException(code);
    }
}

struct WatchContext
{
    /// ZooKeeper instance exists for the entire WatchContext lifetime.
    ZooKeeper & zk;
    WatchCallback callback;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::ZooKeeperWatch};

    WatchContext(ZooKeeper & zk_, WatchCallback callback_) : zk(zk_), callback(std::move(callback_)) {}

    void process(int32_t event_type, int32_t state, const char * path)
    {
        if (callback)
            callback(zk, event_type, state, path);
    }
};

void ZooKeeper::processCallback(zhandle_t * zh, int type, int state, const char * path, void * watcher_ctx)
{
    WatchContext * context = static_cast<WatchContext *>(watcher_ctx);
    context->process(type, state, path);

    /// It is guaranteed that non-ZOO_SESSION_EVENT notification will be delivered only once
    /// (https://issues.apache.org/jira/browse/ZOOKEEPER-890)
    if (type != ZOO_SESSION_EVENT)
        destroyContext(context);
}

void ZooKeeper::init(const std::string & hosts_, int32_t session_timeout_ms_)
{
    log = &Logger::get("ZooKeeper");
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    hosts = hosts_;
    session_timeout_ms = session_timeout_ms_;

    impl = zookeeper_init(hosts.c_str(), nullptr, session_timeout_ms, nullptr, nullptr, 0);
    ProfileEvents::increment(ProfileEvents::ZooKeeperInit);

    if (!impl)
        throw KeeperException("Fail to initialize zookeeper. Hosts are " + hosts);

    default_acl = &ZOO_OPEN_ACL_UNSAFE;
}

ZooKeeper::ZooKeeper(const std::string & hosts, int32_t session_timeout_ms)
{
    init(hosts, session_timeout_ms);
}

struct ZooKeeperArgs
{
    ZooKeeperArgs(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_name, keys);

        std::vector<std::string> hosts_strings;

        session_timeout_ms = DEFAULT_SESSION_TIMEOUT;
        for (const auto & key : keys)
        {
            if (startsWith(key, "node"))
            {
                hosts_strings.push_back(
                    config.getString(config_name + "." + key + ".host") + ":" + config.getString(config_name + "." + key + ".port", "2181"));
            }
            else if (key == "session_timeout_ms")
            {
                session_timeout_ms = config.getInt(config_name + "." + key);
            }
            else throw KeeperException(std::string("Unknown key ") + key + " in config file");
        }

        /// Shuffle the hosts to distribute the load among ZooKeeper nodes.
        std::random_shuffle(hosts_strings.begin(), hosts_strings.end());

        for (auto & host : hosts_strings)
        {
            if (hosts.size())
                hosts += ",";
            hosts += host;
        }
    }

    std::string hosts;
    size_t session_timeout_ms;
};

ZooKeeper::ZooKeeper(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
{
    ZooKeeperArgs args(config, config_name);
    init(args.hosts, args.session_timeout_ms);
}

WatchCallback ZooKeeper::callbackForEvent(const EventPtr & event)
{
    WatchCallback callback;
    if (event)
    {
        callback = [e=event](ZooKeeper &, int, int, const char *) mutable
        {
            if (e)
            {
                e->set();
                e.reset(); /// The event is set only once, even if the callback can fire multiple times due to session events.
            }
        };
    }
    return callback;
}

WatchContext * ZooKeeper::createContext(WatchCallback && callback)
{
    if (callback)
    {
        WatchContext * res = new WatchContext(*this, std::move(callback));
        {
            std::lock_guard<std::mutex> lock(mutex);
            watch_context_store.insert(res);
            if (watch_context_store.size() % 10000 == 0)
            {
                LOG_ERROR(log, "There are " << watch_context_store.size() << " active watches. There must be a leak somewhere.");
            }
        }
        return res;
    }
    else
        return nullptr;
}

void ZooKeeper::destroyContext(WatchContext * context)
{
    if (context)
    {
        std::lock_guard<std::mutex> lock(context->zk.mutex);
        context->zk.watch_context_store.erase(context);
    }
    delete context;
}

int32_t ZooKeeper::getChildrenImpl(const std::string & path, Strings & res,
                    Stat * stat_,
                    WatchCallback watch_callback)
{
    String_vector strings;
    int code;
    Stat stat;
    watcher_fn watcher = watch_callback ? processCallback : nullptr;
    WatchContext * context = createContext(std::move(watch_callback));
    code = zoo_wget_children2(impl, path.c_str(), watcher, context, &strings, &stat);
    ProfileEvents::increment(ProfileEvents::ZooKeeperGetChildren);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code == ZOK)
    {
        if (stat_)
            *stat_ = stat;
        res.resize(strings.count);
        for (int i = 0; i < strings.count; ++i)
            res[i] = std::string(strings.data[i]);
        deallocate_String_vector(&strings);
    }
    else
    {
        /// The call was unsuccessful, so the watch was not set. Destroy the context.
        destroyContext(context);
    }

    return code;
}
Strings ZooKeeper::getChildren(
    const std::string & path, Stat * stat, const EventPtr & watch)
{
    Strings res;
    check(tryGetChildren(path, res, stat, watch), path);
    return res;
}

int32_t ZooKeeper::tryGetChildren(const std::string & path, Strings & res,
                                Stat * stat_, const EventPtr & watch)
{
    int32_t code = retry(std::bind(&ZooKeeper::getChildrenImpl, this, std::ref(path), std::ref(res), stat_, callbackForEvent(watch)));

    if (!(    code == ZOK ||
            code == ZNONODE))
        throw KeeperException(code, path);

    return code;
}

int32_t ZooKeeper::createImpl(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    int code;
    /// The name of the created node can be longer than path if the sequential node is created.
    size_t name_buffer_size = path.size() + SEQUENTIAL_SUFFIX_SIZE;
    char * name_buffer = new char[name_buffer_size];

    code = zoo_create(impl, path.c_str(), data.c_str(), data.size(), getDefaultACL(), mode,  name_buffer, name_buffer_size);
    ProfileEvents::increment(ProfileEvents::ZooKeeperCreate);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code == ZOK)
    {
        path_created = std::string(name_buffer);
    }

    delete[] name_buffer;

    return code;
}

std::string ZooKeeper::create(const std::string & path, const std::string & data, int32_t type)
{
    std::string path_created;
    check(tryCreate(path, data, type, path_created), path);
    return path_created;
}

int32_t ZooKeeper::tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    int code = createImpl(path, data, mode, path_created);

    if (!(    code == ZOK ||
            code == ZNONODE ||
            code == ZNODEEXISTS ||
            code == ZNOCHILDRENFOREPHEMERALS))
        throw KeeperException(code, path);

    return code;
}

int32_t ZooKeeper::tryCreate(const std::string & path, const std::string & data, int32_t mode)
{
    std::string path_created;
    return tryCreate(path, data, mode, path_created);
}

int32_t ZooKeeper::tryCreateWithRetries(const std::string & path, const std::string & data, int32_t mode, std::string & path_created, size_t* attempt)
{
    return retry([&path, &data, mode, &path_created, this] { return tryCreate(path, data, mode, path_created); }, attempt);
}


void ZooKeeper::createIfNotExists(const std::string & path, const std::string & data)
{
    std::string path_created;
    int32_t code = retry(std::bind(&ZooKeeper::createImpl, this, std::ref(path), std::ref(data), zkutil::CreateMode::Persistent, std::ref(path_created)));

    if (code == ZOK || code == ZNODEEXISTS)
        return;
    else
        throw KeeperException(code, path);
}

void ZooKeeper::createAncestors(const std::string & path)
{
    size_t pos = 1;
    while (true)
    {
        pos = path.find('/', pos);
        if (pos == std::string::npos)
            break;
        createIfNotExists(path.substr(0, pos), "");
        ++pos;
    }
}

int32_t ZooKeeper::removeImpl(const std::string & path, int32_t version)
{
    int32_t code = zoo_delete(impl, path.c_str(), version);
    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return code;
}

void ZooKeeper::remove(const std::string & path, int32_t version)
{
    check(tryRemove(path, version), path);
}

void ZooKeeper::removeWithRetries(const std::string & path, int32_t version)
{
    size_t attempt;
    int code = tryRemoveWithRetries(path, version, &attempt);

    if (!(code == ZOK || (code == ZNONODE && attempt > 0)))
        throw KeeperException(code, path);
}

int32_t ZooKeeper::tryRemove(const std::string & path, int32_t version)
{
    int32_t code = removeImpl(path, version);
    if (!(    code == ZOK ||
            code == ZNONODE ||
            code == ZBADVERSION ||
            code == ZNOTEMPTY))
        throw KeeperException(code, path);
    return code;
}

int32_t ZooKeeper::tryRemoveWithRetries(const std::string & path, int32_t version, size_t * attempt)
{
    int32_t code = retry(std::bind(&ZooKeeper::removeImpl, this, std::ref(path), version), attempt);
    if (!(    code == ZOK ||
            code == ZNONODE ||
            code == ZBADVERSION ||
            code == ZNOTEMPTY))
    {
        throw KeeperException(code, path);
    }

    return code;
}

int32_t ZooKeeper::tryRemoveEphemeralNodeWithRetries(const std::string & path, int32_t version, size_t * attempt)
{
    try
    {
        return tryRemoveWithRetries(path, version, attempt);
    }
    catch (const KeeperException &)
    {
        /// Set the flag indicating that the session is better treated as expired so that someone
        /// recreates it and the ephemeral nodes are indeed deleted.
        is_dirty = true;

        throw;
    }
}

int32_t ZooKeeper::existsImpl(const std::string & path, Stat * stat_, WatchCallback watch_callback)
{
    int32_t code;
    Stat stat;
    watcher_fn watcher = watch_callback ? processCallback : nullptr;
    WatchContext * context = createContext(std::move(watch_callback));
    code = zoo_wexists(impl, path.c_str(), watcher, context, &stat);
    ProfileEvents::increment(ProfileEvents::ZooKeeperExists);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code == ZOK)
    {
        if (stat_)
            *stat_ = stat;
    }
    if (code != ZOK && code != ZNONODE)
    {
        /// The call was unsuccessful, so the watch was not set. Destroy the context.
        destroyContext(context);
    }

    return code;
}

bool ZooKeeper::exists(const std::string & path, Stat * stat_, const EventPtr & watch)
{
    int32_t code = retry(std::bind(&ZooKeeper::existsImpl, this, path, stat_, callbackForEvent(watch)));

    if (!(    code == ZOK ||
            code == ZNONODE))
        throw KeeperException(code, path);
    if (code == ZNONODE)
        return false;
    return true;
}

bool ZooKeeper::existsWatch(const std::string & path, Stat * stat_, const WatchCallback & watch_callback)
{
    int32_t code = retry(std::bind(&ZooKeeper::existsImpl, this, path, stat_, watch_callback));

    if (!(    code == ZOK ||
            code == ZNONODE))
        throw KeeperException(code, path);
    if (code == ZNONODE)
        return false;
    return true;
}

int32_t ZooKeeper::getImpl(const std::string & path, std::string & res, Stat * stat_, WatchCallback watch_callback)
{
    char buffer[MAX_NODE_SIZE];
    int buffer_len = MAX_NODE_SIZE;
    int32_t code;
    Stat stat;
    watcher_fn watcher = watch_callback ? processCallback : nullptr;
    WatchContext * context = createContext(std::move(watch_callback));

    code = zoo_wget(impl, path.c_str(), watcher, context, buffer, &buffer_len, &stat);
    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code == ZOK)
    {
        if (stat_)
            *stat_ = stat;

        if (buffer_len < 0)        /// This can happen if the node contains NULL. Do not distinguish it from the empty string.
            res.clear();
        else
            res.assign(buffer, buffer_len);
    }
    else
    {
        /// The call was unsuccessful, so the watch was not set. Destroy the context.
        destroyContext(context);
    }
    return code;
}

std::string ZooKeeper::get(const std::string & path, Stat * stat, const EventPtr & watch)
{
    int code;
    std::string res;
    if (tryGet(path, res, stat, watch, &code))
        return res;
    else
        throw KeeperException("Can't get data for node " + path + ": node doesn't exist", code);
}

bool ZooKeeper::tryGet(const std::string & path, std::string & res, Stat * stat_, const EventPtr & watch, int * return_code)
{
    int32_t code = retry(std::bind(&ZooKeeper::getImpl, this, std::ref(path), std::ref(res), stat_, callbackForEvent(watch)));

    if (!(code == ZOK ||
            code == ZNONODE))
        throw KeeperException(code, path);

    if (return_code)
        *return_code = code;

    return code == ZOK;
}

bool ZooKeeper::tryGetWatch(const std::string & path, std::string & res, Stat * stat_, const WatchCallback & watch_callback, int * return_code)
{
    int32_t code = retry(std::bind(&ZooKeeper::getImpl, this, std::ref(path), std::ref(res), stat_, watch_callback));

    if (!(code == ZOK ||
            code == ZNONODE))
        throw KeeperException(code, path);

    if (return_code)
        *return_code = code;

    return code == ZOK;
}

int32_t ZooKeeper::setImpl(const std::string & path, const std::string & data,
                        int32_t version, Stat * stat_)
{
    Stat stat;
    int32_t code = zoo_set2(impl, path.c_str(), data.c_str(), data.length(), version, &stat);
    ProfileEvents::increment(ProfileEvents::ZooKeeperSet);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code == ZOK)
    {
        if (stat_)
            *stat_ = stat;
    }
    return code;
}

void ZooKeeper::set(const std::string & path, const std::string & data, int32_t version, Stat * stat)
{
    check(trySet(path, data, version, stat), path);
}

void ZooKeeper::createOrUpdate(const std::string & path, const std::string & data, int32_t mode)
{
    int code = trySet(path, data, -1);
    if (code == ZNONODE)
    {
        create(path, data, mode);
    }
    else if (code != ZOK)
        throw zkutil::KeeperException(code, path);
}

int32_t ZooKeeper::trySet(const std::string & path, const std::string & data,
                                    int32_t version, Stat * stat_)
{
    int32_t code = setImpl(path, data, version, stat_);

    if (!(    code == ZOK ||
            code == ZNONODE ||
            code == ZBADVERSION))
        throw KeeperException(code, path);
    return code;
}

int32_t ZooKeeper::multiImpl(const Ops & ops_, OpResultsPtr * out_results_)
{
    if (ops_.empty())
        return ZOK;

    /// Workaround of the libzookeeper bug. If the session is expired, zoo_multi sometimes
    /// segfaults.
    /// Possibly, there is a race condition and a segfault is still possible if the session
    /// expires between this check and zoo_multi call.
    /// TODO: check if the bug is fixed in the latest version of libzookeeper.
    if (expired())
        return ZINVALIDSTATE;

    size_t count = ops_.size();
    OpResultsPtr out_results(new OpResults(count));

    /// Copy the struct containing pointers with default copy-constructor.
    /// It is safe because it hasn't got a destructor.
    std::vector<zoo_op_t> ops;
    for (const auto & op : ops_)
        ops.push_back(*(op->data));

    int32_t code = zoo_multi(impl, ops.size(), ops.data(), out_results->data());
    ProfileEvents::increment(ProfileEvents::ZooKeeperMulti);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (out_results_)
        *out_results_ = out_results;

    return code;
}

OpResultsPtr ZooKeeper::multi(const Ops & ops)
{
    OpResultsPtr results;
    check(tryMulti(ops, &results));
    return results;
}

int32_t ZooKeeper::tryMulti(const Ops & ops_, OpResultsPtr * out_results_)
{
    int32_t code = multiImpl(ops_, out_results_);

    if (!(code == ZOK ||
        code == ZNONODE ||
        code == ZNODEEXISTS ||
        code == ZNOCHILDRENFOREPHEMERALS ||
        code == ZBADVERSION ||
        code == ZNOTEMPTY))
            throw KeeperException(code);
    return code;
}

int32_t ZooKeeper::tryMultiWithRetries(const Ops & ops, OpResultsPtr * out_results, size_t * attempt)
{
    int32_t code = retry(std::bind(&ZooKeeper::multiImpl, this, std::ref(ops), out_results), attempt);
    if (!(code == ZOK ||
        code == ZNONODE ||
        code == ZNODEEXISTS ||
        code == ZNOCHILDRENFOREPHEMERALS ||
        code == ZBADVERSION ||
        code == ZNOTEMPTY))
            throw KeeperException(code);
    return code;
}

static const int BATCH_SIZE = 100;

void ZooKeeper::removeChildrenRecursive(const std::string & path)
{
    Strings children = getChildren(path);
    while (!children.empty())
    {
        zkutil::Ops ops;
        for (size_t i = 0; i < BATCH_SIZE && !children.empty(); ++i)
        {
            removeChildrenRecursive(path + "/" + children.back());
            ops.emplace_back(std::make_unique<Op::Remove>(path + "/" + children.back(), -1));
            children.pop_back();
        }
        multi(ops);
    }
}

void ZooKeeper::tryRemoveChildrenRecursive(const std::string & path)
{
    Strings children;
    if (tryGetChildren(path, children) != ZOK)
        return;
    while (!children.empty())
    {
        zkutil::Ops ops;
        Strings batch;
        for (size_t i = 0; i < BATCH_SIZE && !children.empty(); ++i)
        {
            batch.push_back(path + "/" + children.back());
            children.pop_back();
            tryRemoveChildrenRecursive(batch.back());
            ops.emplace_back(std::make_unique<Op::Remove>(batch.back(), -1));
        }

        /// Try to remove the children with a faster method - in bulk. If this fails,
        /// this means someone is concurrently removing these children and we will have
        /// to remove them one by one.
        if (tryMulti(ops) != ZOK)
        {
            for (const std::string & child : batch)
            {
                tryRemove(child);
            }
        }
    }
}

void ZooKeeper::removeRecursive(const std::string & path)
{
    removeChildrenRecursive(path);
    remove(path);
}

void ZooKeeper::tryRemoveRecursive(const std::string & path)
{
    tryRemoveChildrenRecursive(path);
    tryRemove(path);
}


void ZooKeeper::waitForDisappear(const std::string & path)
{
    while (true)
    {
        zkutil::EventPtr event = std::make_shared<Poco::Event>();

        std::string unused;
        /// Use get instead of exists to prevent watch leak if the node has already disappeared.
        if (!tryGet(path, unused, nullptr, event))
            break;

        event->wait();
    }
}

ZooKeeper::~ZooKeeper()
{
    LOG_INFO(&Logger::get("~ZooKeeper"), "Closing ZooKeeper session");

    int code = zookeeper_close(impl);
    if (code != ZOK)
    {
        LOG_ERROR(&Logger::get("~ZooKeeper"), "Failed to close ZooKeeper session: " << zerror(code));
    }

    LOG_INFO(&Logger::get("~ZooKeeper"), "Removing " << watch_context_store.size() << " watches");

    /// Destroy WatchContexts that will never be used.
    for (WatchContext * context : watch_context_store)
        delete context;

    LOG_INFO(&Logger::get("~ZooKeeper"), "Removed watches");
}

ZooKeeperPtr ZooKeeper::startNewSession() const
{
    return std::make_shared<ZooKeeper>(hosts, session_timeout_ms);
}

Op::Create::Create(const std::string & path_, const std::string & value_, ACLPtr acl, int32_t flags)
    : path(path_), value(value_), created_path(path.size() + ZooKeeper::SEQUENTIAL_SUFFIX_SIZE)
{
    zoo_create_op_init(data.get(), path.c_str(), value.c_str(), value.size(), acl, flags, created_path.data(), created_path.size());
}

ACLPtr ZooKeeper::getDefaultACL()
{
    std::lock_guard<std::mutex> lock(mutex);
    return default_acl;
}

void ZooKeeper::setDefaultACL(ACLPtr new_acl)
{
    std::lock_guard<std::mutex> lock(mutex);
    default_acl = new_acl;
}

std::string ZooKeeper::error2string(int32_t code)
{
    return zerror(code);
}

bool ZooKeeper::expired()
{
    return is_dirty || zoo_state(impl) == ZOO_EXPIRED_SESSION_STATE;
}

Int64 ZooKeeper::getClientID()
{
    return zoo_client_id(impl)->client_id;
}


ZooKeeper::GetFuture ZooKeeper::asyncGet(const std::string & path)
{
    GetFuture future {
        [path] (int rc, const char * value, int value_len, const Stat * stat)
        {
            if (rc != ZOK)
                throw KeeperException(rc, path);

            std::string value_str;
            if (value_len > 0)    /// May be otherwise of the node contains NULL. We don't distinguish it from the empty string.
                value_str = { value, size_t(value_len) };

            return ValueAndStat{ value_str, stat ? *stat : Stat() };
        }};

    int32_t code = zoo_aget(
        impl, path.c_str(), 0,
        [] (int rc, const char * value, int value_len, const Stat * stat, const void * data)
        {
            GetFuture::TaskPtr owned_task = std::move(const_cast<GetFuture::TaskPtr &>(*static_cast<const GetFuture::TaskPtr *>(data)));
            (*owned_task)(rc, value, value_len, stat);
        },
        future.task.get());

    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code != ZOK)
        throw KeeperException(code, path);

    return future;
}

ZooKeeper::TryGetFuture ZooKeeper::asyncTryGet(const std::string & path)
{
    TryGetFuture future {
        [path] (int rc, const char * value, int value_len, const Stat * stat)
        {
            if (rc != ZOK && rc != ZNONODE)
                throw KeeperException(rc, path);

            std::string value_str;
            if (value_len > 0)    /// May be otherwise of the node contains NULL. We don't distinguish it from the empty string.
                value_str = { value, size_t(value_len) };

            return ValueAndStatAndExists{ value_str, stat ? *stat : Stat(), rc != ZNONODE };
        }};

    int32_t code = zoo_aget(
        impl, path.c_str(), 0,
        [] (int rc, const char * value, int value_len, const Stat * stat, const void * data)
        {
            TryGetFuture::TaskPtr owned_task = std::move(const_cast<TryGetFuture::TaskPtr &>(*static_cast<const TryGetFuture::TaskPtr *>(data)));
            (*owned_task)(rc, value, value_len, stat);
        },
        future.task.get());

    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code != ZOK)
        throw KeeperException(code, path);

    return future;
}

ZooKeeper::ExistsFuture ZooKeeper::asyncExists(const std::string & path)
{
    ExistsFuture future {
        [path] (int rc, const Stat * stat)
        {
            if (rc != ZOK && rc != ZNONODE)
                throw KeeperException(rc, path);

            return StatAndExists{ stat ? *stat : Stat(), rc != ZNONODE };
        }};

    int32_t code = zoo_aexists(
        impl, path.c_str(), 0,
        [] (int rc, const Stat * stat, const void * data)
        {
            ExistsFuture::TaskPtr owned_task = std::move(const_cast<ExistsFuture::TaskPtr &>(*static_cast<const ExistsFuture::TaskPtr *>(data)));
            (*owned_task)(rc, stat);
        },
        future.task.get());

    ProfileEvents::increment(ProfileEvents::ZooKeeperExists);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code != ZOK)
        throw KeeperException(code, path);

    return future;
}

ZooKeeper::GetChildrenFuture ZooKeeper::asyncGetChildren(const std::string & path)
{
    GetChildrenFuture future {
        [path] (int rc, const String_vector * strings)
        {
            if (rc != ZOK)
                throw KeeperException(rc, path);

            Strings res;
            res.resize(strings->count);
            for (int i = 0; i < strings->count; ++i)
                res[i] = std::string(strings->data[i]);

            return res;
        }};

    int32_t code = zoo_aget_children(
        impl, path.c_str(), 0,
        [] (int rc, const String_vector * strings, const void * data)
        {
            GetChildrenFuture::TaskPtr owned_task =
                std::move(const_cast<GetChildrenFuture::TaskPtr &>(*static_cast<const GetChildrenFuture::TaskPtr *>(data)));
            (*owned_task)(rc, strings);
        },
        future.task.get());

    ProfileEvents::increment(ProfileEvents::ZooKeeperGetChildren);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code != ZOK)
        throw KeeperException(code, path);

    return future;
}

ZooKeeper::RemoveFuture ZooKeeper::asyncRemove(const std::string & path)
{
    RemoveFuture future {
        [path] (int rc)
        {
            if (rc != ZOK)
                throw KeeperException(rc, path);
        }};

    int32_t code = zoo_adelete(
        impl, path.c_str(), -1,
        [] (int rc, const void * data)
        {
            RemoveFuture::TaskPtr owned_task =
                std::move(const_cast<RemoveFuture::TaskPtr &>(*static_cast<const RemoveFuture::TaskPtr *>(data)));
            (*owned_task)(rc);
        },
        future.task.get());

    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

    if (code != ZOK)
        throw KeeperException(code, path);

    return future;
}

}
