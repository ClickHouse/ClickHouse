#include <zkutil/ZooKeeper.h>
#include <boost/make_shared.hpp>
#include <Yandex/logger_useful.h>
#include <boost/bind.hpp>

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

typedef std::promise<WatchEventInfo> WatchPromise;

struct WatchWithPromise
{
	WatchPromise promise;
	bool notified;
	/// существует все время существования WatchWithPromise
	ZooKeeper & zk;

	WatchWithPromise(ZooKeeper & zk_) : notified(false), zk(zk_) {}

	void process(zhandle_t * zh, int32_t event, int32_t state, const char * path)
	{
		if (notified)
		{
			LOG_WARNING(&Logger::get("WatchWithPromise"), "Ignoring event " << event << " with state "
				<< state << (path ? std::string(" for path ") + path : ""));
			return;
		}
		promise.set_value(WatchEventInfo(event, state, path));
		notified = true;
	}
};

WatchWithPromisePtr ZooKeeper::watchForFuture(WatchFuture * future)
{
	if (!future)
		return nullptr;
	WatchWithPromisePtr res = new WatchWithPromise(*this);
	watch_store.insert(res);
	*future = res->promise.get_future();
	return res;
}


void ZooKeeper::processPromise(zhandle_t * zh, int type, int state, const char * path, void *watcherCtx)
{
	if (watcherCtx)
	{
		WatchWithPromise * watch = reinterpret_cast<WatchWithPromise *>(watcherCtx);
		watch->process(zh, type, state, path);
		delete watch;
		watch->zk.watch_store.erase(watch);
	}
}

void ZooKeeper::init(const std::string & hosts_, int32_t sessionTimeoutMs_, WatchFunction * watch_)
{
	hosts = hosts_;
	sessionTimeoutMs = sessionTimeoutMs_;
	state_watch = watch_;

	if (watch_)
		impl = zookeeper_init(hosts.c_str(), *watch_, sessionTimeoutMs, nullptr, nullptr, 0);
	else
		impl = zookeeper_init(hosts.c_str(), nullptr, sessionTimeoutMs, nullptr, nullptr, 0);

	if (!impl)
		throw KeeperException("Fail to initialize zookeeper. Hosts are " + hosts);

	default_acl = &ZOO_OPEN_ACL_UNSAFE;
}

ZooKeeper::ZooKeeper(const std::string & hosts, int32_t sessionTimeoutMs, WatchFunction * watch_)
{
	zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
	init(hosts, sessionTimeoutMs, watch_);
}

struct ZooKeeperArgs
{
	ZooKeeperArgs(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
	{
		Poco::Util::AbstractConfiguration::Keys keys;
		config.keys(config_name, keys);
		std::string node_key = "node";

		session_timeout_ms = DEFAULT_SESSION_TIMEOUT;
		for (const auto & key : keys)
		{
			if (key == node_key || key.compare(0, node_key.size(), node_key) == 0)
			{
				if (hosts.size())
					hosts += std::string(",");
				hosts += config.getString(config_name + "." + key + ".host") + ":" + config.getString(config_name + "." + key + ".port");
			}
			else if (key == "session_timeout_ms")
			{
				session_timeout_ms = config.getInt(config_name + "." + key);
			}
			else throw KeeperException(std::string("Unknown key ") + key + " in config file");
		}
	}

	std::string hosts;
	size_t session_timeout_ms;
};

ZooKeeper::ZooKeeper(const Poco::Util::AbstractConfiguration & config, const std::string & config_name,
			  WatchFunction * watch)
{
	ZooKeeperArgs args(config, config_name);
	init(args.hosts, args.session_timeout_ms, watch);
}

int32_t ZooKeeper::getChildrenImpl(const std::string & path, Strings & res,
					Stat * stat_,
					WatchFuture * watch)
{
	String_vector strings;
	int code;
	Stat stat;
	if (watch)
	{
		code = zoo_wget_children2(impl, path.c_str(), processPromise, reinterpret_cast<void *>(watchForFuture(watch)), &strings, &stat);
	}
	else
	{
		code = zoo_wget_children2(impl, path.c_str(), nullptr, nullptr, &strings, &stat);
	}

	if (code == ZOK)
	{
		if (stat_)
			*stat_ = stat;
		res.resize(strings.count);
		for (int i = 0; i < strings.count; ++i)
			res[i] = std::string(strings.data[i]);
		deallocate_String_vector(&strings);
	}

	return code;
}
Strings ZooKeeper::getChildren(
	const std::string & path, Stat * stat, WatchFuture * watch)
{
	Strings res;
	check(tryGetChildren(path, res, stat, watch), path);
	return res;
}

int32_t ZooKeeper::tryGetChildren(const std::string & path, Strings & res,
								Stat * stat_, WatchFuture * watch)
{
	int32_t code = retry(boost::bind(&ZooKeeper::getChildrenImpl, this, boost::ref(path), boost::ref(res), stat_, watch));

	if (!(	code == ZOK ||
			code == ZNONODE))
		throw KeeperException(code, path);

	return code;
}

int32_t ZooKeeper::createImpl(const std::string & path, const std::string & data, int32_t mode, std::string & pathCreated)
{
	int code;
	/// имя ноды может быть больше переданного пути, если создается sequential нода.
	size_t name_buffer_size = path.size() + SEQUENTIAL_SUFFIX_SIZE;
	char * name_buffer = new char[name_buffer_size];

	code = zoo_create(impl, path.c_str(), data.c_str(), data.size(), default_acl, mode,  name_buffer, name_buffer_size);

	if (code == ZOK)
	{
		pathCreated = std::string(name_buffer);
	}

	delete[] name_buffer;

	return code;
}

std::string ZooKeeper::create(const std::string & path, const std::string & data, int32_t type)
{
	std::string  pathCreated;
	check(tryCreate(path, data, type, pathCreated), path);
	return pathCreated;
}

int32_t ZooKeeper::tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & pathCreated)
{
	int code = retry(boost::bind(&ZooKeeper::createImpl, this, boost::ref(path), boost::ref(data), mode, boost::ref(pathCreated)));

	if (!(	code == ZOK ||
			code == ZNONODE ||
			code == ZNODEEXISTS ||
			code == ZNOCHILDRENFOREPHEMERALS))
		throw KeeperException(code, path);

	return code;
}

int32_t ZooKeeper::tryCreate(const std::string & path, const std::string & data, int32_t mode)
{
	std::string pathCreated;
	return tryCreate(path, data, mode, pathCreated);
}

int32_t ZooKeeper::removeImpl(const std::string & path, int32_t version)
{
	int32_t code = zoo_delete(impl, path.c_str(), version);
	return code;
}

void ZooKeeper::remove(const std::string & path, int32_t version)
{
	check(tryRemove(path, version), path);
}

int32_t ZooKeeper::tryRemove(const std::string & path, int32_t version)
{
	int32_t code = retry(boost::bind(&ZooKeeper::removeImpl, this, boost::ref(path), version));
	if (!(	code == ZOK ||
			code == ZNONODE ||
			code == ZBADVERSION ||
			code == ZNOTEMPTY))
		throw KeeperException(code, path);
	return code;
}

int32_t ZooKeeper::existsImpl(const std::string & path, Stat * stat_, WatchFuture * watch)
{
	int32_t code;
	Stat stat;
	if (watch)
		code = zoo_wexists(impl, path.c_str(), processPromise, reinterpret_cast<void *>(watchForFuture(watch)), &stat);
	else
		code = zoo_wexists(impl, path.c_str(), nullptr, nullptr, &stat);

	if (code == ZOK)
	{
		if (stat_)
			*stat_ = stat;
	}

	return code;
}

bool ZooKeeper::exists(const std::string & path, Stat * stat_, WatchFuture * watch)
{
	int32_t code = existsImpl(path, stat_, watch);

	if (!(	code == ZOK ||
			code == ZNONODE))
		throw KeeperException(code, path);
	if (code == ZNONODE)
		return false;
	return true;
}

int32_t ZooKeeper::getImpl(const std::string & path, std::string & res, Stat * stat_, WatchFuture * watch)
{
	char buffer[MAX_NODE_SIZE];
	int buffer_len = MAX_NODE_SIZE;
	int32_t code;
	Stat stat;
	if (watch)
		code = zoo_wget(impl, path.c_str(), processPromise, reinterpret_cast<void *>(watchForFuture(watch)), buffer, &buffer_len, &stat);
	else
		code = zoo_wget(impl, path.c_str(), nullptr, nullptr, buffer, &buffer_len, &stat);

	if (code == ZOK)
	{
		if (stat_)
			*stat_ = stat;

		res = std::string(buffer, buffer_len);
	}
	return code;
}

std::string ZooKeeper::get(const std::string & path, Stat * stat, WatchFuture * watch)
{
	std::string res;
	if (tryGet(path, res, stat, watch))
		return res;
	else
		throw KeeperException("Fail to get data for node " + path);
}

bool ZooKeeper::tryGet(const std::string & path, std::string & res, Stat * stat_, WatchFuture * watch)
{
	int32_t code = retry(boost::bind(&ZooKeeper::getImpl, this, boost::ref(path), boost::ref(res), stat_, watch));

	if (!(	code == ZOK ||
			code == ZNONODE))
		throw KeeperException(code, path);

	if (code == ZOK)
		return true;
	else
		return false;
}

int32_t ZooKeeper::setImpl(const std::string & path, const std::string & data,
						int32_t version, Stat * stat_)
{
	Stat stat;
	int32_t code = zoo_set2(impl, path.c_str(), data.c_str(), data.length(), version, &stat);
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

int32_t ZooKeeper::trySet(const std::string & path, const std::string & data,
									int32_t version, Stat * stat_)
{
	int32_t code = retry(boost::bind(&ZooKeeper::setImpl, this, boost::ref(path), boost::ref(data), version, stat_));

	if (!(	code == ZOK ||
			code == ZNONODE ||
			code == ZBADVERSION))
		throw KeeperException(code, path);
	return code;
}

int32_t ZooKeeper::multiImpl(const Ops & ops_, OpResultsPtr * out_results_)
{
	size_t count = ops_.size();
	OpResultsPtr out_results(new OpResults(count));

	/// копируем структуру, содержащую указатели, дефолтным конструктором копирования
	/// это безопасно, т.к. у нее нет деструктора
	std::vector<zoo_op_t> ops;
	for (const Op & op : ops_)
		ops.push_back(*(op.data));

	int32_t code = zoo_multi(impl, ops.size(), ops.data(), out_results->data());

	if (code == ZOK)
	{
		if (out_results_)
			*out_results_ = out_results;
	}

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
	int32_t code = retry(boost::bind(&ZooKeeper::multiImpl, this, boost::ref(ops_), out_results_));

	if (code != ZOK &&
		code != ZNONODE &&
		code != ZNODEEXISTS &&
		code != ZNOCHILDRENFOREPHEMERALS &&
		code != ZBADVERSION &&
		code != ZNOTEMPTY)
			throw KeeperException(code);
	return code;
}

void ZooKeeper::removeChildrenRecursive(const std::string & path)
{
	Strings children = getChildren(path);
	zkutil::Ops ops;
	Strings strings;
	for (const std::string & child : children)
	{
		removeChildrenRecursive(path + "/" + child);
		strings.push_back(path + "/" + child);
		ops.push_back(new Op::Remove(strings.back(), -1));
	}
	multi(ops);
}

void ZooKeeper::removeRecursive(const std::string & path)
{
	removeChildrenRecursive(path);
	remove(path);
}

ZooKeeper::~ZooKeeper()
{
	int code = zookeeper_close(impl);
	if (code != ZOK)
	{
		LOG_ERROR(&Logger::get("~ZooKeeper"), "Failed to close ZooKeeper session: " << zerror(code));
	}

	/// удаляем WatchWithPromise которые уже никогда не будут обработаны
	for (WatchWithPromise * watch : watch_store)
		delete watch;
	watch_store.clear();
}

ZooKeeperPtr ZooKeeper::startNewSession() const
{
	return new ZooKeeper(hosts, sessionTimeoutMs, state_watch);
}

Op::Create::Create(const std::string & path_, const std::string & value_, AclPtr acl, int32_t flags)
: path(path_), value(value_), created_path(path.size() + ZooKeeper::SEQUENTIAL_SUFFIX_SIZE)
{
	zoo_create_op_init(data.get(), path.c_str(), value.c_str(), value.size(), acl, flags, created_path.data(), created_path.size());
}

AclPtr ZooKeeper::getDefaultACL()
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);
	return default_acl;
}

void ZooKeeper::setDefaultACL(AclPtr new_acl)
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);
	default_acl = new_acl;
}

std::string ZooKeeper::error2string(int32_t code)
{
	return zerror(code);
}

int ZooKeeper::state()
{
	return zoo_state(impl);
}

bool ZooKeeper::expired()
{
	return state() == ZOO_EXPIRED_SESSION_STATE;
}
}
