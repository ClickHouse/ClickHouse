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

struct WatchWithEvent
{
	/// существует все время существования WatchWithEvent
	ZooKeeper & zk;
	EventPtr event;

	WatchWithEvent(ZooKeeper & zk_, EventPtr event_) : zk(zk_), event(event_) {}

	void process(zhandle_t * zh, int32_t event_type, int32_t state, const char * path)
	{
		if (!event)
		{
			event->set();
			event = nullptr;
		}
	}
};

void ZooKeeper::processEvent(zhandle_t * zh, int type, int state, const char * path, void *watcherCtx)
{
	if (watcherCtx)
	{
		WatchWithEvent * watch = reinterpret_cast<WatchWithEvent *>(watcherCtx);
		watch->process(zh, type, state, path);

		/// Гарантируется, что не-ZOO_SESSION_EVENT событие придет ровно один раз (https://issues.apache.org/jira/browse/ZOOKEEPER-890).
		if (type != ZOO_SESSION_EVENT)
		{
			watch->zk.watch_store.erase(watch);
			delete watch;
		}
	}
}

void ZooKeeper::init(const std::string & hosts_, int32_t sessionTimeoutMs_, WatchFunction * watch_)
{
	log = &Logger::get("ZooKeeper");
	zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
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

void * ZooKeeper::watchForEvent(EventPtr event)
{
	if (event)
	{
		WatchWithEvent * res = new WatchWithEvent(*this, event);
		watch_store.insert(res);
		return reinterpret_cast<void *>(res);
	}
	else
	{
		return nullptr;
	}
}

watcher_fn ZooKeeper::callbackForEvent(EventPtr event)
{
	return event ? processEvent : nullptr;
}

int32_t ZooKeeper::getChildrenImpl(const std::string & path, Strings & res,
					Stat * stat_,
					EventPtr watch)
{
	String_vector strings;
	int code;
	Stat stat;
	code = zoo_wget_children2(impl, path.c_str(), callbackForEvent(watch), watchForEvent(watch), &strings, &stat);

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
	const std::string & path, Stat * stat, EventPtr watch)
{
	Strings res;
	check(tryGetChildren(path, res, stat, watch), path);
	return res;
}

int32_t ZooKeeper::tryGetChildren(const std::string & path, Strings & res,
								Stat * stat_, EventPtr watch)
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
	int code = createImpl(path, data, mode, pathCreated);

	if (!(	code == ZOK ||
			code == ZNONODE ||
			code == ZNODEEXISTS ||
			code == ZNOCHILDRENFOREPHEMERALS ||
			code == ZCONNECTIONLOSS ||
			code == ZOPERATIONTIMEOUT))
		throw KeeperException(code, path);

	return code;
}

int32_t ZooKeeper::tryCreate(const std::string & path, const std::string & data, int32_t mode)
{
	std::string pathCreated;
	return tryCreate(path, data, mode, pathCreated);
}

void ZooKeeper::createIfNotExists(const std::string & path, const std::string & data)
{
	int32_t code = retry(boost::bind(&ZooKeeper::tryCreate, this, boost::ref(path), boost::ref(data), zkutil::CreateMode::Persistent));

	if (code == ZOK || code == ZNODEEXISTS)
		return;
	else
		throw KeeperException(code, path);
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
	int32_t code = removeImpl(path, version);
	if (!(	code == ZOK ||
			code == ZNONODE ||
			code == ZBADVERSION ||
			code == ZNOTEMPTY ||
			code == ZCONNECTIONLOSS ||
			code == ZOPERATIONTIMEOUT))
		throw KeeperException(code, path);
	return code;
}

int32_t ZooKeeper::tryRemoveWithRetries(const std::string & path, int32_t version)
{
	return retry(boost::bind(&ZooKeeper::tryRemove, this, boost::ref(path), version));
}

int32_t ZooKeeper::existsImpl(const std::string & path, Stat * stat_, EventPtr watch)
{
	int32_t code;
	Stat stat;
	code = zoo_wexists(impl, path.c_str(), callbackForEvent(watch), watchForEvent(watch), &stat);

	if (code == ZOK)
	{
		if (stat_)
			*stat_ = stat;
	}

	return code;
}

bool ZooKeeper::exists(const std::string & path, Stat * stat_, EventPtr watch)
{
	int32_t code = retry(boost::bind(&ZooKeeper::existsImpl, this, path, stat_, watch));

	if (!(	code == ZOK ||
			code == ZNONODE))
		throw KeeperException(code, path);
	if (code == ZNONODE)
		return false;
	return true;
}

int32_t ZooKeeper::getImpl(const std::string & path, std::string & res, Stat * stat_, EventPtr watch)
{
	char buffer[MAX_NODE_SIZE];
	int buffer_len = MAX_NODE_SIZE;
	int32_t code;
	Stat stat;
	code = zoo_wget(impl, path.c_str(), callbackForEvent(watch), watchForEvent(watch), buffer, &buffer_len, &stat);

	if (code == ZOK)
	{
		if (stat_)
			*stat_ = stat;

		res = std::string(buffer, buffer_len);
	}
	return code;
}

std::string ZooKeeper::get(const std::string & path, Stat * stat, EventPtr watch)
{
	std::string res;
	if (tryGet(path, res, stat, watch))
		return res;
	else
		throw KeeperException("Can't get data for node " + path + ": node doesn't exist");
}

bool ZooKeeper::tryGet(const std::string & path, std::string & res, Stat * stat_, EventPtr watch)
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
	int32_t code = setImpl(path, data, version, stat_);

	if (!(	code == ZOK ||
			code == ZNONODE ||
			code == ZBADVERSION ||
			code == ZCONNECTIONLOSS ||
			code == ZOPERATIONTIMEOUT))
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
		code == ZNOTEMPTY ||
		code == ZCONNECTIONLOSS ||
		code == ZOPERATIONTIMEOUT))
			throw KeeperException(code);
	return code;
}

int32_t ZooKeeper::tryMultiWithRetries(const Ops & ops, OpResultsPtr * out_results)
{
	return retry(boost::bind(&ZooKeeper::tryMulti, this, boost::ref(ops), out_results));
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

void ZooKeeper::tryRemoveChildrenRecursive(const std::string & path)
{
	Strings children;
	if (tryGetChildren(path, children) != ZOK)
		return;
	zkutil::Ops ops;
	Strings strings;
	for (const std::string & child : children)
	{
		tryRemoveChildrenRecursive(path + "/" + child);
		strings.push_back(path + "/" + child);
		ops.push_back(new Op::Remove(strings.back(), -1));
	}

	/** Сначала пытаемся удалить детей более быстрым способом - всех сразу. Если не получилось,
	  *  значит кто-то кроме нас удаляет этих детей, и придется удалять их по одному.
	  */
	if (tryMulti(ops) != ZOK)
	{
		for (const std::string & child : children)
		{
			tryRemove(child);
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

ZooKeeper::~ZooKeeper()
{
	int code = zookeeper_close(impl);
	if (code != ZOK)
	{
		LOG_ERROR(&Logger::get("~ZooKeeper"), "Failed to close ZooKeeper session: " << zerror(code));
	}

	/// удаляем WatchWithEvent которые уже никогда не будут обработаны
	for (WatchWithEvent * watch : watch_store)
		delete watch;
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
