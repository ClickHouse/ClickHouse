#include <functional>
#include <zkutil/ZooKeeper.h>
#include <boost/make_shared.hpp>
#include <Yandex/logger_useful.h>
#include <DB/Common/ProfileEvents.h>


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
		if (event)
		{
			event->set();
			event = nullptr;
		}
	}
};

void ZooKeeper::processEvent(zhandle_t * zh, int type, int state, const char * path, void * watcherCtx)
{
	if (watcherCtx)
	{
		WatchWithEvent * watch = static_cast<WatchWithEvent *>(watcherCtx);
		watch->process(zh, type, state, path);

		/// Гарантируется, что не-ZOO_SESSION_EVENT событие придет ровно один раз (https://issues.apache.org/jira/browse/ZOOKEEPER-890).
		if (type != ZOO_SESSION_EVENT)
		{
			{
				Poco::ScopedLock<Poco::FastMutex> lock(watch->zk.mutex);
				watch->zk.watch_store.erase(watch);
			}
			delete watch;
		}
	}
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
		std::string node_key = "node";

		std::vector<std::string> hosts_strings;

		session_timeout_ms = DEFAULT_SESSION_TIMEOUT;
		for (const auto & key : keys)
		{
			if (key == node_key || key.compare(0, node_key.size(), node_key) == 0)
			{
				hosts_strings.push_back(
					config.getString(config_name + "." + key + ".host") + ":" + config.getString(config_name + "." + key + ".port"));
			}
			else if (key == "session_timeout_ms")
			{
				session_timeout_ms = config.getInt(config_name + "." + key);
			}
			else throw KeeperException(std::string("Unknown key ") + key + " in config file");
		}

		/// перемешиваем порядок хостов, чтобы сделать нагрузку на zookeeper более равномерной
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

ZooKeeper::ZooKeeper(const Poco::Util::AbstractConfiguration& config, const std::string& config_name, int32_t session_timeout_ms_)
{
	ZooKeeperArgs args(config, config_name);
	init(args.hosts, session_timeout_ms_);
}


void * ZooKeeper::watchForEvent(EventPtr event)
{
	if (event)
	{
		WatchWithEvent * res = new WatchWithEvent(*this, event);
		{
			Poco::ScopedLock<Poco::FastMutex> lock(mutex);
			watch_store.insert(res);
			if (watch_store.size() % 10000 == 0)
			{
				LOG_ERROR(log, "There are " << watch_store.size() << " active watches. There must be a leak somewhere.");
			}
		}
		return res;
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
	int32_t code = retry(std::bind(&ZooKeeper::getChildrenImpl, this, std::ref(path), std::ref(res), stat_, watch));

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

	code = zoo_create(impl, path.c_str(), data.c_str(), data.size(), getDefaultACL(), mode,  name_buffer, name_buffer_size);
	ProfileEvents::increment(ProfileEvents::ZooKeeperCreate);
	ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

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
			code == ZNOCHILDRENFOREPHEMERALS))
		throw KeeperException(code, path);

	return code;
}

int32_t ZooKeeper::tryCreate(const std::string & path, const std::string & data, int32_t mode)
{
	std::string pathCreated;
	return tryCreate(path, data, mode, pathCreated);
}

int32_t ZooKeeper::tryCreateWithRetries(const std::string & path, const std::string & data, int32_t mode, std::string & pathCreated, size_t* attempt)
{
	return retry([&path, &data, mode, &pathCreated, this] { return tryCreate(path, data, mode, pathCreated); }, attempt);
}


void ZooKeeper::createIfNotExists(const std::string & path, const std::string & data)
{
	std::string pathCreated;
	int32_t code = retry(std::bind(&ZooKeeper::createImpl, this, std::ref(path), std::ref(data), zkutil::CreateMode::Persistent, std::ref(pathCreated)));

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

int32_t ZooKeeper::tryRemove(const std::string & path, int32_t version)
{
	int32_t code = removeImpl(path, version);
	if (!(	code == ZOK ||
			code == ZNONODE ||
			code == ZBADVERSION ||
			code == ZNOTEMPTY))
		throw KeeperException(code, path);
	return code;
}

int32_t ZooKeeper::tryRemoveWithRetries(const std::string & path, int32_t version, size_t * attempt)
{
	int32_t code = retry(std::bind(&ZooKeeper::removeImpl, this, std::ref(path), version), attempt);
	if (!(	code == ZOK ||
			code == ZNONODE ||
			code == ZBADVERSION ||
			code == ZNOTEMPTY))
		throw KeeperException(code, path);
	return code;
}

int32_t ZooKeeper::existsImpl(const std::string & path, Stat * stat_, EventPtr watch)
{
	int32_t code;
	Stat stat;
	code = zoo_wexists(impl, path.c_str(), callbackForEvent(watch), watchForEvent(watch), &stat);
	ProfileEvents::increment(ProfileEvents::ZooKeeperExists);
	ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

	if (code == ZOK)
	{
		if (stat_)
			*stat_ = stat;
	}

	return code;
}

bool ZooKeeper::exists(const std::string & path, Stat * stat_, EventPtr watch)
{
	int32_t code = retry(std::bind(&ZooKeeper::existsImpl, this, path, stat_, watch));

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
	ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
	ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);

	if (code == ZOK)
	{
		if (stat_)
			*stat_ = stat;

		if (buffer_len < 0)		/// Такое бывает, если в ноде в ZK лежит NULL. Не будем отличать его от пустой строки.
			res.clear();
		else
			res.assign(buffer, buffer_len);
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
	int32_t code = retry(std::bind(&ZooKeeper::getImpl, this, std::ref(path), std::ref(res), stat_, watch));

	if (!(	code == ZOK ||
			code == ZNONODE))
		throw KeeperException(code, path);

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

int32_t ZooKeeper::trySet(const std::string & path, const std::string & data,
									int32_t version, Stat * stat_)
{
	int32_t code = setImpl(path, data, version, stat_);

	if (!(	code == ZOK ||
			code == ZNONODE ||
			code == ZBADVERSION))
		throw KeeperException(code, path);
	return code;
}

int32_t ZooKeeper::multiImpl(const Ops & ops_, OpResultsPtr * out_results_)
{
	if (ops_.empty())
		return ZOK;

	/// Workaround ошибки в сишном клиенте ZooKeeper. Если сессия истекла, zoo_multi иногда падает с segfault.
	/// Наверно, здесь есть race condition, и возможен segfault, если сессия истечет между этой проверкой и zoo_multi.
	/// TODO: Посмотреть, не исправлено ли это в последней версии клиента, и исправить.
	if (expired())
		return ZINVALIDSTATE;

	size_t count = ops_.size();
	OpResultsPtr out_results(new OpResults(count));

	/// копируем структуру, содержащую указатели, дефолтным конструктором копирования
	/// это безопасно, т.к. у нее нет деструктора
	std::vector<zoo_op_t> ops;
	for (const Op & op : ops_)
		ops.push_back(*(op.data));

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
			ops.push_back(new Op::Remove(path + "/" + children.back(), -1));
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
			ops.push_back(new Op::Remove(batch.back(), -1));
		}

		/** Сначала пытаемся удалить детей более быстрым способом - сразу пачкой. Если не получилось,
		  *  значит кто-то кроме нас удаляет этих детей, и придется удалять их по одному.
		  */
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

ZooKeeper::~ZooKeeper()
{
	LOG_INFO(&Logger::get("~ZooKeeper"), "Closing ZooKeeper session");

	int code = zookeeper_close(impl);
	if (code != ZOK)
	{
		LOG_ERROR(&Logger::get("~ZooKeeper"), "Failed to close ZooKeeper session: " << zerror(code));
	}

	LOG_INFO(&Logger::get("~ZooKeeper"), "Removing " << watch_store.size() << " watches");

	/// удаляем WatchWithEvent которые уже никогда не будут обработаны
	for (WatchWithEvent * watch : watch_store)
		delete watch;

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
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);
	return default_acl;
}

void ZooKeeper::setDefaultACL(ACLPtr new_acl)
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

int64_t ZooKeeper::getClientID()
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

			return ValueAndStat{ {value, size_t(value_len)}, *stat };
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

			return ValueAndStatAndExists{ {value, size_t(value_len)}, *stat, rc != ZNONODE };
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

			return StatAndExists{ *stat, rc != ZNONODE };
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

}
