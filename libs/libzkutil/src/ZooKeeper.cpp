#include <zkutil/ZooKeeper.h>
#include <boost/make_shared.hpp>
#include <Yandex/logger_useful.h>


#define CHECKED(x, path) { ReturnCode::type code = x; if (code != ReturnCode::Ok) throw KeeperException(code, path); }
#define CHECKED_WITHOUT_PATH(x) { ReturnCode::type code = x; if (code != ReturnCode::Ok) throw KeeperException(code); }

namespace zkutil
{

typedef std::promise<WatchEventInfo> WatchPromise;

struct WatchWithPromise : public zk::Watch
{
	WatchPromise promise;
	bool notified;

	WatchWithPromise() : notified(false) {}

	void process(WatchEvent::type event, SessionState::type state, const std::string & path)
	{
		if (notified)
		{
			LOG_WARNING(&Logger::get("WatchWithPromise"), "Ignoring event " << WatchEvent::toString(event) << " with state "
				<< SessionState::toString(state) << " for path " << path);
			return;
		}
		promise.set_value(WatchEventInfo(event, state, path));
		notified = true;
	}
};

typedef boost::shared_ptr<zk::Watch> WatchPtr;
typedef boost::shared_ptr<WatchWithPromise> WatchWithPromisePtr;

static WatchPtr watchForFuture(WatchFuture * future)
{
	if (!future)
		return nullptr;
	WatchWithPromisePtr res = boost::make_shared<WatchWithPromise>();
	*future = res->promise.get_future();
	return res;
}

struct StateWatch : public zk::Watch
{
	ZooKeeper * owner;

	StateWatch(ZooKeeper * owner_) : owner(owner_) {}

	void process(WatchEvent::type event, SessionState::type state, const std::string & path)
	{
		owner->stateChanged(event, state, path);
	}
};

void ZooKeeper::init(const std::string & hosts, int32_t sessionTimeoutMs, WatchFunction * watch_)
{
	state_watch = watch_;

	CHECKED_WITHOUT_PATH(impl.init(hosts, sessionTimeoutMs, boost::make_shared<StateWatch>(this)));

	ACL perm;
	perm.getid().getscheme() = "world";
	perm.getid().getid() = "anyone";
	perm.setperms(zk::Permission::All);
	default_acl.push_back(perm);
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

void ZooKeeper::stateChanged(WatchEvent::type event, SessionState::type state, const std::string & path)
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	session_state = state;
	if (state_watch)
		(*state_watch)(event, state, path);
}

void ZooKeeper::checkNotExpired()
{
	if (disconnected())
		throw KeeperException(ReturnCode::SessionExpired);
}

bool ZooKeeper::disconnected()
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	return session_state == SessionState::Expired || session_state == SessionState::AuthFailed;
}

void ZooKeeper::setDefaultACL(ACLs & acl)
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	default_acl = acl;
}

ACLs ZooKeeper::getDefaultACL()
{
	Poco::ScopedLock<Poco::FastMutex> lock(mutex);

	return default_acl;
}

Strings ZooKeeper::getChildren(
	const std::string & path, Stat * stat, WatchFuture * watch)
{
	checkNotExpired();
	Stat s;
	Strings res;
	CHECKED(impl.getChildren(path, watchForFuture(watch), res, s), path);
	if (stat)
		*stat = s;
	return res;
}

bool ZooKeeper::tryGetChildren(const std::string & path, Strings & res,
								Stat * stat, WatchFuture * watch)
{
	checkNotExpired();
	Stat s;
	ReturnCode::type code = impl.getChildren(path, watchForFuture(watch), res, s);
	if (!(	code == ReturnCode::Ok ||
			code == ReturnCode::NoNode))
		throw KeeperException(code, path);
	if (code == ReturnCode::NoNode)
		return false;
	if (stat)
		*stat = s;
	return true;
}

std::string ZooKeeper::create(const std::string & path, const std::string & data, CreateMode::type mode)
{
	checkNotExpired();

	std::string res;
	CHECKED(impl.create(path, data, getDefaultACL(), mode, res), path);
	return res;
}

ReturnCode::type ZooKeeper::tryCreate(const std::string & path, const std::string & data, CreateMode::type mode, std::string & pathCreated)
{
	checkNotExpired();

	ReturnCode::type code = impl.create(path, data, getDefaultACL(), mode, pathCreated);
	if (!(	code == ReturnCode::Ok ||
			code == ReturnCode::NoNode ||
			code == ReturnCode::NodeExists ||
			code == ReturnCode::NoChildrenForEphemerals))
		throw KeeperException(code, path);
	return code;
}

void ZooKeeper::remove(const std::string & path, int32_t version)
{
	checkNotExpired();
	CHECKED(impl.remove(path, version), path);
}

ReturnCode::type ZooKeeper::tryRemove(const std::string & path, int32_t version)
{
	checkNotExpired();
	ReturnCode::type code = impl.remove(path, version);
	if (!(	code == ReturnCode::Ok ||
			code == ReturnCode::NoNode ||
			code == ReturnCode::BadVersion ||
			code == ReturnCode::NotEmpty))
		throw KeeperException(code, path);
	return code;
}

bool ZooKeeper::exists(const std::string & path, Stat * stat, WatchFuture * watch)
{
	checkNotExpired();
	Stat s;
	ReturnCode::type code = impl.exists(path, watchForFuture(watch), s);
	if (!(	code == ReturnCode::Ok ||
			code == ReturnCode::NoNode))
		throw KeeperException(code, path);
	if (code == ReturnCode::NoNode)
		return false;
	if (stat)
		*stat = s;
	return true;
}

std::string ZooKeeper::get(const std::string & path, Stat * stat, WatchFuture * watch)
{
	checkNotExpired();
	std::string res;
	Stat s;
	CHECKED(impl.get(path, watchForFuture(watch), res, s), path);
	if (stat)
		*stat = s;
	return res;
}

bool ZooKeeper::tryGet(const std::string & path, std::string & res, Stat * stat, WatchFuture * watch)
{
	checkNotExpired();
	Stat s;
	ReturnCode::type code = impl.get(path, watchForFuture(watch), res, s);
	if (!(	code == ReturnCode::Ok ||
			code == ReturnCode::NoNode))
		throw KeeperException(code, path);
	if (code == ReturnCode::NoNode)
		return false;
	if (stat)
		*stat = s;
	return true;
}

void ZooKeeper::set(const std::string & path, const std::string & data, int32_t version, Stat * stat)
{
	checkNotExpired();
	Stat s;
	CHECKED(impl.set(path, data, version, s), path);
	if (stat)
		*stat = s;
}

ReturnCode::type ZooKeeper::trySet(const std::string & path, const std::string & data,
									int32_t version, Stat * stat)
{
	checkNotExpired();
	Stat s;
	ReturnCode::type code = impl.set(path, data, version, s);
	if (!(	code == ReturnCode::Ok ||
			code == ReturnCode::NoNode ||
			code == ReturnCode::BadVersion))
		throw KeeperException(code, path);
	if (stat)
		*stat = s;
	return code;
}

OpResultsPtr ZooKeeper::multi(const Ops & ops)
{
	checkNotExpired();
	OpResultsPtr res = std::make_shared<OpResults>();
	CHECKED_WITHOUT_PATH(impl.multi(ops, *res));
	for (size_t i = 0; i < res->size(); ++i)
	{
		if ((*res)[i].getReturnCode() != ReturnCode::Ok)
			throw KeeperException((*res)[i].getReturnCode());
	}
	return res;
}

ReturnCode::type ZooKeeper::tryMulti(const Ops & ops, OpResultsPtr * out_results)
{
	checkNotExpired();
	OpResultsPtr results = std::make_shared<OpResults>();
	ReturnCode::type code = impl.multi(ops, *results);
	if (out_results)
		*out_results = results;
	if (code != ReturnCode::Ok &&
		code != ReturnCode::NoNode &&
		code != ReturnCode::NodeExists &&
		code != ReturnCode::NoChildrenForEphemerals &&
		code != ReturnCode::BadVersion &&
		code != ReturnCode::NotEmpty)
			throw KeeperException(code);
	return code;
}

void ZooKeeper::removeRecursive(const std::string & path)
{
	Strings children = getChildren(path);
	for (const std::string & child : children)
		removeRecursive(path + "/" + child);
	remove(path);
}

void ZooKeeper::close()
{
	CHECKED_WITHOUT_PATH(impl.close());
}

ZooKeeper::~ZooKeeper()
{
	try
	{
		close();
	}
	catch(...)
	{
		LOG_ERROR(&Logger::get("~ZooKeeper"), "Failed to close ZooKeeper session");
	}
}

}
