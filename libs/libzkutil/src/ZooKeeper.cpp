#include <zkutil/ZooKeeper.h>
#include <zkutil/KeeperException.h>
#include <boost/make_shared.hpp>


#define CHECKED(x) { ReturnCode::type code = x; if (code) throw KeeperException(code); }

namespace zkutil
{

typedef std::promise<WatchEventInfo> WatchPromise;

struct WatchWithPromise : public zk::Watch
{
	WatchPromise promise;

	void process(WatchEvent::type event, SessionState::type state, const std::string & path)
	{
		promise.set_value(WatchEventInfo(event, state, path));
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

ZooKeeper::ZooKeeper(const std::string & hosts, int32_t sessionTimeoutMs, WatchFunction * watch_)
	: state_watch(watch_)
{
	CHECKED(impl.init(hosts, sessionTimeoutMs, boost::make_shared<StateWatch>(this)));

	ACL perm;
	perm.getid().getscheme() = "world";
	perm.getid().getid() = "anyone";
	perm.setperms(zk::Permission::All);
	default_acl.push_back(perm);
}

void ZooKeeper::stateChanged(WatchEvent::type event, SessionState::type state, const std::string & path)
{
	session_state = state;
	if (state_watch)
		(*state_watch)(event, state, path);
}

bool ZooKeeper::disconnected()
{
	return session_state == SessionState::Expired || session_state == SessionState::AuthFailed;
}

void ZooKeeper::setDefaultACL(ACLs & acl)
{
	default_acl = acl;
}

std::vector<std::string> ZooKeeper::getChildren(
	const std::string & path, Stat * stat, WatchFuture * watch)
{
	Stat s;
	Strings res;
	CHECKED(impl.getChildren(path, watchForFuture(watch), res, s));
	if (stat)
		*stat = s;
	return res;
}

}

