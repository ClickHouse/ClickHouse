#pragma once
#include <zookeeper/zookeeper.hh>
#include <Yandex/Common.h>
#include <boost/function.hpp>
#include <future>


namespace zkutil
{

namespace zk = org::apache::zookeeper;
namespace CreateMode = zk::CreateMode;
namespace ReturnCode = zk::ReturnCode;
namespace SessionState = zk::SessionState;
namespace WatchEvent = zk::WatchEvent;

typedef zk::data::Stat Stat;
typedef zk::data::ACL ACL;
typedef zk::Op Op;
typedef zk::OpResult OpResult;

typedef std::vector<ACL> ACLs;
typedef boost::ptr_vector<Op> Ops;
typedef boost::ptr_vector<OpResult> OpResults;
typedef std::shared_ptr<boost::ptr_vector<OpResult> > OpResultsPtr;
typedef std::vector<std::string> Strings;

typedef boost::function<void (WatchEvent::type event, SessionState::type state,
                         const std::string & path)> WatchFunction;

struct WatchEventInfo
{
	WatchEvent::type event;
	SessionState::type state;
	std::string path;

	WatchEventInfo() {}
	WatchEventInfo(WatchEvent::type event_, SessionState::type state_, const std::string & path_)
		: event(event_), state(state_), path(path_) {}
};

typedef std::future<WatchEventInfo> WatchFuture;

}
