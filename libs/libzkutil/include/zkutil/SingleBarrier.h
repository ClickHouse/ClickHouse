#pragma once

#include <zkutil/Common.h>
#include <string>
#include <functional>

namespace zkutil
{

/// Implementation of a distributed single barrier for ZooKeeper.
/// Consider a barrier with N slots. A node that wants to enter this barrier
/// creates a uniquely identified token and puts it in a free slot, then waits
/// until all of the N slots have been taken.
/// This implementation allows entering a barrier more than once. This is done
/// by prefixing tokens with a tag which is an integer number representing the
/// number of times this barrier has already been crossed. When a node attempts
/// to puts its token into a slot, first it looks for lingering obsolete tokens,
/// then, if found, deletes them. If a node has put its token into the last free
/// slot, it increments the tag value.
class SingleBarrier final
{
public:
	using CancellationHook = std::function<void()>;

public:
	SingleBarrier(GetZooKeeper get_zookeeper_, const std::string & path_, size_t counter_);

	SingleBarrier(const SingleBarrier &) = delete;
	SingleBarrier & operator=(const SingleBarrier &) = delete;

	SingleBarrier(SingleBarrier &&) = default;
	SingleBarrier & operator=(SingleBarrier &&) = default;

	/// Register a function that checks whether barrier operation should be cancelled.
	void setCancellationHook(CancellationHook cancellation_hook_);

	void enter(uint64_t timeout = 0);

private:
	void abortIfRequested();

private:
	GetZooKeeper get_zookeeper;
	EventPtr event = std::make_shared<Poco::Event>();
	CancellationHook cancellation_hook;
	std::string path;
	std::string token;
	size_t counter;
};

}
