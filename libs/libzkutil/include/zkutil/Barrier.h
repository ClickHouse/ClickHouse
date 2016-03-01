#pragma once

#include <zkutil/ZooKeeperHolder.h>
#include <string>
#include <functional>

namespace zkutil
{

/** Double distributed barrier for ZooKeeper.
  */
class Barrier final
{
public:
	using CancellationHook = std::function<void()>;

public:
	Barrier(ZooKeeperPtr zookeeper_, const std::string & path_, size_t counter_);

	Barrier(const Barrier &) = delete;
	Barrier & operator=(const Barrier &) = delete;

	Barrier(Barrier &&) = default;
	Barrier & operator=(Barrier &&) = default;

	/// Register a function that checks whether barrier operation should be cancelled.
	void setCancellationHook(CancellationHook cancellation_hook_);

	void enter(uint64_t timeout = 0);
	void leave(uint64_t timeout = 0);

private:
	void abortIfRequested();

private:
	ZooKeeperPtr zookeeper;
	EventPtr event = new Poco::Event;
	CancellationHook cancellation_hook;
	std::string path;
	size_t counter;
};

}
