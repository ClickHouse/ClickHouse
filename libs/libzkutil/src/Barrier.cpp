#include <zkutil/Barrier.h>
#include <DB/Common/getFQDNOrHostName.h>
#include <DB/Common/Exception.h>
#include <Poco/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int BARRIER_TIMEOUT;

}

}

namespace zkutil
{

namespace
{

constexpr long wait_duration = 1000;

}

Barrier::Barrier(ZooKeeperPtr zookeeper_, const std::string & path_, size_t counter_)
	: zookeeper(zookeeper_), path(path_), counter(counter_)
{
	int32_t code = zookeeper->tryCreate(path, "", CreateMode::Persistent);
	if ((code != ZOK) && (code != ZNODEEXISTS))
		throw KeeperException(code);
}

void Barrier::setCancellationHook(CancellationHook cancellation_hook_)
{
	cancellation_hook = cancellation_hook_;
}

void Barrier::enter(uint64_t timeout)
{
	__sync_synchronize();

	auto key = zookeeper->create(path + "/" + getFQDNOrHostName(), "", zkutil::CreateMode::Ephemeral);
	key = key.substr(path.length() + 1);

	Poco::Stopwatch watch;

	if (timeout > 0)
		watch.start();

	while (true)
	{
		auto children = zookeeper->getChildren(path, nullptr, event);

		std::sort(children.begin(), children.end());
		auto it = std::lower_bound(children.cbegin(), children.cend(), key);

		/// This should never happen.
		if ((it == children.cend()) || (*it != key))
			throw DB::Exception("Barrier: corrupted queue. Own request not found.",
				DB::ErrorCodes::LOGICAL_ERROR);

		if (children.size() == counter)
			break;

		do
		{
			if (static_cast<uint32_t>(watch.elapsedSeconds()) > timeout)
				throw DB::Exception("Barrier: timeout", DB::ErrorCodes::BARRIER_TIMEOUT);

			abortIfRequested();
		}
		while (!event->tryWait(wait_duration));
	}
}

void Barrier::leave(uint64_t timeout)
{
	__sync_synchronize();

	zookeeper->remove(path + "/" + getFQDNOrHostName());

	Poco::Stopwatch watch;

	if (timeout > 0)
		watch.start();

	while (true)
	{
		auto children = zookeeper->getChildren(path, nullptr, event);

		if (children.empty())
			break;

		do
		{
			if (static_cast<uint32_t>(watch.elapsedSeconds()) > timeout)
				throw DB::Exception("Barrier: timeout", DB::ErrorCodes::BARRIER_TIMEOUT);

			abortIfRequested();
		}
		while (!event->tryWait(wait_duration));
	}
}

void Barrier::abortIfRequested()
{
	if (cancellation_hook)
	{
		try
		{
			cancellation_hook();
		}
		catch (...)
		{
			try
			{
				event->reset();
				zookeeper->tryRemove(path + "/" + getFQDNOrHostName());
			}
			catch (...)
			{
			}
			throw;
		}
	}
}

}
