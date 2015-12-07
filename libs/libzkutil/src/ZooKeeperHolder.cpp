#include <zkutil/ZooKeeperHolder.h>

using namespace zkutil;

std::unique_ptr<ZooKeeperHolder> ZooKeeperHolder::instance;

ZooKeeperHolder & ZooKeeperHolder::getInstance()
{
	if (!instance)
		throw DB::Exception("ZooKeeperHolder should be initialized before getInstance is called.");
	return *instance;
}

ZooKeeperHolder::UnstorableZookeeperHandler ZooKeeperHolder::getZooKeeper()
{
	auto & inst = getInstance();
	std::unique_lock<std::mutex> lock(inst.mutex);
	return UnstorableZookeeperHandler(inst.ptr);
}

bool ZooKeeperHolder::replaceZooKeeperSessionToNewOne()
{
	auto & inst = getInstance();
	std::unique_lock<std::mutex> lock(inst.mutex);

	if (inst.ptr.unique())
	{
		inst.ptr = inst.ptr->startNewSession();
		return true;
	}
		return false;
}

bool ZooKeeperHolder::isSessionExpired()
{
	return getZooKeeper()->expired();
}

ZooKeeperHolder::UnstorableZookeeperHandler::UnstorableZookeeperHandler(ZooKeeper::Ptr zk_ptr_)
: zk_ptr(zk_ptr_)
{
}