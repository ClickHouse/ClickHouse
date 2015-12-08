#include <zkutil/ZooKeeperHolder.h>

using namespace zkutil;

ZooKeeperHolder::UnstorableZookeeperHandler ZooKeeperHolder::getZooKeeper()
{
	std::unique_lock<std::mutex> lock(mutex);
	return UnstorableZookeeperHandler(ptr);
}

bool ZooKeeperHolder::replaceZooKeeperSessionToNewOne()
{
	std::unique_lock<std::mutex> lock(mutex);

	if (ptr.unique())
	{
		ptr = ptr->startNewSession();
		return true;
	}
		return false;
}

bool ZooKeeperHolder::isSessionExpired() const
{
	return ptr ? ptr->expired() : false;
}

ZooKeeperHolder::UnstorableZookeeperHandler::UnstorableZookeeperHandler(ZooKeeper::Ptr zk_ptr_)
: zk_ptr(zk_ptr_)
{
}