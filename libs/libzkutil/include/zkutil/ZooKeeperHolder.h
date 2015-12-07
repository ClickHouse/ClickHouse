#pragma once

#include <zkutil/ZooKeeper.h>
#include <mutex>

namespace zkutil
{

class ZooKeeperHolder
{
protected:
	class UnstorableZookeeperHandler;

public:
	/// вызывать из одного потока - не thread safe
	template <class... Args>
	static void create(Args&&... args);

	static ZooKeeperHolder & getInstance();

	static UnstorableZookeeperHandler getZooKeeper();
	static bool replaceZooKeeperSessionToNewOne();

	static bool isSessionExpired();

protected:
	/** Хендлер для подсчета количества используемых ссылок на ZooKeeper
	*
	*  Специально поддерживает только хранение на стеке.
	*  Что вынуждает перед каждым использованием явно запросить хэндлер у ZooKeeperHolder
	*/
	class UnstorableZookeeperHandler
	{
	public:
		UnstorableZookeeperHandler(ZooKeeper::Ptr zk_ptr_);

		ZooKeeper * operator->() { return zk_ptr.get(); }
		const ZooKeeper * operator->() const { return zk_ptr.get(); }
		ZooKeeper & operator*() { return *zk_ptr; }
		const ZooKeeper & operator*() const { return *zk_ptr; }

	private:
		ZooKeeper::Ptr zk_ptr;
	};

protected:
	template <class... Args>
	ZooKeeperHolder(Args&&... args);

private:
	static std::unique_ptr<ZooKeeperHolder> instance;

	mutable std::mutex mutex;
	ZooKeeper::Ptr ptr;
};

template <class... Args>
ZooKeeperHolder::ZooKeeperHolder(Args&&... args)
	: ptr(std::make_shared<ZooKeeper>(std::forward<Args>(args)...))
{
}

template <class... Args>
void ZooKeeperHolder::create(Args&&... args)
{
	if (instance)
		throw DB::Exception("Initialization is called twice");
	instance.reset(new ZooKeeperHolder(std::forward<Args>(args)...));
}

}; /*namespace zkutil*/