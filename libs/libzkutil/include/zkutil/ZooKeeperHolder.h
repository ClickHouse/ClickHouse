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
	ZooKeeperHolder() = default;

	/// вызывать из одного потока - не thread safe
	template <class... Args>
	void init(Args&&... args);

	UnstorableZookeeperHandler getZooKeeper();
	bool replaceZooKeeperSessionToNewOne();

	bool isSessionExpired() const;

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

		explicit operator bool() const { return bool(zk_ptr); }
		bool operator==(nullptr_t) const { return zk_ptr == nullptr; }
		bool operator!=(nullptr_t) const { return !(*this == nullptr); }

		ZooKeeper * operator->() { return zk_ptr.get(); }
		const ZooKeeper * operator->() const { return zk_ptr.get(); }
		ZooKeeper & operator*() { return *zk_ptr; }
		const ZooKeeper & operator*() const { return *zk_ptr; }

	private:
		ZooKeeper::Ptr zk_ptr;
	};

private:
	mutable std::mutex mutex;
	ZooKeeper::Ptr ptr;
};

template <class... Args>
void ZooKeeperHolder::init(Args&&... args)
{
	ptr = std::make_shared<ZooKeeper>(std::forward<Args>(args)...);
}

}; /*namespace zkutil*/