#pragma once

#include <zkutil/ZooKeeper.h>
#include <mutex>
#include <boost/noncopyable.hpp>

namespace zkutil
{

class ZooKeeperHolder : public boost::noncopyable
{
protected:
	class UnstorableZookeeperHandler;

public:
	ZooKeeperHolder() = default;

	/// вызывать из одного потока - не thread safe
	template <class... Args>
	void init(Args&&... args);
	/// был ли класс инициализирован
	bool isInitialized() const { return ptr != nullptr; }

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

		/// в случае nullptr методы разыменования кидают исключение,
		/// с более подробным текстом, чем просто nullptr
		ZooKeeper * operator->();
		const ZooKeeper * operator->() const;
		ZooKeeper & operator*();
		const ZooKeeper & operator*() const;

	private:
		ZooKeeper::Ptr zk_ptr;
	};

private:
	mutable std::mutex mutex;
	ZooKeeper::Ptr ptr;

	Logger * log = &Logger::get("ZooKeeperHolder");

	static std::string nullptr_exception_message;
};

template <class... Args>
void ZooKeeperHolder::init(Args&&... args)
{
	ptr = std::make_shared<ZooKeeper>(std::forward<Args>(args)...);
}

using ZooKeeperHolderPtr = std::shared_ptr<ZooKeeperHolder>;

}; /*namespace zkutil*/