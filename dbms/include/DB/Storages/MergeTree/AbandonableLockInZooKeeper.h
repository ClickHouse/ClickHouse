#pragma once

#include <zkutil/ZooKeeper.h>
#include <DB/Common/Exception.h>
#include <DB/IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}

/** Примитив синхронизации. Работает следующим образом:
  * При создании создает неэфемерную инкрементную ноду и помечает ее как заблокированную (LOCKED).
  * unlock() разблокирует ее (UNLOCKED).
  * При вызове деструктора или завершении сессии в ZooKeeper, переходит в состояние ABANDONED.
  *  (В том числе при падении программы).
  */
class AbandonableLockInZooKeeper : private boost::noncopyable
{
public:
	enum State
	{
		UNLOCKED,
		LOCKED,
		ABANDONED,
	};

	AbandonableLockInZooKeeper(
		const String & path_prefix_, const String & temp_path, zkutil::ZooKeeper & zookeeper_)
		: zookeeper(zookeeper_), path_prefix(path_prefix_)
	{
		/// Создадим вспомогательную эфемерную ноду.
		holder_path = zookeeper.create(temp_path + "/abandonable_lock-", "", zkutil::CreateMode::EphemeralSequential);

		/// Запишем в основную ноду путь к вспомогательной.
		path = zookeeper.create(path_prefix, holder_path, zkutil::CreateMode::PersistentSequential);

		if (path.size() <= path_prefix.size())
			throw Exception("Logical error: name of sequential node is shorter than prefix.", ErrorCodes::LOGICAL_ERROR);
	}

	AbandonableLockInZooKeeper(AbandonableLockInZooKeeper && rhs)
		: zookeeper(rhs.zookeeper)
	{
		std::swap(path_prefix, rhs.path_prefix);
		std::swap(path, rhs.path);
		std::swap(holder_path, rhs.holder_path);
	}

	String getPath() const
	{
		return path;
	}

	/// Распарсить число в конце пути.
	UInt64 getNumber() const
	{
		return parse<UInt64>(path.c_str() + path_prefix.size(), path.size() - path_prefix.size());
	}

	void unlock()
	{
		zookeeper.remove(path);
		zookeeper.remove(holder_path);
		holder_path = "";
	}

	/// Добавляет в список действия, эквивалентные unlock().
	void getUnlockOps(zkutil::Ops & ops)
	{
		ops.push_back(new zkutil::Op::Remove(path, -1));
		ops.push_back(new zkutil::Op::Remove(holder_path, -1));
	}

	~AbandonableLockInZooKeeper()
	{
		if (holder_path.empty())
			return;

		try
		{
			zookeeper.tryRemoveEphemeralNodeWithRetries(holder_path);
			zookeeper.trySet(path, ""); /// Это не обязательно.
		}
		catch (...)
		{
			tryLogCurrentException("~AbandonableLockInZooKeeper");
		}
	}

	static State check(const String & path, zkutil::ZooKeeper & zookeeper)
	{
		String holder_path;

		/// Если нет основной ноды, UNLOCKED.
		if (!zookeeper.tryGet(path, holder_path))
			return UNLOCKED;

		/// Если в основной ноде нет пути к вспомогательной, ABANDONED.
		if (holder_path.empty())
			return ABANDONED;

		/// Если вспомогательная нода жива, LOCKED.
		if (zookeeper.exists(holder_path))
			return LOCKED;

		/// Если вспомогательной ноды нет, нужно еще раз проверить существование основной ноды,
		///  потому что за это время могли успеть вызвать unlock().
		/// Заодно уберем оттуда путь к вспомогательной ноде.
		if (zookeeper.trySet(path, "") == ZOK)
			return ABANDONED;

		return UNLOCKED;
	}

	static void createAbandonedIfNotExists(const String & path, zkutil::ZooKeeper & zookeeper)
	{
		zookeeper.createIfNotExists(path, "");
	}

private:
	zkutil::ZooKeeper & zookeeper;
	String path_prefix;
	String path;
	String holder_path;
};

}
