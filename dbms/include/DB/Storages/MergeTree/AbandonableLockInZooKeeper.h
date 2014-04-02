#pragma once

#include <zkutil/ZooKeeper.h>
#include <DB/Core/Exception.h>


namespace DB
{

/** Примитив синхронизации. Работает следующим образом:
  * При создании создает неэфемерную инкрементную ноду и помечает ее как заблокированную (LOCKED).
  * unlock() разблокирует ее (UNLOCKED).
  * При вызове деструктора или завершении сессии в ZooKeeper, переходит в состояние ABANDONED.
  *  (В том числе при падении программы)
  */
class AbandonableLockInZooKeeper
{
public:
	enum State
	{
		UNLOCKED,
		LOCKED,
		ABANDONED,
	};

	AbandonableLockInZookeeper(
		const String & path_prefix_, const String & temp_path, zkutil::ZooKeeper & zookeeper_)
		: zookeeper(zookeeper_), path_prefix(path_prefix_)
	{
		/// Создадим вспомогательную эфемерную ноду.
		holder_path = zookeeper.create(temp_path + "/abandonable-lock-", "", zkutil::CreateMode::EphemeralSequential);

		/// Запишем в основную ноду путь к вспомогательной.
		path = zookeeper.create(path_prefix, holder_path, zkutil::CreateMode::PersistentSequential);
	}

	String getPath()
	{
		return path;
	}

	/// Распарсить число в конце пути.
	UInt64 getNumber()
	{
		return static_cast<UInt64>(atol(path.substr(path_prefix.size())));
	}

	void unlock()
	{
		zookeeper.remove(path);
		zookeeper.remove(holder_path);
	}

	~AbandonableLockInZookeeper()
	{
		try
		{
			zookeeper.remove(holder_path);
			zookeeper.set(path, ""); /// Это не обязательно.
		}
		catch (...)
		{
			tryLogCurrentException();
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
		if (zookeeper.trySet(path, "") == zkutil::ReturnCode::Ok)
			return ABANDONED;

		return UNLOCKED;
	}

private:
	zkutil::ZooKeeper & zookeeper;
	String path_prefix;
	String path;
	String holder_path;
};

}
