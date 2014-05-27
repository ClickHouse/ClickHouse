#pragma once

#include <zkutil/ZooKeeper.h>
#include <Yandex/logger_useful.h>
#include <DB/Core/Exception.h>

namespace zkutil
{
	class Lock
	{
	public:
		/// lock_prefix - относительный путь до блокировки в ZK. Начинается со слеша
		/// lock_name - имя ноды блокировки в ZK
		Lock(zkutil::ZooKeeperPtr zk, const std::string & lock_prefix_, const std::string & lock_name_) :
			zookeeper(zk), lock_path(lock_prefix_ + "/" + lock_name_), log(&Logger::get("zkutil::Lock"))
		{
			if (zookeeper->exists(lock_prefix_))
				zookeeper->create(lock_prefix_, "", zkutil::CreateMode::Ephemeral);
		}

		~Lock()
		{
			try
			{
				unlock();
			}
			catch (const zkutil::KeeperException & e)
			{
				DB::tryLogCurrentException(__PRETTY_FUNCTION__);
			}
		}

		enum Status
		{
			UNLOCKED,
			LOCKED,
		};

		/// проверяет создана ли эфемерная нода.
		/// если мы сами создавали эфемерную ноду, то надо вызывать этот метод, чтобы убедится,
		/// что сессия с зукипером не порвалось
		Status check()
		{
			if (zookeeper->exists(lock_path))
				return LOCKED;
			else
				return UNLOCKED;
		}

		void unlock()
		{
			if (locked)
			{
				zookeeper->remove(lock_path);
				locked = false;
			}
		}

		bool tryLock()
		{
			zkutil::ReturnCode::type ret = zookeeper->tryCreate(lock_path, "", zkutil::CreateMode::Ephemeral);
			if (ret == zkutil::ReturnCode::Ok)
			{
				locked = true;
				return true;
			}
			else if (ret == zkutil::ReturnCode::NodeExists)
				return false;
			else
				throw zkutil::KeeperException(ret);
		}

		/// путь к ноде блокировки в zookeeper
		const std::string & getPath() { return lock_path; }

	private:
		zkutil::ZooKeeperPtr zookeeper;
		std::string lock_path;
		Logger * log;
		bool locked = false;
	};
}
