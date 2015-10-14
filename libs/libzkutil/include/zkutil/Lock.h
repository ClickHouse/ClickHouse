#pragma once

#include <zkutil/ZooKeeper.h>
#include <common/logger_useful.h>
#include <DB/Common/Exception.h>

namespace zkutil
{
	class Lock
	{
	public:
		/// lock_prefix - относительный путь до блокировки в ZK. Начинается со слеша
		/// lock_name - имя ноды блокировки в ZK
		Lock(zkutil::ZooKeeperPtr zk, const std::string & lock_prefix_, const std::string & lock_name_, const std::string & lock_message_ = "",
			 bool create_parent_path = false) :
			zookeeper(zk), lock_path(lock_prefix_ + "/" + lock_name_), lock_message(lock_message_), log(&Logger::get("zkutil::Lock"))
		{
			if (create_parent_path)
				zookeeper->createAncestors(lock_prefix_);
			
			zookeeper->createIfNotExists(lock_prefix_, "");
		}

		Lock(const Lock &) = delete;
		Lock & operator=(const Lock &) = delete;

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
			LOCKED_BY_ME,
			LOCKED_BY_OTHER,
			END
		};
		std::string status2String(Status status);

		/// проверяет создана ли эфемерная нода и кто ее владелец.
		/// если мы сами создавали эфемерную ноду, то надо вызывать этот метод, чтобы убедится,
		/// что сессия с зукипером не порвалось
		Status check();

		void unlock();

		bool tryLock();

		/// путь к ноде блокировки в zookeeper
		const std::string & getPath() { return lock_path; }

	private:
		Status checkImpl();
		zkutil::ZooKeeperPtr zookeeper;
		std::string lock_path;
		std::string lock_message;
		Logger * log;
		bool locked = false;
	};
}
