#include <zkutil/Lock.h>

using namespace zkutil;

bool Lock::tryLock()
{
	auto zookeeper = zookeeper_holder->getZooKeeper();
	if (locked)
	{
		/// проверим, что нода создана и я ее владелец
		if (tryCheck() != Status::LOCKED_BY_ME)
			locked.reset(nullptr);
	}

	if (!locked)
	{
		size_t attempt;
		std::string dummy;
		int32_t code = zookeeper->tryCreateWithRetries(lock_path, lock_message, zkutil::CreateMode::Ephemeral, dummy, &attempt);

		if (code == ZNODEEXISTS)
		{
			if (attempt == 0)
				locked.reset(nullptr);
			else
			{
				zkutil::Stat stat;
				zookeeper->get(lock_path, &stat);
				if (stat.ephemeralOwner == zookeeper->getClientID())
					locked.reset(new ZooKeeperHandler(zookeeper));
				else
					locked.reset(nullptr);
			}
		}
		else if (code == ZOK)
		{
			locked.reset(new ZooKeeperHandler(zookeeper));
		}
		else
		{
			throw zkutil::KeeperException(code);
		}
	}
	return bool(locked);
}

void Lock::unlock()
{
	auto zookeeper = zookeeper_holder->getZooKeeper();

	if (locked)
	{
		try
		{
			if (tryCheck() == Status::LOCKED_BY_ME)
			{
				size_t attempt;
				int32_t code = zookeeper->tryRemoveEphemeralNodeWithRetries(lock_path, -1, &attempt);
				if (attempt)
				{
					if (code != ZOK)
						throw zkutil::KeeperException(code);
				}
				else
				{
					if (code == ZNONODE)
						LOG_ERROR(log, "Node " << lock_path << " has been already removed. Probably due to network error.");
					else if (code != ZOK)
						throw zkutil::KeeperException(code);
				}
			}
		}
		catch (const zkutil::KeeperException & e)
		{
			/// если сессия находится в невостанавливаемом состоянии, то эфемерные ноды нам больше не принадлежат
			/// и лок через таймаут будет отпущен
			if (!e.isUnrecoverable())
				throw;
		}
		locked.reset(nullptr);
	}
}

Lock::Status Lock::tryCheck() const
{
	auto zookeeper = zookeeper_holder->getZooKeeper();

	Status lock_status;
	Stat stat;
	std::string dummy;
	bool result = zookeeper->tryGet(lock_path, dummy, &stat);
	if (!result)
		lock_status = UNLOCKED;
	else
	{
		if (stat.ephemeralOwner == zookeeper->getClientID())
		{
			lock_status = LOCKED_BY_ME;
		}
		else
		{
			lock_status = LOCKED_BY_OTHER;
		}
	}

	if (locked && lock_status != LOCKED_BY_ME)
		LOG_WARNING(log, "Lock is lost. It is normal if session was reinitialized. Path: " << lock_path << "/" << lock_message);

	return lock_status;
}

std::string Lock::status2String(Status status)
{
	if (status >= END)
		throw zkutil::KeeperException("Wrong status code: " + std::to_string(status));
	static const char * names[] = {"Unlocked", "Locked by me", "Locked by other"};
	return names[status];
}

