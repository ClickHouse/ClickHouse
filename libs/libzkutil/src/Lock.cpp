#include <zkutil/Lock.h>

using namespace zkutil;

bool Lock::tryLock()
{
	if (locked)
	{
		/// проверим, что нода создана и я ее владелец
		check();
	}
	else
	{
		size_t attempt;
		std::string dummy;
		int32_t code = zookeeper->tryCreateWithRetries(lock_path, lock_message, zkutil::CreateMode::Ephemeral, dummy, &attempt);

		if (code == ZNODEEXISTS)
		{
			if (attempt == 0)
				locked = false;
			else
			{
				zkutil::Stat stat;
				zookeeper->get(lock_path, &stat);
				if (stat.ephemeralOwner == zookeeper->getClientID())
					locked = true;
				else
					locked = false;
			}
		}
		else if (code == ZOK)
		{
			locked = true;
		}
		else
		{
			throw zkutil::KeeperException(code);
		}
	}
	return locked;
}

void Lock::unlock()
{
	if (locked)
	{
		/// проверим, что до сих пор мы владельцы ноды
		check();

		size_t attempt;
		int32_t code = zookeeper->tryRemoveWithRetries(lock_path, -1, &attempt);
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
		locked = false;
	}
}

Lock::Status Lock::check()
{
	Status status = checkImpl();
	if ((locked && status != LOCKED_BY_ME) || (!locked && (status != UNLOCKED && status != LOCKED_BY_OTHER)))
		throw zkutil::KeeperException(std::string("Incompability of local state and state in zookeeper. Local is ") + (locked ? "locked" : "unlocked") + ". Zookeeper state is " + status2String(status));
	return status;
}

Lock::Status Lock::checkImpl()
{
	Stat stat;
	std::string dummy;
	bool result = zookeeper->tryGet(lock_path, dummy, &stat);
	if (!result)
		return UNLOCKED;
	else
	{
		if (stat.ephemeralOwner == zookeeper->getClientID())
		{
			return LOCKED_BY_ME;
		}
		else
		{
			return LOCKED_BY_OTHER;
		}
	}
}

std::string Lock::status2String(Status status)
{
	if (status >= END)
		throw zkutil::KeeperException("Wrong status code: " + std::to_string(status));
	static const char * names[] = {"Unlocked", "Locked by me", "Locked by other"};
	return names[status];
}
