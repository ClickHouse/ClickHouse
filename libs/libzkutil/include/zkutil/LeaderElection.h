#pragma once

#include <zkutil/ZooKeeper.h>
#include <functional>
#include <common/logger_useful.h>


namespace zkutil
{

/** Реализует метод выбора лидера, описанный здесь: http://zookeeper.apache.org/doc/r3.4.5/recipes.html#sc_leaderElection
  */
class LeaderElection
{
public:
	typedef std::function<void()> LeadershipHandler;

	/** handler вызывается, когда этот экземпляр становится лидером.
	  */
	LeaderElection(const std::string & path_, ZooKeeper & zookeeper_, LeadershipHandler handler_, const std::string & identifier_ = "")
		: path(path_), zookeeper(zookeeper_), handler(handler_), identifier(identifier_),
		log(&Logger::get("LeaderElection"))
	{
		node = EphemeralNodeHolder::createSequential(path + "/leader_election-", zookeeper, identifier);

		std::string node_path = node->getPath();
		node_name = node_path.substr(node_path.find_last_of('/') + 1);

		/** Если есть ноды с таким же ephemeralOwner, то удалим их.
		  * Такие ноды могли остаться после неуспешного удаления, если сессия при этом не истекла.
		  */
		zkutil::Stat node_stat;
		zookeeper.get(node_path, &node_stat);

		Strings brothers = zookeeper.getChildren(path);
		for (const auto & brother : brothers)
		{
			if (brother == node_name)
				continue;

			zkutil::Stat brother_stat;
			std::string brother_path = path + "/" + brother;
			zookeeper.get(brother_path, &brother_stat);

			if (brother_stat.ephemeralOwner == node_stat.ephemeralOwner)
			{
				LOG_WARNING(log, "Found obsolete ephemeral node from same session, removing: " + brother_path);
				zookeeper.tryRemoveWithRetries(brother_path);
			}
		}

		thread = std::thread(&LeaderElection::threadFunction, this);
	}

	enum State
	{
		WAITING_LEADERSHIP,
		LEADER,
		LEADERSHIP_LOST
	};

	/// если возвращает LEADER, то еще sessionTimeoutMs мы будем лидером, даже если порвется соединение с zookeeper
	State getState()
	{
		if (state == LEADER)
		{
			try
			{
				/// возможно, если сессия разорвалась и заново был вызван init
				if (!zookeeper.exists(node->getPath()))
				{
					LOG_WARNING(log, "Leadership lost. Node " << node->getPath() << " doesn't exist.");
					state = LEADERSHIP_LOST;
				}
			}
			catch (const KeeperException & e)
			{
				LOG_WARNING(log, "Leadership lost. e.message() = " << e.message());
				state = LEADERSHIP_LOST;
			}
		}

		return state;

	}

	~LeaderElection()
	{
		shutdown = true;
		event->set();
		thread.join();
	}

private:
	std::string path;
	ZooKeeper & zookeeper;
	LeadershipHandler handler;
	std::string identifier;

	EphemeralNodeHolderPtr node;
	std::string node_name;

	std::thread thread;
	volatile bool shutdown = false;
	zkutil::EventPtr event = new Poco::Event();

	State state = WAITING_LEADERSHIP;

	Logger * log;

	void threadFunction()
	{
		while (!shutdown)
		{
			bool success = false;

			try
			{
				Strings children = zookeeper.getChildren(path);
				std::sort(children.begin(), children.end());
				auto it = std::lower_bound(children.begin(), children.end(), node_name);
				if (it == children.end() || *it != node_name)
					throw Poco::Exception("Assertion failed in LeaderElection");

				if (it == children.begin())
				{
					state = LEADER;
					handler();
					return;
				}

				if (zookeeper.exists(path + "/" + *(it - 1), nullptr, event))
					event->wait();

				success = true;
			}
			catch (...)
			{
				DB::tryLogCurrentException("LeaderElection");
			}

			if (!success)
				event->tryWait(10 * 1000);
		}
	}
};

typedef Poco::SharedPtr<LeaderElection> LeaderElectionPtr;

}
