#pragma once

#include <zkutil/ZooKeeper.h>
#include <functional>
#include <memory>
#include <common/logger_useful.h>


namespace zkutil
{

/** Реализует метод выбора лидера, описанный здесь: http://zookeeper.apache.org/doc/r3.4.5/recipes.html#sc_leaderElection
  */
class LeaderElection
{
public:
	using LeadershipHandler = std::function<void()>;

	/** handler вызывается, когда этот экземпляр становится лидером.
	  */
	LeaderElection(const std::string & path_, ZooKeeper & zookeeper_, LeadershipHandler handler_, const std::string & identifier_ = "")
		: path(path_), zookeeper(zookeeper_), handler(handler_), identifier(identifier_),
		log(&Logger::get("LeaderElection"))
	{
		node = EphemeralNodeHolder::createSequential(path + "/leader_election-", zookeeper, identifier);

		std::string node_path = node->getPath();
		node_name = node_path.substr(node_path.find_last_of('/') + 1);

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
	zkutil::EventPtr event = std::make_shared<Poco::Event>();

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

using LeaderElectionPtr = std::shared_ptr<LeaderElection>;

}
