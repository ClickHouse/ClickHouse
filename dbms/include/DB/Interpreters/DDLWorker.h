#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/InterpreterCreateQuery.h>
#include <zkutil/ZooKeeper.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace DB
{

class DDLWorker
{
public:
	DDLWorker(Context * ctx, const std::string & host, int port)
		: context(ctx)
		, stop_flag(false)
		, thread(&DDLWorker::run, this)
	{
		local_addr = host + ":" + std::to_string(port);
	}

	~DDLWorker()
	{
		stop_flag = true;
		cond_var.notify_one();
		thread.join();
	}

private:
	void processTasks()
	{
		auto zookeeper = context->getZooKeeper();
		const std::string & base_path =
			"/clickhouse/task_queue/ddl/" + local_addr + "/create";

		const Strings & children = zookeeper->getChildren(base_path);

		for (const auto & name : children)
		{
			std::string value = zookeeper->get(base_path + "/" + name);

			std::cerr << name << std::endl;
			std::cerr << value << std::endl;
		}
	}

	void run()
	{
		using namespace std::chrono_literals;

		while (!stop_flag)
		{
			try
			{
				processTasks();
			}
			catch (...)
			{
				// TODO
			}

			std::unique_lock<std::mutex> g(lock);
			cond_var.wait_for(g, 10s);
		}
	}

private:
	Context * context;
	std::string local_addr;

	std::atomic<bool> stop_flag;
	std::condition_variable cond_var;
	std::mutex lock;
	std::thread thread;
};

}
