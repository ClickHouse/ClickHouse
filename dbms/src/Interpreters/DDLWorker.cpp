#include <DB/Interpreters/DDLWorker.h>

namespace DB
{

DDLWorker::DDLWorker(Context * ctx, const std::string & host, int port)
	: context(ctx)
	, stop_flag(false)
	, thread(&DDLWorker::run, this)
{
	local_addr = host + ":" + std::to_string(port);
}

DDLWorker::~DDLWorker()
{
	stop_flag = true;
	cond_var.notify_one();
	thread.join();
}

void DDLWorker::processTasks()
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

void DDLWorker::run()
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

}
