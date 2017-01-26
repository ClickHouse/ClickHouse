#include <DB/Interpreters/DDLWorker.h>
#include <DB/Interpreters/executeQuery.h>

namespace DB
{

DDLWorker::DDLWorker(Context * ctx, const std::string & host, int port)
	: context(ctx)
	, stop_flag(false)
	, thread(&DDLWorker::run, this)
{
	local_addr = host + ":" + std::to_string(port);
	base_path = "/clickhouse/task_queue/ddl/";
}

DDLWorker::~DDLWorker() {
	stop_flag = true;
	cond_var.notify_one();
	thread.join();
}

void DDLWorker::processTasks() {
	const std::string path = base_path + local_addr;

	processCreate(path + "/create");
}

void DDLWorker::processCreate(const std::string & path) {
	auto zookeeper = context->getZooKeeper();
	const Strings & children = zookeeper->getChildren(path);

	for (const auto & name : children) {
		try {
			std::string path = path + "/" + name;
			std::string value = zookeeper->get(path);

			if (!value.empty()) {
				executeQuery(value, *context);
			}

			zookeeper->remove(path);
		} catch (const std::exception& e) {
			std::cerr << "execption " << e.what() << std::endl;
		}
	}
}

void DDLWorker::run() {
	using namespace std::chrono_literals;

	while (!stop_flag) {
		try {
			processTasks();
		} catch (const std::exception& e) {
			std::cerr << "execption " << e.what() << std::endl;
		}

		std::unique_lock<std::mutex> g(lock);
		cond_var.wait_for(g, 10s);
	}
}

}
