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
	DDLWorker(const Poco::Util::AbstractConfiguration & config,
			  const std::string & config_name, Context & context_,
			  const std::string & host, int port);
	~DDLWorker();

private:
	void processTasks();
	void processCreate(const std::string & path);

	void run();

private:
	Context & context;
	std::string local_addr;
	std::string base_path;

	std::atomic<bool> stop_flag;
	std::condition_variable cond_var;
	std::mutex lock;
	std::thread thread;
};

}
