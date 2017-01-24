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
	DDLWorker(Context * ctx, const std::string & host, int port);
	~DDLWorker();

private:
	void processTasks();

	void run();

private:
	Context * context;
	std::string local_addr;

	std::atomic<bool> stop_flag;
	std::condition_variable cond_var;
	std::mutex lock;
	std::thread thread;
};

}
