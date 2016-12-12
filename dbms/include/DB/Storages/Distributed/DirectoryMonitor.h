#pragma once

#include <DB/Storages/StorageDistributed.h>

#include <thread>
#include <mutex>
#include <condition_variable>


namespace DB
{

/** Details of StorageDistributed.
  * This type is not designed for standalone use.
  */
class StorageDistributedDirectoryMonitor
{
public:
	StorageDistributedDirectoryMonitor(StorageDistributed & storage, const std::string & name);
	~StorageDistributedDirectoryMonitor();

private:
	void run();
	ConnectionPoolPtr createPool(const std::string & name);
	bool findFiles();
	void processFile(const std::string & file_path);
	std::string getLoggerName() const;

	StorageDistributed & storage;
	ConnectionPoolPtr pool;
	std::string path;
	size_t error_count{};
	std::chrono::milliseconds default_sleep_time;
	std::chrono::milliseconds sleep_time;
	std::chrono::time_point<std::chrono::system_clock> last_decrease_time {std::chrono::system_clock::now()};
	bool quit {false};
	std::mutex mutex;
	std::condition_variable cond;
	Logger * log;
	std::thread thread {&StorageDistributedDirectoryMonitor::run, this};
};

}
