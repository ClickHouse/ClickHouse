#pragma once

#include <DB/Core/Types.h>
#include <DB/Interpreters/Context.h>

#include <common/logger_useful.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace DB
{

/**
 *
 */
class DDLWorker
{
public:
	DDLWorker(const Poco::Util::AbstractConfiguration & config,
			  const std::string & config_name, Context & context_);
	~DDLWorker();

private:
	/// Получить список баз данных для указанного кластера.
	/// Если в заданном кластере ни чего не создано, то возвращается
	/// пустой список.
	Strings getClusterDatabases(const String & cluster_name) const;

	/// Список кластеров, в которые входит текущий процесс.
	Strings getClusters() const;

	/// Синхронизировать состояние в указанном кластере.
	void processCluster(const String & cluster_name, const Strings & databases);

	void processTable(const std::string & path);

	void run();

private:
	Context & context;
	Logger * log = &Logger::get("DDLWorker");

	std::string clusters_path;

	std::atomic<bool> stop_flag;
	std::condition_variable cond_var;
	std::mutex lock;
	std::thread thread;
};

}
