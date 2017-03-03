#include <DB/Interpreters/Cluster.h>
#include <DB/Interpreters/DDLWorker.h>
#include <DB/Interpreters/executeQuery.h>
#include <DB/Common/getFQDNOrHostName.h>

#include <zkutil/ZooKeeper.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_ELEMENT_IN_CONFIG;
	extern const int INVALID_CONFIG_PARAMETER;
}

namespace {

/// Helper class which extracts from the ClickHouse configuration file
/// the parameters we need for operating the ddl thread.
class Arguments final
{
public:
	Arguments(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
	{
		Poco::Util::AbstractConfiguration::Keys keys;
		config.keys(config_name, keys);

		for (const auto & key : keys)
		{
			if (key == "ddl_path")
				ddl_path = config.getString(config_name + "." + key);
			else
				throw Exception{"Unknown parameter in ddl configuration", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};
		}

		if (ddl_path.empty())
			throw Exception{"DDL: missing parameter ddl_path", ErrorCodes::INVALID_CONFIG_PARAMETER};
	}

	Arguments(const Arguments &) = delete;
	Arguments & operator=(const Arguments &) = delete;

	std::string getDDLPath() const
	{
		return ddl_path;
	}

private:
	std::string ddl_path;
};

}

DDLWorker::DDLWorker(const Poco::Util::AbstractConfiguration & config,
				 	 const std::string & config_name, Context & context_)
	: context(context_)
	, stop_flag(false)
{
	Arguments arguments(config, config_name);
	auto zookeeper = context.getZooKeeper();

	clusters_path = arguments.getDDLPath();
	if (clusters_path.back() != '/')
		clusters_path += "/";

	thread = std::thread(&DDLWorker::run, this);
}

DDLWorker::~DDLWorker()
{
	stop_flag = true;
	cond_var.notify_one();
	thread.join();
}

Strings DDLWorker::getClusterDatabases(const String & cluster_name) const {
	Strings result;

	if (context.getZooKeeper()->tryGetChildren(clusters_path + cluster_name + "/databases", result) == 0)
		return result;

	return Strings();
}

Strings DDLWorker::getClusters() const {
	return context.getClusters().getClustersForHost(getFQDNOrHostName());
}

void DDLWorker::processCluster(const String & cluster_name, const Strings & databases)
{
	// Пройтись по всем базам данных
	for (const auto & database : databases)
	{
		Strings tables;

		if (context.getZooKeeper()->tryGetChildren(clusters_path + cluster_name + "/databases/" + database + "/tables", tables) != 0)
			continue;

		for (const auto & table : tables)
			try {
				// Временно считаем, что таблица соответствует спецификации,
				// но, вообще, надо проверять.
				if (context.isTableExist(database, table))
					continue;
				else
				{
					processTable(clusters_path + cluster_name + "/databases/" + database + "/tables/" + table);
				}
			}
			catch (...)
			{
				tryLogCurrentException(log);
			}
	}
}

void DDLWorker::processTable(const std::string & path)
{
	String command;

	if (context.getZooKeeper()->tryGet(path, command))
	{
		if (!command.empty())
			executeQuery(command, context);
	}
}

void DDLWorker::run()
{
	using namespace std::chrono_literals;

	while (!stop_flag)
	{
		try
		{
			// Вычислить в какой список кластеров входит текущий процесс.
			// Если список пустой, то ни чего делать не нужно,
			// иначе надо выполнить функцию синхронизации состояния.
			const Strings & clusters = getClusters();

			// Последовательно синхронизировать состояние всех кластеров.
			for (const String & name : clusters)
			{
				const Strings databases = getClusterDatabases(name);

				if (!databases.empty())
					processCluster(name, databases);
			}
		}
		catch (...)
		{
			tryLogCurrentException(log);
		}

		std::unique_lock<std::mutex> g(lock);
		cond_var.wait_for(g, 10s);
	}
}

}
