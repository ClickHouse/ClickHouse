#include <iostream>

#include <boost/program_options.hpp>
#include <Poco/Util/XMLConfiguration.h>

#include <zkutil/ZooKeeperNodeCache.h>
#include <DB/Common/ConfigProcessor.h>
#include <DB/Common/Exception.h>


static std::string extractFromConfig(
		const std::string & config_path, const std::string & key, bool process_zk_includes)
{
	ConfigProcessor processor(/* throw_on_bad_incl = */ false, /* log_to_console = */ true);
	bool has_zk_includes;
	XMLDocumentPtr config_xml = processor.processConfig(config_path, &has_zk_includes);
	if (has_zk_includes && process_zk_includes)
	{
		ConfigurationPtr bootstrap_configuration(new Poco::Util::XMLConfiguration(config_xml));
		zkutil::ZooKeeperPtr zookeeper = std::make_shared<zkutil::ZooKeeper>(
				*bootstrap_configuration, "zookeeper");
		zkutil::ZooKeeperNodeCache zk_node_cache([&] { return zookeeper; });
		config_xml = processor.processConfig(config_path, &has_zk_includes, &zk_node_cache);
	}
	ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));
	return configuration->getString(key);
}

int mainEntryClickHouseExtractFromConfig(int argc, char ** argv)
{
	bool print_stacktrace = false;
	bool process_zk_includes = false;
	std::string config_path;
	std::string key;

	namespace po = boost::program_options;

	po::options_description options_desc("Allowed options");
	options_desc.add_options()
		("help", "produce this help message")
		("stacktrace", po::bool_switch(&print_stacktrace), "print stack traces of exceptions")
		("process-zk-includes", po::bool_switch(&process_zk_includes),
		 "if there are from_zk elements in config, connect to ZooKeeper and process them")
		("config-file,c", po::value<std::string>(&config_path)->required(), "path to config file")
		("key,k", po::value<std::string>(&key)->required(), "key to get value for");

	po::positional_options_description positional_desc;
	positional_desc.add("config-file", 1);
	positional_desc.add("key", 1);

	try
	{
		po::variables_map options;
		po::store(po::command_line_parser(argc, argv).options(options_desc).positional(positional_desc).run(), options);

		if (options.count("help"))
		{
			std::cerr << "Preprocess config file and extract value of the given key." << std::endl
				<< std::endl;
			std::cerr << "Usage: clickhouse --extract-from-config [options]" << std::endl
				<< std::endl;
			std::cerr << options_desc << std::endl;
			return 0;
		}

		po::notify(options);

		std::cout << extractFromConfig(config_path, key, process_zk_includes) << std::endl;
	}
	catch (...)
	{
		std::cerr << DB::getCurrentExceptionMessage(print_stacktrace, true) << std::endl;
		return DB::getCurrentExceptionCode();
	}

	return 0;
}
