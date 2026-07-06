#include <filesystem>
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <queue>

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Exception.h>
#include <Common/parseGlobs.h>
#include <Common/re2.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_LOAD_CONFIG;
    }
}

namespace fs = std::filesystem;

static void setupLogging(const std::string & log_level)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel);
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
    formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel(log_level);
}


static std::vector<std::string> extactFromConfigAccordingToGlobs(DB::ConfigurationPtr configuration, const std::string & pattern, bool ignore_errors)
{
    auto pattern_prefix = pattern.substr(0, pattern.find_first_of("*?{"));
    boost::algorithm::trim_if(pattern_prefix, [](char s){ return s == '.'; });

    auto matcher = std::make_unique<re2::RE2>(DB::makeRegexpPatternFromGlobs(pattern));

    std::vector<std::string> result;
    std::queue<std::string> working_queue;
    working_queue.emplace(pattern_prefix);

    while (!working_queue.empty())
    {
        auto node = working_queue.front();
        working_queue.pop();

        /// Disclose one more layer
        Poco::Util::AbstractConfiguration::Keys keys;
        configuration->keys(node, keys);

        /// This is a leave
        if (keys.empty())
        {
            if (!re2::RE2::FullMatch(node, *matcher))
                continue;


            if (ignore_errors)
            {
                auto value = configuration->getString(node, "");
                if (!value.empty())
                    result.emplace_back(value);
            }
            else
            {
                result.emplace_back(configuration->getString(node));
            }
            continue;
        }

        /// Add new nodes to working queue
        for (const auto & key : keys)
            working_queue.emplace(fmt::format("{}.{}", node, key));
    }

    return result;
}


static DB::ConfigurationPtr get_configuration(const std::string & config_path, bool process_zk_includes, bool throw_on_bad_include_from)
{
    DB::ConfigProcessor processor(config_path,
        /* throw_on_bad_incl = */ false,
        /* log_to_console = */ false,
        /* substitutions= */ {},
        /* throw_on_bad_include_from= */ throw_on_bad_include_from);
    bool has_zk_includes;
    DB::XMLDocumentPtr config_xml = processor.processConfig(&has_zk_includes);
    if (has_zk_includes && process_zk_includes)
    {
        DB::ConfigurationPtr bootstrap_configuration(new Poco::Util::XMLConfiguration(config_xml));

        zkutil::validateZooKeeperConfig(*bootstrap_configuration);

        zkutil::ZooKeeperPtr zookeeper = zkutil::ZooKeeper::createWithoutKillingPreviousSessions(
            *bootstrap_configuration, bootstrap_configuration->has("zookeeper") ? "zookeeper" : "keeper");

        zkutil::ZooKeeperNodeCache zk_node_cache([&] { return zookeeper; });
        config_xml = processor.processConfig(&has_zk_includes, &zk_node_cache);
    }
    return DB::ConfigurationPtr(new Poco::Util::XMLConfiguration(config_xml));
}


static std::vector<std::string> extractFromConfig(const std::string & config_path, const std::string & key, bool process_zk_includes, bool ignore_errors, bool get_users)
{
    DB::ConfigurationPtr configuration = get_configuration(config_path, process_zk_includes, !ignore_errors);

    if (get_users)
    {
        bool has_user_directories = configuration->has("user_directories");
        if (!has_user_directories && !ignore_errors)
            throw DB::Exception(DB::ErrorCodes::CANNOT_LOAD_CONFIG, "Can't load config for users");

        std::string users_config_path = configuration->getString("user_directories.users_xml.path");
        const auto config_dir = fs::path{config_path}.remove_filename().string();
        if (fs::path(users_config_path).is_relative() && fs::exists(fs::path(config_dir) / users_config_path))
            users_config_path = fs::path(config_dir) / users_config_path;
        configuration = get_configuration(users_config_path, process_zk_includes, !ignore_errors);
    }

    /// Check if a key has globs.
    if (key.find_first_of("*?{") != std::string::npos)
        return extactFromConfigAccordingToGlobs(configuration, key, ignore_errors);

    /// Do not throw exception if not found.
    if (ignore_errors)
        return {configuration->getString(key, "")};
    return {configuration->getString(key)};
}

#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseExtractFromConfig(int argc, char ** argv)
{
    bool print_stacktrace = false;
    bool process_zk_includes = false;
    bool ignore_errors = false;
    bool get_users = false;
    std::string log_level;
    std::string config_path;
    std::string key;

    namespace po = boost::program_options;

    po::options_description options_desc("Allowed options");
    options_desc.add_options()
        ("help", "produce this help message")
        ("stacktrace", po::bool_switch(&print_stacktrace), "print stack traces of exceptions")
        ("process-zk-includes", po::bool_switch(&process_zk_includes),
         "if there are from_zk elements in config, connect to ZooKeeper and process them")
        ("try", po::bool_switch(&ignore_errors), "Do not warn about missing keys, missing users configurations or non existing file from include_from tag")
        ("users", po::bool_switch(&get_users), "Return values from users.xml config")
        ("log-level", po::value<std::string>(&log_level)->default_value("error"), "log level")
        ("config-file,c", po::value<std::string>(&config_path)->required(), "path to config file")
        ("key,k", po::value<std::string>(&key)->required(), "key to get value for");

    po::positional_options_description positional_desc;
    positional_desc.add("config-file", 1);
    positional_desc.add("key", 1);

    try
    {
        po::variables_map options;
        po::store(po::command_line_parser(argc, argv).options(options_desc).positional(positional_desc).run(), options);

        if (options.contains("help"))
        {
            std::cerr << "Preprocess config file and extract value of the given key." << std::endl
                << std::endl;
            std::cerr << "Usage: clickhouse extract-from-config [options]" << std::endl
                << std::endl;
            std::cerr << options_desc << std::endl;
            return 0;
        }

        po::notify(options);

        setupLogging(log_level);
        for (const auto & value : extractFromConfig(config_path, key, process_zk_includes, ignore_errors, get_users))
            std::cout << value << std::endl;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(print_stacktrace, true) << std::endl;
        return DB::getCurrentExceptionCode();
    }

    return 0;
}
