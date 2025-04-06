#include <filesystem>
#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <queue>
#include <sstream>
#include <fstream>

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/DOMWriter.h>
#include <Poco/DOM/Node.h>
#include <Poco/XML/XMLWriter.h>

#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Exception.h>
#include <Common/parseGlobs.h>
#include <Common/re2.h>
#include <Common/XMLUtils.h>

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

static std::string getXMLSubTreeAsString(DB::XMLDocumentPtr config_xml, const std::string& key)
{
    Poco::XML::DOMWriter writer;
    writer.setNewLine("\n");
    writer.setIndent("    ");
    writer.setOptions(Poco::XML::XMLWriter::PRETTY_PRINT);

    std::ostringstream oss;

    Poco::XML::Node * node = DB::XMLUtils::getRootNode(config_xml.get())->getNodeByPath(key);
    if (!node)
        return "";

    if (key.empty())
    {
        // Write the entire tree
        writer.writeNode(oss, node);
    }
    else
    {
        // Write the node's children
        Poco::XML::Node * child = node->firstChild();
        while (child)
        {
            writer.writeNode(oss, child);
            child = child->nextSibling();
        }
    }

    return oss.str();
}

static std::vector<std::string> extactFromConfigAccordingToGlobs(DB::ConfigurationPtr configuration, const std::string & pattern, bool try_get)
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

        /// This is a leaf
        if (keys.empty())
        {
            if (!re2::RE2::FullMatch(node, *matcher))
                continue;


            if (try_get)
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


static DB::XMLDocumentPtr getXMLconfiguration(const std::string & config_path, bool process_zk_includes)
{
    DB::ConfigProcessor processor(config_path, /* throw_on_bad_incl = */ false, /* log_to_console = */ false);
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
    return config_xml;
}

static DB::XMLDocumentPtr getUsersXMLConfiguration(DB::XMLDocumentPtr configuration_xml, const std::string & config_path, bool try_get)
{
    DB::ConfigurationPtr configuration = DB::ConfigurationPtr(new Poco::Util::XMLConfiguration(configuration_xml));
    bool has_user_directories = configuration->has("user_directories");
    if (!has_user_directories && !try_get)
        throw DB::Exception(DB::ErrorCodes::CANNOT_LOAD_CONFIG, "Can't load config for users");

    std::string users_config_path = configuration->getString("user_directories.users_xml.path");
    const auto config_dir = fs::path{config_path}.remove_filename().string();
    if (fs::path(users_config_path).is_relative() && fs::exists(fs::path(config_dir) / users_config_path))
        users_config_path = fs::path(config_dir) / users_config_path;
    return getXMLconfiguration(users_config_path, false);
}

static std::vector<std::string> extractFromConfig(
        const std::string & config_path, const std::string & key, bool process_zk_includes, bool try_get = false, bool get_users = false)
{
    DB::XMLDocumentPtr configuration_xml = getXMLconfiguration(config_path, process_zk_includes);
    if (get_users)
        configuration_xml = getUsersXMLConfiguration(configuration_xml, config_path, try_get);
    DB::ConfigurationPtr configuration = DB::ConfigurationPtr(new Poco::Util::XMLConfiguration(configuration_xml));

    // Check if this key has a non-scalar value by looking for subkeys
    Poco::Util::XMLConfiguration::Keys keys;
    configuration->keys(key, keys);
    if (!keys.empty())
    {
        // Non-scalar object, Output XML node
        return {getXMLSubTreeAsString(configuration_xml, key)};
    }

    // Check if a key has globs.
    if (key.find_first_of("*?{") != std::string::npos)
        return extactFromConfigAccordingToGlobs(configuration, key, try_get);

    // Do not throw exception if not found.
    if (try_get)
        return {configuration->getString(key, "")};
    return {configuration->getString(key)};
}

#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseExtractFromConfig(int argc, char ** argv)
{
    bool print_stacktrace = false;
    bool process_zk_includes = false;
    bool try_get = false;
    bool get_users = false;
    std::string log_level;
    std::string config_path;
    std::string key;
    std::string output_file;

    namespace po = boost::program_options;

    po::options_description options_desc("Allowed options");
    options_desc.add_options()
        ("help", "produce this help message")
        ("stacktrace", po::bool_switch(&print_stacktrace), "print stack traces of exceptions")
        ("process-zk-includes", po::bool_switch(&process_zk_includes),
         "if there are from_zk elements in config, connect to ZooKeeper and process them")
        ("try", po::bool_switch(&try_get), "Do not warn about missing keys")
        ("users", po::bool_switch(&get_users), "Return values from users.xml config")
        ("log-level", po::value<std::string>(&log_level)->default_value("error"), "log level")
        ("config-file,c", po::value<std::string>(&config_path)->required(), "path to config file")
        ("key,k", po::value<std::string>(&key)->default_value(""), "key to get value for")
        ("output,o", po::value<std::string>(&output_file), "output file path");

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
            std::cerr << "Usage: clickhouse extract-from-config [options]" << std::endl
                << std::endl;
            std::cerr << options_desc << std::endl;
            return 0;
        }

        po::notify(options);

        setupLogging(log_level);

        std::ostream* output_stream = &std::cout;
        std::unique_ptr<std::ofstream> file_stream;

        // If output file is provided, use it as the output stream
        if (!output_file.empty())
        {
            file_stream = std::make_unique<std::ofstream>(output_file);
            if (!file_stream->is_open())
                throw DB::Exception(DB::ErrorCodes::CANNOT_LOAD_CONFIG, "Cannot open output file: {}", output_file);
            output_stream = file_stream.get();
        }

        if (key.empty())
        {
            // If no key provided, print the entire configuration tree
            DB::XMLDocumentPtr configuration_xml = getXMLconfiguration(config_path, process_zk_includes);
            if (get_users)
                configuration_xml = getUsersXMLConfiguration(configuration_xml, config_path, try_get);
            *output_stream << getXMLSubTreeAsString(configuration_xml, "") << std::endl;
        }
        else
        {
            for (const auto & value : extractFromConfig(config_path, key, process_zk_includes, try_get, get_users))
                *output_stream << value << std::endl;
        }

        if (file_stream)
            file_stream->close();

    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(print_stacktrace, true) << std::endl;
        return DB::getCurrentExceptionCode();
    }

    return 0;
}
