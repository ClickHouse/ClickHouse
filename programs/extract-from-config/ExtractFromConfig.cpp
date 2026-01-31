#include <filesystem>
#include <iostream>
#include <sstream>
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
#include <Poco/DOM/DOMWriter.h>
#include <Poco/DOM/Document.h>
#include <Poco/DOM/Element.h>
#include <Poco/DOM/Node.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/XML/XMLWriter.h>

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

        zkutil::ZooKeeperArgs args(*bootstrap_configuration, bootstrap_configuration->has("zookeeper") ? "zookeeper" : "keeper");
        zkutil::ZooKeeperPtr zookeeper = zkutil::ZooKeeper::createWithoutKillingPreviousSessions(std::move(args));

        zkutil::ZooKeeperNodeCache zk_node_cache([&] { return zookeeper; });
        config_xml = processor.processConfig(&has_zk_includes, &zk_node_cache);
    }
    return DB::ConfigurationPtr(new Poco::Util::XMLConfiguration(config_xml));
}

static void printYamlLike(const Poco::Util::AbstractConfiguration & config, const std::string & key, std::ostream & out, int indent = 0, bool is_root_call = true)
{
    std::vector<std::string> subkeys;
    config.keys(key, subkeys);

    std::string indent_str(indent, ' ');
    std::string tag = key.substr(key.find_last_of('.') + 1);

    if (subkeys.empty())
    {
        // Leaf node
        std::string value = config.getString(key);

        // Quote the value if it contains special characters or is empty
        bool needs_quotes = value.empty() || value.find_first_of(":#-?*{}[],&!|>'\"%@`") != std::string::npos;
        if (needs_quotes)
            value = "\"" + value + "\"";

        if (indent == 0 && is_root_call)
        {
            // Direct leaf value at root
            out << value;
        }
        else
        {
            out << indent_str << tag << ": " << value << "\n";
        }
        return;
    }

    // Node with children
    // Skip printing the tag only when we're at the document root (empty key at root call)
    if (is_root_call && key.empty())
    {
        // Document root - don't print a wrapper tag, just print children
        for (const auto & subkey : subkeys)
        {
            printYamlLike(config, subkey, out, indent, false);
        }
    }
    else
    {
        // Normal node or specific key at root - print the tag
        if (indent > 0 || is_root_call)
        {
            out << indent_str << tag << ":\n";
        }
        for (const auto & subkey : subkeys)
        {
            std::string full_key = key.empty() ? subkey : key + "." + subkey;
            printYamlLike(config, full_key, out, indent + 2, false);
        }
    }
}

/// Find an XML node by dotted path (e.g., "zookeeper.identity" -> finds <zookeeper><identity>...)
static Poco::XML::Node * findNodeByPath(Poco::XML::Node * node, const std::string & path)
{
    if (path.empty())
        return node;

    std::vector<std::string> path_parts;
    std::string current;
    for (char c : path)
    {
        if (c == '.')
        {
            if (!current.empty())
            {
                path_parts.push_back(current);
                current.clear();
            }
        }
        else
        {
            current += c;
        }
    }
    if (!current.empty())
        path_parts.push_back(current);

    Poco::XML::Node * current_node = node;
    for (const auto & part : path_parts)
    {
        if (!current_node)
            return nullptr;

        Poco::XML::NodeList * children = current_node->childNodes();
        Poco::XML::Node * found = nullptr;
        for (unsigned long i = 0; i < children->length(); ++i)
        {
            Poco::XML::Node * child = children->item(i);
            if (child && child->nodeType() == Poco::XML::Node::ELEMENT_NODE && child->nodeName() == part)
            {
                found = child;
                break;
            }
        }
        children->release();

        if (!found)
            return nullptr;

        current_node = found;
    }

    return current_node;
}

static void printXmlPretty(Poco::XML::Node * node, std::ostream & out)
{
    Poco::XML::DOMWriter writer;
    writer.setNewLine("\n");
    writer.setIndent("    ");
    writer.setOptions(Poco::XML::XMLWriter::PRETTY_PRINT);

    std::ostringstream oss;
    writer.writeNode(oss, node);

    // Remove XML declaration if present (we only want the node content)
    std::string result = oss.str();
    if (result.starts_with("<?xml"))
    {
        size_t end = result.find("?>");
        if (end != std::string::npos)
            result = result.substr(end + 2);
    }

    // Trim leading newlines
    size_t start = result.find_first_not_of("\n\r");
    if (start != std::string::npos)
        result = result.substr(start);

    out << result;
}

static std::vector<std::string> extractFromConfig(const std::string & config_path, const std::string & key, bool process_zk_includes, bool ignore_errors, bool get_users, std::string & format)
{
    std::string actual_config_path = config_path;

    // If we need to extract from users config
    if (get_users)
    {
        DB::ConfigurationPtr configuration = get_configuration(config_path, process_zk_includes, !ignore_errors);
        bool has_user_directories = configuration->has("user_directories");
        if (!has_user_directories && !ignore_errors)
            throw DB::Exception(DB::ErrorCodes::CANNOT_LOAD_CONFIG, "Can't load config for users");

        std::string users_config_path = configuration->getString("user_directories.users_xml.path");
        const auto config_dir = fs::path{config_path}.remove_filename().string();
        if (fs::path(users_config_path).is_relative() && fs::exists(fs::path(config_dir) / users_config_path))
            users_config_path = fs::path(config_dir) / users_config_path;
        actual_config_path = users_config_path;
    }

    // For XML pretty printing, we need the raw XML document to preserve attributes
    if (format == "xml")
    {
        DB::ConfigProcessor processor(actual_config_path,
            /* throw_on_bad_incl = */ false,
            /* log_to_console = */ false,
            /* substitutions= */ {},
            /* throw_on_bad_include_from= */ !ignore_errors);
        bool has_zk_includes;
        DB::XMLDocumentPtr config_xml = processor.processConfig(&has_zk_includes);
        if (has_zk_includes && process_zk_includes)
        {
            DB::ConfigurationPtr bootstrap_configuration(new Poco::Util::XMLConfiguration(config_xml));

            zkutil::validateZooKeeperConfig(*bootstrap_configuration);

            zkutil::ZooKeeperArgs args(*bootstrap_configuration, bootstrap_configuration->has("zookeeper") ? "zookeeper" : "keeper");
            zkutil::ZooKeeperPtr zookeeper = zkutil::ZooKeeper::createWithoutKillingPreviousSessions(std::move(args));

            zkutil::ZooKeeperNodeCache zk_node_cache([&] { return zookeeper; });
            config_xml = processor.processConfig(&has_zk_includes, &zk_node_cache);
        }

        Poco::XML::Node * root = config_xml->documentElement();
        Poco::XML::Node * target_node;

        // If key is empty, use the root element
        if (key.empty())
        {
            target_node = root;
        }
        else
        {
            target_node = findNodeByPath(root, key);
            if (!target_node)
            {
                if (!ignore_errors)
                    throw DB::Exception(DB::ErrorCodes::CANNOT_LOAD_CONFIG, "Not found: {}", key);
                return {""};
            }
        }

        std::ostringstream oss;
        printXmlPretty(target_node, oss);
        return {oss.str()};
    }

    // For YAML and plain text, use AbstractConfiguration
    DB::ConfigurationPtr configuration = get_configuration(actual_config_path, process_zk_includes, !ignore_errors);

    /// Check if a key has globs (only if key is not empty).
    if (!key.empty() && key.find_first_of("*?{") != std::string::npos)
        return extactFromConfigAccordingToGlobs(configuration, key, ignore_errors);

    /// Do not throw exception if not found (skip check for empty key - root always exists).
    if (!key.empty() && !configuration->has(key))
    {
        if (!ignore_errors)
            throw DB::Exception(DB::ErrorCodes::CANNOT_LOAD_CONFIG, "Not found: {}", key);
        return {""};
    }

    if (format == "yaml" || format == "yml")
    {
        std::ostringstream oss;
        printYamlLike(*configuration, key, oss);
        return {oss.str()};
    }
    else
    {
        // For plain text output, we need a specific key
        if (key.empty())
        {
            if (!ignore_errors)
                throw DB::Exception(DB::ErrorCodes::CANNOT_LOAD_CONFIG, "Key is required for plain text output. Use --pretty for formatted output.");
            return {""};
        }
        return {configuration->getString(key)} ;
    }
}

#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseExtractFromConfig(int argc, char ** argv)
{
    bool print_stacktrace = false;
    bool process_zk_includes = false;
    bool ignore_errors = false;
    bool get_users = false;
    bool pretty = false;
    std::string log_level;
    std::string config_path;
    std::string key;
    std::string format;
    std::set<std::string> allowed_output_formats = {"", "yaml", "yml", "xml"};

    namespace po = boost::program_options;

    po::options_description options_desc("Allowed options");
    options_desc.add_options()
        ("help", "produce this help message")
        ("stacktrace", po::bool_switch(&print_stacktrace), "print stack traces of exceptions")
        ("process-zk-includes", po::bool_switch(&process_zk_includes),
         "if there are from_zk elements in config, connect to ZooKeeper and process them")
        ("try", po::bool_switch(&ignore_errors), "Do not warn about missing keys, missing users configurations or non existing file from include_from tag")
        ("pretty", po::bool_switch(&pretty), "Enable pretty printing (defaults to yaml format, use -o to specify xml)")
        ("format,o", po::value<std::string>(&format)->default_value(""), "Output format for pretty printing: yaml or xml (only used with --pretty)")
        ("users", po::bool_switch(&get_users), "Return values from users.xml config")
        ("log-level", po::value<std::string>(&log_level)->default_value("error"), "log level")
        ("config-file,c", po::value<std::string>(&config_path)->required(), "path to config file")
        ("key,k", po::value<std::string>(&key)->default_value(""), "key to get value for (if empty, outputs entire config)");

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
                << "If no key is provided with --pretty, outputs the entire config in the specified format." << std::endl
                << std::endl;
            std::cerr << "Usage: clickhouse extract-from-config [options] config-file [key]" << std::endl
                << std::endl;
            std::cerr << options_desc << std::endl;
            return 0;
        }

        boost::algorithm::to_lower(format);
        if (!allowed_output_formats.contains(format))
        {
            std::cerr << "Invalid format: " << format << "\n"
                      << "Allowed formats yaml, yml, xml or empty\n";
            return 1;
        }

        po::notify(options);

        if (pretty)
        {
            if (format.empty())
                format = "yaml";
        }
        else
        {
            // Keep backward compatibility -- ignore format and return plain value
            format = "";
        }

        setupLogging(log_level);
        for (const auto & value : extractFromConfig(config_path, key, process_zk_includes, ignore_errors, get_users, format))
        {
            std::cout << value;
            // check trailing new line
            if (!value.empty() && value.back() != '\n')
                std::cout << std::endl;
        }
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(print_stacktrace, true) << std::endl;
        return DB::getCurrentExceptionCode();
    }

    return 0;
}
