#include "YAMLProcessor.h"

#include <sys/utsname.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <functional>
#include <filesystem>
#include <Poco/DOM/Text.h>
#include <Poco/DOM/Attr.h>
#include <Poco/DOM/Comment.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Exception.h>
#include <common/getResource.h>
#include <common/errnoToString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


#define PREPROCESSED_SUFFIX "-preprocessed"


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

/// For cutting preprocessed path to this base
static std::string main_config_path;

/// Extracts from a string the first encountered number consisting of at least two digits.
static std::string numberFromHost(const std::string & s)
{
    for (size_t i = 0; i < s.size(); ++i)
    {
        std::string res;
        size_t j = i;
        while (j < s.size() && isNumericASCII(s[j]))
            res += s[j++];
        if (res.size() >= 2)
        {
            while (res[0] == '0')
                res.erase(res.begin());
            return res;
        }
    }
    return "";
}

bool YAMLProcessor::isPreprocessedFile(const std::string & path)
{
    return endsWith(Poco::Path(path).getBaseName(), PREPROCESSED_SUFFIX);
}


YAMLProcessor::YAMLProcessor(
    const std::string & path_,
    bool throw_on_bad_incl_,
    bool log_to_console,
    const Substitutions & substitutions_)
    : path(path_)
    , throw_on_bad_incl(throw_on_bad_incl_)
    , substitutions(substitutions_)
    /// We need larger name pool to allow to support vast amount of users in users.xml files for ClickHouse.
    /// Size is prime because Poco::XML::NamePool uses bad (inefficient, low quality)
    ///  hash function internally, and its size was prime by default.
{
    if (log_to_console && !Poco::Logger::has("YAMLProcessor"))
    {
        channel_ptr = new Poco::ConsoleChannel;
        log = &Poco::Logger::create("YAMLProcessor", channel_ptr.get(), Poco::Message::PRIO_TRACE);
    }
    else
    {
        log = &Poco::Logger::get("YAMLProcessor");
    }
}

YAMLProcessor::~YAMLProcessor()
{
    if (channel_ptr) /// This means we have created a new console logger in the constructor.
        Poco::Logger::destroy("YAMLProcessor");
}


/// Vector containing the name of the element and a sorted list of attribute names and values
/// (except "remove" and "replace" attributes).
/// Serves as a unique identifier of the element contents for comparison.
using ElementIdentifier = std::vector<std::string>;

using NamedNodeMapPtr = Poco::AutoPtr<Poco::XML::NamedNodeMap>;
/// NOTE getting rid of iterating over the result of Node.childNodes() call is a good idea
/// because accessing the i-th element of this list takes O(i) time.
using NodeListPtr = Poco::AutoPtr<Poco::XML::NodeList>;

static ElementIdentifier getElementIdentifier(YAML::Node element)
{
    /*const NamedNodeMapPtr attrs = element->attributes(); //get yaml node attr
    std::vector<std::pair<std::string, std::string>> attrs_kv;
    for (size_t i = 0, size = attrs->length(); i < size; ++i)
    {
        const Node * node = attrs->item(i);
        std::string name = node->nodeName();
        const auto * subst_name_pos = std::find(YAMLProcessor::SUBSTITUTION_ATTRS.begin(), YAMLProcessor::SUBSTITUTION_ATTRS.end(), name);
        if (name == "replace" || name == "remove" ||
            subst_name_pos != YAMLProcessor::SUBSTITUTION_ATTRS.end())
            continue;
        std::string value = node->nodeValue();
        attrs_kv.push_back(std::make_pair(name, value));
    }
    std::sort(attrs_kv.begin(), attrs_kv.end());

    ElementIdentifier res;
    res.push_back(element->nodeName());
    for (const auto & attr : attrs_kv)
    {
        res.push_back(attr.first);
        res.push_back(attr.second);
    }

    return res;*/
}

static YAML::Node getRootNode(YAML::Node input)
{
    for (YAML::const_iterator i = input.begin(); i != input.end(); ++i)
    {
        auto child = children->item(i);
        /// Besides the root element there can be comment nodes on the top level.
        /// Skip them.
        if (child->nodeType() == Node::ELEMENT_NODE)
            return child;
    }

    throw Poco::Exception("No root node in document");
}

static bool allWhitespace(const std::string & s)
{
    //return s.find_first_not_of(" \t\n\r") == std::string::npos;
}

inline const YAML::Node & cnode(const YAML::Node &n) {
    return n;
}

YAML::Node YAMLProcessor::mergeRecursive(YAML::Node config_root, const YAML::Node with_root)
{
    if (!b.IsMap()) {
        // If b is not a map, merge result is b, unless b is null
        return b.IsNull() ? a : b;
    }
    if (!a.IsMap()) {
        // If a is not a map, merge result is b
        return b;
    }
    if (!b.size()) {
        // If a is a map, and b is an empty map, return a
        return a;
    }
    // Create a new map 'c' with the same mappings as a, merged with b
    auto c = YAML::Node(YAML::NodeType::Map);
    for (auto n : a) {
        if (n.first.IsScalar()) {
            const std::string & key = n.first.Scalar();
            auto t = YAML::Node(cnode(b)[key]);
        if (t) {
            c[n.first] = mergeRecursive(n.second, t);
            continue;
        }
    }
    c[n.first] = n.second;
    }
    // Add the mappings from 'b' not already in 'c'
    for (auto n : b) {
        if (!n.first.IsScalar() || !cnode(c)[n.first.Scalar()]) {
        c[n.first] = n.second;
        }
    }
    return c;
}

void YAMLProcessor::merge(YMLDocumentPtr config, YMLDocumentPtr with)
{
    YAML::Node config_root = YAML::LoadFile(config);
    YAML::Node with_root = YAML::LoadFile(with);

    if (config_root->nodeName() != with_root->nodeName())
        // find the name of the node
        //throw Poco::Exception("Root element doesn't have the corresponding root element as the config file. It must be <" + config_root->nodeName() + ">");

    auto merged = mergeRecursive(config_root, with_root);
}

static std::string layerFromHost()
{
    utsname buf;
    if (uname(&buf))
        throw Poco::Exception(std::string("uname failed: ") + errnoToString(errno));

    std::string layer = numberFromHost(buf.nodename);
    if (layer.empty())
        throw Poco::Exception(std::string("no layer in host name: ") + buf.nodename);

    return layer;
}

void YAMLProcessor::doIncludesRecursive(
        YMLDocumentPtr config,
        YMLDocumentPtr include_from,
        YAML::Node node,
        zkutil::ZooKeeperNodeCache * zk_node_cache,
        const zkutil::EventPtr & zk_changed_event,
        std::unordered_set<std::string> & contributing_zk_paths)
{
    if (node.Type() == YAML::Node::Scalar) //text from poco
    {
        for (auto & substitution : substitutions)
        {
            std::string value = node->nodeValue();

            bool replace_occured = false;
            size_t pos;
            while ((pos = value.find(substitution.first)) != std::string::npos)
            {
                value.replace(pos, substitution.first.length(), substitution.second);
                replace_occured = true;
            }

            if (replace_occured)
                node->setNodeValue(value);
        }
    }

    if (node->nodeType() != Node::ELEMENT_NODE)
        return;

    /// Substitute <layer> for the number extracted from the hostname only if there is an
    /// empty <layer> tag without attributes in the original file.
    if (node->nodeName() == "layer"
        && !node->hasAttributes()
        && !node->hasChildNodes()
        && node->nodeValue().empty())
    {
        NodePtr new_node = config->createTextNode(layerFromHost());
        node->appendChild(new_node);
        return;
    }

    std::map<std::string, const YAML::Node> attr_nodes;
    NamedNodeMapPtr attributes = node->attributes();
    size_t substs_count = 0;
    for (const auto & attr_name : SUBSTITUTION_ATTRS)
    {
        const auto * subst = attributes->getNamedItem(attr_name);
        attr_nodes[attr_name] = subst;
        substs_count += static_cast<size_t>(subst == nullptr);
    }

    if (substs_count < SUBSTITUTION_ATTRS.size() - 1) /// only one substitution is allowed
        throw Poco::Exception("several substitutions attributes set for element <" + node->nodeName() + ">");

    /// Replace the original contents, not add to it.
    bool replace = attributes->getNamedItem("replace");

    bool included_something = false;

    auto process_include = [&](const Node * include_attr, const std::function<const Node * (const std::string &)> & get_node, const char * error_msg)
    {
        const std::string & name = include_attr->getNodeValue();
        const Node * node_to_include = get_node(name);
        if (!node_to_include)
        {
            if (attributes->getNamedItem("optional"))
                node->parentNode()->removeChild(node);
            else if (throw_on_bad_incl)
                throw Poco::Exception(error_msg + name);
            else
                LOG_WARNING(log, "{}{}", error_msg, name);
        }
        else
        {
            Element & element = dynamic_cast<Element &>(*node);

            for (const auto & attr_name : SUBSTITUTION_ATTRS)
                element.removeAttribute(attr_name);

            if (replace)
            {
                while (Node * child = node->firstChild())
                    node->removeChild(child);

                element.removeAttribute("replace");
            }

            const NodeListPtr children = node_to_include->childNodes();
            for (size_t i = 0, size = children->length(); i < size; ++i)
            {
                NodePtr new_node = config->importNode(children->item(i), true);
                node->appendChild(new_node);
            }

            const NamedNodeMapPtr from_attrs = node_to_include->attributes();
            for (size_t i = 0, size = from_attrs->length(); i < size; ++i)
            {
                element.setAttributeNode(dynamic_cast<Attr *>(config->importNode(from_attrs->item(i), true)));
            }

            included_something = true;
        }
    };

    if (attr_nodes["incl"]) // we have include subst
    {
        auto get_incl_node = [&](const std::string & name)
        {
            return include_from ? include_from->getNodeByPath("yandex/" + name) : nullptr;
        };

        process_include(attr_nodes["incl"], get_incl_node, "Include not found: ");
    }

    if (attr_nodes["from_zk"]) /// we have zookeeper subst
    {
        contributing_zk_paths.insert(attr_nodes["from_zk"]->getNodeValue());

        if (zk_node_cache)
        {
            YMLDocumentPtr zk_document;
            auto get_zk_node = [&](const std::string & name) -> const Node *
            {
                zkutil::ZooKeeperNodeCache::ZNode znode = zk_node_cache->get(name, zk_changed_event);
                if (!znode.exists)
                    return nullptr;

                /// Enclose contents into a fake <from_zk> tag to allow pure text substitutions.
                zk_document = dom_parser.parseString("<from_zk>" + znode.contents + "</from_zk>");
                return getRootNode(zk_document.get());
            };

            process_include(attr_nodes["from_zk"], get_zk_node, "Could not get ZooKeeper node: ");
        }
    }

    if (attr_nodes["from_env"]) /// we have env subst
    {
        YMLDocumentPtr env_document;
        auto get_env_node = [&](const std::string & name) -> const Node *
        {
            const char * env_val = std::getenv(name.c_str());
            if (env_val == nullptr)
                return nullptr;

            env_document = dom_parser.parseString("<from_env>" + std::string{env_val} + "</from_env>");

            return getRootNode(env_document.get());
        };

        process_include(attr_nodes["from_env"], get_env_node, "Env variable is not set: ");
    }

    if (included_something)
        doIncludesRecursive(config, include_from, node, zk_node_cache, zk_changed_event, contributing_zk_paths);
    else
    {
        NodeListPtr children = node->childNodes();
        Node * child = nullptr;
        for (size_t i = 0; (child = children->item(i)); ++i)
            doIncludesRecursive(config, include_from, child, zk_node_cache, zk_changed_event, contributing_zk_paths);
    }
}

YAMLProcessor::Files YAMLProcessor::getConfigMergeFiles(const std::string & config_path)
{
    Files files;

    Poco::Path merge_dir_path(config_path);
    std::set<std::string> merge_dirs;

    /// Add path_to_config/config_name.d dir
    merge_dir_path.setExtension("d");
    merge_dirs.insert(merge_dir_path.toString());
    /// Add path_to_config/conf.d dir
    merge_dir_path.setBaseName("conf");
    merge_dirs.insert(merge_dir_path.toString());

    for (const std::string & merge_dir_name : merge_dirs)
    {
        Poco::File merge_dir(merge_dir_name);
        if (!merge_dir.exists() || !merge_dir.isDirectory())
            continue;

        for (Poco::DirectoryIterator it(merge_dir_name); it != Poco::DirectoryIterator(); ++it)
        {
            Poco::File & file = *it;
            Poco::Path path(file.path());
            std::string extension = path.getExtension();
            std::string base_name = path.getBaseName();

            // Skip non-config and temporary files
            if (file.isFile() && (extension == "yml" || extension == "yaml") && !startsWith(base_name, "."))
                files.push_back(file.path());
        }
    }

    std::sort(files.begin(), files.end());

    return files;
}

YMLDocumentPtr YAMLProcessor::processConfig(
    bool * has_zk_includes,
    zkutil::ZooKeeperNodeCache * zk_node_cache,
    const zkutil::EventPtr & zk_changed_event)
{
    YMLDocumentPtr config; 
    LOG_DEBUG(log, "Processing configuration file '{}'.", path);

    if (fs::exists(path))
    {   
        config = fopen(path, "r");
    }
    else
    {
        if (path == "config.yml" || path == "config.yaml")
        {
            auto resource = getResource("embedded.yaml");
            if (resource.empty())
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Configuration file {} doesn't exist and there is no embedded config", path);
            LOG_DEBUG(log, "There is no file '{}', will use embedded config.", path);
            config = fopen("embedded.yaml", "r");
        }
        else
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Configuration file {} doesn't exist", path);
    }

    std::vector<std::string> contributing_files;
    contributing_files.push_back(path);

    for (auto & merge_file : getConfigMergeFiles(path))
    {
        try
        {
            LOG_DEBUG(log, "Merging configuration file '{}'.", merge_file);

            
            YMLDocumentPtr with = fopen(merge_file, "r");
            merge(config, with);
            contributing_files.push_back(merge_file);
        }
        catch (Exception & e)
        {
            e.addMessage("while merging config '" + path + "' with '" + merge_file + "'");
            throw;
        }
        catch (Poco::Exception & e)
        {
            throw Poco::Exception("Failed to merge config with '" + merge_file + "': " + e.displayText());
        }
    }

    std::unordered_set<std::string> contributing_zk_paths;
    try
    {
        YAML::Node node = config->getNodeByPath("yandex/include_from");
        YMLDocumentPtr include_from;
        std::string include_from_path;
        if (node)
        {
            /// if we include_from env or zk.
            doIncludesRecursive(config, nullptr, node, zk_node_cache, zk_changed_event, contributing_zk_paths);
            include_from_path = node->innerText();
        }
        else
        {
            std::string default_path = "/etc/metrika.xml";
            if (Poco::File(default_path).exists())
                include_from_path = default_path;
        }
        if (!include_from_path.empty())
        {
            LOG_DEBUG(log, "Including configuration file '{}'.", include_from_path);

            contributing_files.push_back(include_from_path);
            include_from = dom_parser.parse(include_from_path);
        }

        doIncludesRecursive(config, include_from, getRootNode(config.get()), zk_node_cache, zk_changed_event, contributing_zk_paths);
    }
    catch (Exception & e)
    std::unordered_set<std::string> contributing_zk_paths;
    {
        e.addMessage("while preprocessing config '" + path + "'");
        throw;
    }
    catch (Poco::Exception & e)
    {
        throw Poco::Exception("Failed to preprocess config '" + path + "': " + e.displayText(), e);
    }

    if (has_zk_includes)
        *has_zk_includes = !contributing_zk_paths.empty();

    WriteBufferFromOwnString comment;
    comment <<     " This file was generated automatically.\n";
    comment << "     Do not edit it: it is likely to be discarded and generated again before it's read next time.\n";
    comment << "     Files used to generate this file:";
    for (const std::string & contributing_file : contributing_files)
    {
        comment << "\n       " << contributing_file;
    }
    if (zk_node_cache && !contributing_zk_paths.empty())
    {
        comment << "\n     ZooKeeper nodes used to generate this file:";
        for (const std::string & contributing_zk_path : contributing_zk_paths)
            comment << "\n       " << contributing_zk_path;
    }

    comment << "      ";
    NodePtr new_node = config->createTextNode("\n\n");
    config->insertBefore(new_node, config->firstChild());
    new_node = config->createComment(comment.str());
    config->insertBefore(new_node, config->firstChild());

    return config;
}

YAMLProcessor::LoadedConfig YAMLProcessor::loadConfig(bool allow_zk_includes)
{
    bool has_zk_includes;
    YMLDocumentPtr config_yml = processConfig(&has_zk_includes);

    if (has_zk_includes && !allow_zk_includes)
        throw Poco::Exception("Error while loading config '" + path + "': from_zk includes are not allowed!");

    //ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_yml));

    return LoadedConfig{configuration, has_zk_includes, /* loaded_from_preprocessed = */ false, config_xml, path};
}

YAMLProcessor::LoadedConfig YAMLProcessor::loadConfigWithZooKeeperIncludes(
        zkutil::ZooKeeperNodeCache & zk_node_cache,
        const zkutil::EventPtr & zk_changed_event,
        bool fallback_to_preprocessed)
{
    YMLDocumentPtr config_yml;
    bool has_zk_includes;
    bool processed_successfully = false;
    try
    {
        config_yml = processConfig(&has_zk_includes, &zk_node_cache, zk_changed_event);
        processed_successfully = true;
    }
    catch (const Poco::Exception & ex)
    {
        if (!fallback_to_preprocessed)
            throw;

        const auto * zk_exception = dynamic_cast<const Coordination::Exception *>(ex.nested());
        if (!zk_exception)
            throw;

        LOG_WARNING(log, "Error while processing from_zk config includes: {}. Config will be loaded from preprocessed file: {}", zk_exception->message(), preprocessed_path);

        //config_xml = dom_parser.parse(preprocessed_path);
    }

    //ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));

    return LoadedConfig{configuration, has_zk_includes, !processed_successfully, config_xml, path};
}

void YAMLProcessor::savePreprocessedConfig(const LoadedConfig & loaded_config, std::string preprocessed_dir)
{
    try
    {
        if (preprocessed_path.empty())
        {
            fs::path preprocessed_configs_path("preprocessed_configs/");
            auto new_path = loaded_config.config_path;
            if (new_path.substr(0, main_config_path.size()) == main_config_path)
                new_path.replace(0, main_config_path.size(), "");
            std::replace(new_path.begin(), new_path.end(), '/', '_');

            if (preprocessed_dir.empty())
            {
                if (!loaded_config.configuration->has("path"))
                {
                    // Will use current directory
                    auto parent_path = Poco::Path(loaded_config.config_path).makeParent();
                    preprocessed_dir = parent_path.toString();
                    Poco::Path poco_new_path(new_path);
                    poco_new_path.setBaseName(poco_new_path.getBaseName() + PREPROCESSED_SUFFIX);
                    new_path = poco_new_path.toString();
                }
                else
                {
                    fs::path loaded_config_path(loaded_config.configuration->getString("path"));
                    preprocessed_dir = loaded_config_path / preprocessed_configs_path;
                }
            }
            else
            {
                fs::path preprocessed_dir_path(preprocessed_dir);
                preprocessed_dir = (preprocessed_dir_path / preprocessed_configs_path).string();
            }

            preprocessed_path = (fs::path(preprocessed_dir) / fs::path(new_path)).string();
            auto preprocessed_path_parent = Poco::Path(preprocessed_path).makeParent();
            if (!preprocessed_path_parent.toString().empty())
                Poco::File(preprocessed_path_parent).createDirectories();
        }
        DOMWriter().writeNode(preprocessed_path, loaded_config.preprocessed_xml);
        LOG_DEBUG(log, "Saved preprocessed configuration to '{}'.", preprocessed_path);
    }
    catch (Poco::Exception & e)
    {
        LOG_WARNING(log, "Couldn't save preprocessed config to {}: {}", preprocessed_path, e.displayText());
    }
}

void YAMLProcessor::setConfigPath(const std::string & config_path)
{
    main_config_path = config_path;
}

}
