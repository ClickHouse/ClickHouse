#include "ConfigProcessor.h"

#include <sys/utsname.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <iostream>
#include <functional>
#include <Poco/DOM/Text.h>
#include <Poco/DOM/Attr.h>
#include <Poco/DOM/Comment.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/StringUtils/StringUtils.h>

#define PREPROCESSED_SUFFIX "-preprocessed"


using namespace Poco::XML;


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

static std::string preprocessedConfigPath(const std::string & path)
{
    Poco::Path preprocessed_path(path);
    preprocessed_path.setBaseName(preprocessed_path.getBaseName() + PREPROCESSED_SUFFIX);
    return preprocessed_path.toString();
}

bool ConfigProcessor::isPreprocessedFile(const std::string & path)
{
    return endsWith(Poco::Path(path).getBaseName(), PREPROCESSED_SUFFIX);
}


ConfigProcessor::ConfigProcessor(
    const std::string & path_,
    bool throw_on_bad_incl_,
    bool log_to_console,
    const Substitutions & substitutions_)
    : path(path_)
    , preprocessed_path(preprocessedConfigPath(path))
    , throw_on_bad_incl(throw_on_bad_incl_)
    , substitutions(substitutions_)
    /// We need larger name pool to allow to support vast amount of users in users.xml files for ClickHouse.
    /// Size is prime because Poco::XML::NamePool uses bad (inefficient, low quality)
    ///  hash function internally, and its size was prime by default.
    , name_pool(new Poco::XML::NamePool(65521))
    , dom_parser(name_pool)
{
    if (log_to_console && Logger::has("ConfigProcessor") == nullptr)
    {
        channel_ptr = new Poco::ConsoleChannel;
        log = &Logger::create("ConfigProcessor", channel_ptr.get(), Poco::Message::PRIO_TRACE);
    }
    else
    {
        log = &Logger::get("ConfigProcessor");
    }
}

ConfigProcessor::~ConfigProcessor()
{
    if (channel_ptr) /// This means we have created a new console logger in the constructor.
        Logger::destroy("ConfigProcessor");
}


/// Vector containing the name of the element and a sorted list of attribute names and values
/// (except "remove" and "replace" attributes).
/// Serves as a unique identifier of the element contents for comparison.
using ElementIdentifier = std::vector<std::string>;

using NamedNodeMapPtr = Poco::AutoPtr<Poco::XML::NamedNodeMap>;
/// NOTE getting rid of iterating over the result of Node.childNodes() call is a good idea
/// because accessing the i-th element of this list takes O(i) time.
using NodeListPtr = Poco::AutoPtr<Poco::XML::NodeList>;

static ElementIdentifier getElementIdentifier(Node * element)
{
    const NamedNodeMapPtr attrs = element->attributes();
    std::vector<std::pair<std::string, std::string>> attrs_kv;
    for (size_t i = 0, size = attrs->length(); i < size; ++i)
    {
        const Node * node = attrs->item(i);
        std::string name = node->nodeName();
        auto subst_name_pos = std::find(ConfigProcessor::SUBSTITUTION_ATTRS.begin(), ConfigProcessor::SUBSTITUTION_ATTRS.end(), name);
        if (name == "replace" || name == "remove" ||
            subst_name_pos != ConfigProcessor::SUBSTITUTION_ATTRS.end())
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

    return res;
}

static Node * getRootNode(Document * document)
{
    const NodeListPtr children = document->childNodes();
    for (size_t i = 0, size = children->length(); i < size; ++i)
    {
        Node * child = children->item(i);
        /// Besides the root element there can be comment nodes on the top level.
        /// Skip them.
        if (child->nodeType() == Node::ELEMENT_NODE)
            return child;
    }

    throw Poco::Exception("No root node in document");
}

static bool allWhitespace(const std::string & s)
{
    return s.find_first_not_of(" \t\n\r") == std::string::npos;
}

void ConfigProcessor::mergeRecursive(XMLDocumentPtr config, Node * config_root, const Node * with_root)
{
    const NodeListPtr with_nodes = with_root->childNodes();
    using ElementsByIdentifier = std::multimap<ElementIdentifier, Node *>;
    ElementsByIdentifier config_element_by_id;
    for (Node * node = config_root->firstChild(); node;)
    {
        Node * next_node = node->nextSibling();
        /// Remove text from the original config node.
        if (node->nodeType() == Node::TEXT_NODE && !allWhitespace(node->getNodeValue()))
        {
            config_root->removeChild(node);
        }
        else if (node->nodeType() == Node::ELEMENT_NODE)
        {
            config_element_by_id.insert(ElementsByIdentifier::value_type(getElementIdentifier(node), node));
        }
        node = next_node;
    }

    for (size_t i = 0, size = with_nodes->length(); i < size; ++i)
    {
        Node * with_node = with_nodes->item(i);

        bool merged = false;
        bool remove = false;
        if (with_node->nodeType() == Node::ELEMENT_NODE)
        {
            Element & with_element = dynamic_cast<Element &>(*with_node);
            remove = with_element.hasAttribute("remove");
            bool replace = with_element.hasAttribute("replace");

            if (remove && replace)
                throw Poco::Exception("both remove and replace attributes set for element <" + with_node->nodeName() + ">");

            ElementsByIdentifier::iterator it = config_element_by_id.find(getElementIdentifier(with_node));

            if (it != config_element_by_id.end())
            {
                Node * config_node = it->second;
                config_element_by_id.erase(it);

                if (remove)
                {
                    config_root->removeChild(config_node);
                }
                else if (replace)
                {
                    with_element.removeAttribute("replace");
                    NodePtr new_node = config->importNode(with_node, true);
                    config_root->replaceChild(new_node, config_node);
                }
                else
                {
                    mergeRecursive(config, config_node, with_node);
                }
                merged = true;
            }
        }
        if (!merged && !remove)
        {
            NodePtr new_node = config->importNode(with_node, true);
            config_root->appendChild(new_node);
        }
    }
}

void ConfigProcessor::merge(XMLDocumentPtr config, XMLDocumentPtr with)
{
    mergeRecursive(config, getRootNode(&*config), getRootNode(&*with));
}

std::string ConfigProcessor::layerFromHost()
{
    utsname buf;
    if (uname(&buf))
        throw Poco::Exception(std::string("uname failed: ") + std::strerror(errno));

    std::string layer = numberFromHost(buf.nodename);
    if (layer.empty())
        throw Poco::Exception(std::string("no layer in host name: ") + buf.nodename);

    return layer;
}

void ConfigProcessor::doIncludesRecursive(
        XMLDocumentPtr config,
        XMLDocumentPtr include_from,
        Node * node,
        zkutil::ZooKeeperNodeCache * zk_node_cache,
        std::unordered_set<std::string> & contributing_zk_paths)
{
    if (node->nodeType() == Node::TEXT_NODE)
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
    if ( node->nodeName() == "layer" &&
        !node->hasAttributes() &&
        !node->hasChildNodes() &&
         node->nodeValue().empty())
    {
        NodePtr new_node = config->createTextNode(layerFromHost());
        node->appendChild(new_node);
        return;
    }

    std::map<std::string, const Node *> attr_nodes;
    NamedNodeMapPtr attributes = node->attributes();
    size_t substs_count = 0;
    for (const auto & attr_name : SUBSTITUTION_ATTRS)
    {
        auto subst = attributes->getNamedItem(attr_name);
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
        std::string name = include_attr->getNodeValue();
        const Node * node_to_include = get_node(name);
        if (!node_to_include)
        {
            if (attributes->getNamedItem("optional"))
                node->parentNode()->removeChild(node);
            else if (throw_on_bad_incl)
                throw Poco::Exception(error_msg + name);
            else
                LOG_WARNING(log, error_msg << name);
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
            XMLDocumentPtr zk_document;
            auto get_zk_node = [&](const std::string & name) -> const Node *
            {
                std::optional<std::string> contents = zk_node_cache->get(name);
                if (!contents)
                    return nullptr;

                /// Enclose contents into a fake <from_zk> tag to allow pure text substitutions.
                zk_document = dom_parser.parseString("<from_zk>" + *contents + "</from_zk>");
                return getRootNode(zk_document.get());
            };

            process_include(attr_nodes["from_zk"], get_zk_node, "Could not get ZooKeeper node: ");
        }
    }

    if (attr_nodes["from_env"]) /// we have env subst
    {
        XMLDocumentPtr env_document;
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
        doIncludesRecursive(config, include_from, node, zk_node_cache, contributing_zk_paths);
    else
    {
        NodeListPtr children = node->childNodes();
        Node * child = nullptr;
        for (size_t i = 0; (child = children->item(i)); ++i)
            doIncludesRecursive(config, include_from, child, zk_node_cache, contributing_zk_paths);
    }
}

ConfigProcessor::Files ConfigProcessor::getConfigMergeFiles(const std::string & config_path)
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
    /// Add path_to_config/config.d dir
    merge_dir_path.setBaseName("config");
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
            if (file.isFile() && (extension == "xml" || extension == "conf") && !startsWith(base_name, "."))
                files.push_back(file.path());
        }
    }

    std::sort(files.begin(), files.end());

    return files;
}

XMLDocumentPtr ConfigProcessor::processConfig(
    bool * has_zk_includes,
    zkutil::ZooKeeperNodeCache * zk_node_cache)
{
    XMLDocumentPtr config = dom_parser.parse(path);

    std::vector<std::string> contributing_files;
    contributing_files.push_back(path);

    for (auto & merge_file : getConfigMergeFiles(path))
    {
        try
        {
            XMLDocumentPtr with = dom_parser.parse(merge_file);
            merge(config, with);
            contributing_files.push_back(merge_file);
        }
        catch (Poco::Exception & e)
        {
            throw Poco::Exception("Failed to merge config with '" + merge_file + "': " + e.displayText());
        }
    }

    std::unordered_set<std::string> contributing_zk_paths;
    try
    {
        Node * node = config->getNodeByPath("yandex/include_from");
        XMLDocumentPtr include_from;
        std::string include_from_path;
        if (node)
        {
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
            contributing_files.push_back(include_from_path);
            include_from = dom_parser.parse(include_from_path);
        }

        doIncludesRecursive(config, include_from, getRootNode(config.get()), zk_node_cache, contributing_zk_paths);
    }
    catch (Poco::Exception & e)
    {
        throw Poco::Exception("Failed to preprocess config '" + path + "': " + e.displayText(), e);
    }

    if (has_zk_includes)
        *has_zk_includes = !contributing_zk_paths.empty();

    std::stringstream comment;
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

ConfigProcessor::LoadedConfig ConfigProcessor::loadConfig(bool allow_zk_includes)
{
    bool has_zk_includes;
    XMLDocumentPtr config_xml = processConfig(&has_zk_includes);

    if (has_zk_includes && !allow_zk_includes)
        throw Poco::Exception("Error while loading config `" + path + "': from_zk includes are not allowed!");

    ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));

    return LoadedConfig{configuration, has_zk_includes, /* loaded_from_preprocessed = */ false, config_xml};
}

ConfigProcessor::LoadedConfig ConfigProcessor::loadConfigWithZooKeeperIncludes(
        zkutil::ZooKeeperNodeCache & zk_node_cache,
        bool fallback_to_preprocessed)
{
    XMLDocumentPtr config_xml;
    bool has_zk_includes;
    bool processed_successfully = false;
    try
    {
        config_xml = processConfig(&has_zk_includes, &zk_node_cache);
        processed_successfully = true;
    }
    catch (const Poco::Exception & ex)
    {
        if (!fallback_to_preprocessed)
            throw;

        const auto * zk_exception = dynamic_cast<const zkutil::KeeperException *>(ex.nested());
        if (!zk_exception)
            throw;

        LOG_WARNING(
                log,
                "Error while processing from_zk config includes: " + zk_exception->message() +
                ". Config will be loaded from preprocessed file: " + preprocessed_path);

        config_xml = dom_parser.parse(preprocessed_path);
    }

    ConfigurationPtr configuration(new Poco::Util::XMLConfiguration(config_xml));

    return LoadedConfig{configuration, has_zk_includes, !processed_successfully, config_xml};
}

void ConfigProcessor::savePreprocessedConfig(const LoadedConfig & loaded_config)
{
    try
    {
        DOMWriter().writeNode(preprocessed_path, loaded_config.preprocessed_xml);
    }
    catch (Poco::Exception & e)
    {
        LOG_WARNING(log, "Couldn't save preprocessed config to " << preprocessed_path << ": " << e.displayText());
    }
}
