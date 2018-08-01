#pragma once

#include <string>
#include <unordered_set>
#include <vector>

#include <Poco/DOM/Document.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/DOM/DOMWriter.h>
#include <Poco/DOM/NodeList.h>
#include <Poco/DOM/NamedNodeMap.h>
#include <Poco/AutoPtr.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <common/logger_useful.h>


namespace zkutil
{
    class ZooKeeperNodeCache;
}

using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;
using XMLDocumentPtr = Poco::AutoPtr<Poco::XML::Document>;

class ConfigProcessor
{
public:
    using Substitutions = std::vector<std::pair<std::string, std::string>>;

    /// Set log_to_console to true if the logging subsystem is not initialized yet.
    explicit ConfigProcessor(
        const std::string & path,
        bool throw_on_bad_incl = false,
        bool log_to_console = false,
        const Substitutions & substitutions = Substitutions());

    ~ConfigProcessor();

    /// Perform config includes and substitutions and return the resulting XML-document.
    ///
    /// Suppose path is "/path/file.xml"
    /// 1) Merge XML trees of /path/file.xml with XML trees of all files from /path/{conf,file}.d/*.{conf,xml}
    ///    * If an element has a "replace" attribute, replace the matching element with it.
    ///    * If an element has a "remove" attribute, remove the matching element.
    ///    * Else, recursively merge child elements.
    /// 2) Determine the includes file from the config: <include_from>/path2/metrika.xml</include_from>
    ///    If this path is not configured, use /etc/metrika.xml
    /// 3) Replace elements matching the "<foo incl="bar"/>" pattern with
    ///    "<foo>contents of the yandex/bar element in metrika.xml</foo>"
    /// 4) If zk_node_cache is non-NULL, replace elements matching the "<foo from_zk="/bar">" pattern with
    ///    "<foo>contents of the /bar ZooKeeper node</foo>".
    ///    If has_zk_includes is non-NULL and there are such elements, set has_zk_includes to true.
    /// 5) (Yandex.Metrika-specific) Substitute "<layer/>" with "<layer>layer number from the hostname</layer>".
    XMLDocumentPtr processConfig(
        bool * has_zk_includes = nullptr,
        zkutil::ZooKeeperNodeCache * zk_node_cache = nullptr);


    /// loadConfig* functions apply processConfig and create Poco::Util::XMLConfiguration.
    /// The resulting XML document is saved into a file with the name
    /// resulting from adding "-preprocessed" suffix to the path file name.
    /// E.g., config.xml -> config-preprocessed.xml

    struct LoadedConfig
    {
        ConfigurationPtr configuration;
        bool has_zk_includes;
        bool loaded_from_preprocessed;
        XMLDocumentPtr preprocessed_xml;
    };

    /// If allow_zk_includes is true, expect that the configuration XML can contain from_zk nodes.
    /// If it is the case, set has_zk_includes to true and don't write config-preprocessed.xml,
    /// expecting that config would be reloaded with zookeeper later.
    LoadedConfig loadConfig(bool allow_zk_includes = false);

    /// If fallback_to_preprocessed is true, then if KeeperException is thrown during config
    /// processing, load the configuration from the preprocessed file.
    LoadedConfig loadConfigWithZooKeeperIncludes(
        zkutil::ZooKeeperNodeCache & zk_node_cache,
        bool fallback_to_preprocessed = false);

    void savePreprocessedConfig(const LoadedConfig & loaded_config);

public:
    using Files = std::vector<std::string>;

    static Files getConfigMergeFiles(const std::string & config_path);

    /// Is the file named as result of config preprocessing, not as original files.
    static bool isPreprocessedFile(const std::string & config_path);

    static inline const auto SUBSTITUTION_ATTRS = {"incl", "from_zk", "from_env"};

private:
    const std::string path;
    const std::string preprocessed_path;

    bool throw_on_bad_incl;

    Logger * log;
    Poco::AutoPtr<Poco::Channel> channel_ptr;

    Substitutions substitutions;

    Poco::AutoPtr<Poco::XML::NamePool> name_pool;
    Poco::XML::DOMParser dom_parser;

private:
    using NodePtr = Poco::AutoPtr<Poco::XML::Node>;

    void mergeRecursive(XMLDocumentPtr config, Poco::XML::Node * config_node, const Poco::XML::Node * with_node);

    void merge(XMLDocumentPtr config, XMLDocumentPtr with);

    std::string layerFromHost();

    void doIncludesRecursive(
            XMLDocumentPtr config,
            XMLDocumentPtr include_from,
            Poco::XML::Node * node,
            zkutil::ZooKeeperNodeCache * zk_node_cache,
            std::unordered_set<std::string> & contributing_zk_paths);
};
