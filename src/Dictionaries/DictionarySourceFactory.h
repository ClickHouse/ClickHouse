#pragma once

#include <Dictionaries/IDictionarySource.h>
#include <Interpreters/Context_fwd.h>

#include <unordered_map>
#include <boost/noncopyable.hpp>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}

class Logger;
}

namespace DB
{

class Block;
struct DictionaryStructure;

/// creates IDictionarySource instance from config and DictionaryStructure
class DictionarySourceFactory : private boost::noncopyable
{
public:
    static DictionarySourceFactory & instance();

    /// 'default_database' - the database where dictionary itself was created.
    /// It is used as default_database for ClickHouse dictionary source when no explicit database was specified.
    /// Does not make sense for other sources.
    using Creator = std::function<DictionarySourcePtr(
        const String & name,
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        Block & sample_block,
        ContextPtr global_context,
        const std::string & default_database,
        bool check_config)>;

    DictionarySourceFactory();

    void registerSource(const std::string & source_type, Creator create_source);

    DictionarySourcePtr create(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const DictionaryStructure & dict_struct,
        ContextPtr global_context,
        const std::string & default_database,
        bool check_config) const;

    /// Checks that a specified source exists and available for the current user.
    void checkSourceAvailable(const std::string & source_type, const std::string & dictionary_name, const ContextPtr & context) const;

private:
    using SourceRegistry = std::unordered_map<std::string, Creator>;
    SourceRegistry registered_sources;

    LoggerPtr log;
};

}
