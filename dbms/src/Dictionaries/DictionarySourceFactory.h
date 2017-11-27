#pragma once

#include <Dictionaries/IDictionarySource.h>
#include <ext/singleton.h>

#include <unordered_map>

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

class Context;
struct DictionaryStructure;

/// creates IDictionarySource instance from config and DictionaryStructure
class DictionarySourceFactory : public ext::singleton<DictionarySourceFactory>
{
public:
    using Creator = std::function<DictionarySourcePtr(
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        Block & sample_block,
        const Context & context)>;

    DictionarySourceFactory();

    void registerSource(const std::string & source_type, Creator create_source);

    DictionarySourcePtr create(
        const std::string & name, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
        const DictionaryStructure & dict_struct, Context & context) const;

private:
    using SourceRegistry = std::unordered_map<std::string, Creator>;
    SourceRegistry registered_sources;

    Poco::Logger * log;
};

}
