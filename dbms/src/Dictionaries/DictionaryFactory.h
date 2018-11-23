#pragma once

#include "IDictionary.h"
//#include <Poco/Util/AbstractConfiguration.h>
#include <ext/singleton.h>

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

class DictionaryFactory : public ext::singleton<DictionaryFactory>
{
public:
    DictionaryPtr create(const std::string & name, const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix, Context & context) const;

    using Creator = std::function<DictionaryPtr(
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        //Block & sample_block,
        Context & context)>;

    //DictionaryFactory();

    void registerLayout(const std::string & layout_type, Creator create_source);

    DictionarySourcePtr create(
        const std::string & name, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
        const DictionaryStructure & dict_struct, Context & context) const;

private:
    using LayoutRegistry = std::unordered_map<std::string, Creator>;
    LayoutRegistry registered_layouts;
};

}
