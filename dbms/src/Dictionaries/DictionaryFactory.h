#pragma once

#include <ext/singleton.h>
#include "IDictionary.h"


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
    DictionaryPtr create(const std::string & name, const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, Context & context) const;

    using Creator = std::function<DictionaryPtr(
        const std::string & name,
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        DictionarySourcePtr source_ptr)>;

    void registerLayout(const std::string & layout_type, Creator create_layout);

private:
    using LayoutRegistry = std::unordered_map<std::string, Creator>;
    LayoutRegistry registered_layouts;
};

}
