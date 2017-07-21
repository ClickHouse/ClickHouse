#pragma once

#include <Dictionaries/IDictionarySource.h>
#include <ext/singleton.h>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{

class Context;
struct DictionaryStructure;

/// creates IDictionarySource instance from config and DictionaryStructure
class DictionarySourceFactory : public ext::singleton<DictionarySourceFactory>
{
public:
    DictionarySourceFactory();

    DictionarySourcePtr create(
        const std::string & name, Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
        const DictionaryStructure & dict_struct, Context & context) const;
};

}
