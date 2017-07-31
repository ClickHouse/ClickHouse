#pragma once

#include <Dictionaries/IDictionary.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <ext/singleton.h>


namespace DB
{

class Context;

class DictionaryFactory : public ext::singleton<DictionaryFactory>
{
public:
    DictionaryPtr create(const std::string & name, Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix, Context & context) const;
};

}
