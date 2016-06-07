#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

class Context;

class DictionaryFactory : public Singleton<DictionaryFactory>
{
public:
	DictionaryPtr create(const std::string & name, Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix, Context & context) const;
};

}
