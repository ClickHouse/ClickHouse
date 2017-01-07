#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <common/singleton.h>


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
class DictionarySourceFactory : public Singleton<DictionarySourceFactory>
{
public:
    DictionarySourceFactory();

	DictionarySourcePtr create(
		const std::string & name, Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
		const DictionaryStructure & dict_struct, Context & context) const;
};

}
