#pragma once

#include <DB/Dictionaries/DictionarySourceFactory.h>
#include <DB/Dictionaries/FlatDictionary.h>
#include <DB/Dictionaries/HashedDictionary.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <Yandex/singleton.h>
#include <statdaemons/ext/memory.hpp>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

class DictionaryFactory : public Singleton<DictionaryFactory>
{
public:
	DictionaryPtr create(const std::string & name, Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix,
		const Context & context) const
	{
		auto dict_struct = DictionaryStructure::fromConfig(config, config_prefix + "structure");

		auto source_ptr = DictionarySourceFactory::instance().create(
			config, config_prefix + "source.", dict_struct, context);

		const auto & layout_prefix = config_prefix + "layout.";

		if (config.has(layout_prefix + "flat"))
		{
			return ext::make_unique<FlatDictionary>(name, dict_struct, std::move(source_ptr));
		}
		else if (config.has(layout_prefix + "hashed"))
		{
			return ext::make_unique<HashedDictionary>(name, dict_struct, std::move(source_ptr));
		}
		else if (config.has(layout_prefix + "cache"))
		{
			const auto size = config.getInt(layout_prefix + "cache.size", 0);
			if (size == 0)
				throw Exception{
					"Dictionary of type 'cache' cannot have size of 0 bytes",
					ErrorCodes::TOO_SMALL_BUFFER_SIZE
				};

			throw Exception{
				"Dictionary of type 'cache' is not yet implemented",
				ErrorCodes::NOT_IMPLEMENTED
			};
		}

		throw Exception{"No dictionary type specified", ErrorCodes::BAD_ARGUMENTS};
	}
};

}
