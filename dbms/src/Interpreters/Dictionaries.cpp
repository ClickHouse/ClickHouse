#include <DB/Interpreters/Dictionaries.h>
#include <DB/Dictionaries/DictionaryFactory.h>
#include <DB/Dictionaries/config_ptr_t.h>


namespace DB
{

namespace
{
	std::string findKeyForDictionary(Poco::Util::XMLConfiguration & config, const std::string & name)
	{
		Poco::Util::AbstractConfiguration::Keys keys;
		config.keys(keys);

		for (const auto & key : keys)
		{
			if (0 != strncmp(key.data(), "dictionary", strlen("dictionary")))
				continue;

			if (name == config.getString(key + ".name"))
				return key;
		}

		return {};
	}
}

void Dictionaries::reloadExternals()
{
	const std::lock_guard<std::mutex> lock{externals_mutex};

	const auto config_path = Poco::Util::Application::instance().config().getString("dictionaries_config");
	if (config_path.empty())
		return;

	const auto last_modified = Poco::File{config_path}.getLastModified();
	if (last_modified > dictionaries_last_modified)
	{
		dictionaries_last_modified = last_modified;

		const config_ptr_t<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration{config_path}};

		/// get all dictionaries' definitions
		Poco::Util::AbstractConfiguration::Keys keys;
		config->keys(keys);

		/// for each dictionary defined in xml config
		for (const auto & key : keys)
		{
			if (0 != strncmp(key.data(), "dictionary", strlen("dictionary")))
			{
				LOG_WARNING(log, "unknown node in dictionaries file: '" + key + "', 'dictionary'");
				continue;
			}

			const auto & prefix = key + '.';

			const auto & name = config->getString(prefix + "name");
			if (name.empty())
			{
				LOG_WARNING(log, "dictionary name cannot be empty");
				continue;
			}

			try
			{
				auto it = external_dictionaries.find(name);
				if (it == std::end(external_dictionaries))
				{
					/// such a dictionary is not present at the moment
					auto dict_ptr = DictionaryFactory::instance().create(name, *config, prefix, context);
					external_dictionaries.emplace(name, std::make_shared<MultiVersion<IDictionary>>(dict_ptr.release()));
				}
				else
				{
					/// dictionary exists, it may be desirable to reload it
					auto & current = it->second->get();
					if (current->isCached())
						const_cast<IDictionary *>(current.get())->reload();
					else
					{
						/// @todo check that timeout has passed
						auto dict_ptr = DictionaryFactory::instance().create(name, *config, prefix, context);
						it->second->set(dict_ptr.release());
					}
				}
			}
			catch (const Exception &)
			{
				handleException();
			}
		}
	}
	else
	{
		config_ptr_t<Poco::Util::XMLConfiguration> config;
		for (auto & dictionary : external_dictionaries)
		{
			try
			{
				auto current = dictionary.second->get();
				if (current->isCached())
				{
					const_cast<IDictionary *>(current.get())->reload();
				}
				else
				{
					/// @todo check that timeout has passed and load new version
					if (!current->getSource()->isModified())
						continue;

					/// source has supposedly been modified, load it over again
					if (!config)
						config.reset(new Poco::Util::XMLConfiguration{config_path});

					const auto & name = current->getName();
					const auto & key = findKeyForDictionary(*config, name);
					if (!key.empty())
					{
						auto dict_ptr = DictionaryFactory::instance().create(name, *config, key + '.', context);
						dictionary.second->set(dict_ptr.release());
					}
				}
			}
			catch (const Exception &)
			{
				handleException();
			}
		}
	}
}

}
