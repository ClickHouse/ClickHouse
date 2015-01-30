#include <DB/Interpreters/Dictionaries.h>
#include <DB/Dictionaries/DictionaryFactory.h>
#include <DB/Dictionaries/config_ptr_t.h>

namespace DB
{

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
			try
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

				const auto & lifetime_key = prefix + "lifetime";
				const auto & lifetime_min_key = lifetime_key + ".min";
				const auto has_min = config->has(lifetime_min_key);
				const auto min_update_time = has_min ? config->getInt(lifetime_min_key) : config->getInt(lifetime_key);
				const auto max_update_time = has_min ? config->getInt(lifetime_key + ".max") : min_update_time;

				std::cout << "min_update_time = " << min_update_time << " max_update_time = " << max_update_time << std::endl;

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
			catch (...)
			{
				handleException();
			}
		}
	}
	else
	{
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

					auto new_version = current->clone();
					dictionary.second->set(new_version.release());
				}
			}
			catch (...)
			{
				handleException();
			}
		}
	}
}

}
