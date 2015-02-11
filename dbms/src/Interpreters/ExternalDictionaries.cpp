#include <DB/Interpreters/ExternalDictionaries.h>
#include <DB/Dictionaries/DictionaryFactory.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <statdaemons/ext/scope_guard.hpp>
#include <Poco/Util/Application.h>

namespace DB
{

namespace
{
	std::string getDictionariesConfigPath(const Poco::Util::AbstractConfiguration & config)
	{
		const auto path = config.getString("dictionaries_config", "");
		if (path.empty())
			return path;

		if (path[0] != '/')
		{
			const auto app_config_path = config.getString("config-file", "config.xml");
			const auto config_dir = Poco::Path{app_config_path}.parent().toString();
			const auto absolute_path = config_dir + path;
			if (Poco::File{absolute_path}.exists())
				return absolute_path;
		}

		return path;
	}
}

void ExternalDictionaries::reloadImpl()
{
	const auto config_path = getDictionariesConfigPath(Poco::Util::Application::instance().config());
	const Poco::File config_file{config_path};

	if (config_path.empty() || !config_file.exists())
	{
		LOG_WARNING(log, "config file '" + config_path + "' does not exist");
	}
	else
	{
		const auto last_modified = config_file.getLastModified();
		if (last_modified > config_last_modified)
		{
			/// definitions of dictionaries may have changed, recreate all of them
			config_last_modified = last_modified;

			const auto config = new Poco::Util::XMLConfiguration{config_path};
			SCOPE_EXIT(
				config->release();
			);

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

					const auto name = config->getString(key + ".name");
					if (name.empty())
					{
						LOG_WARNING(log, "dictionary name cannot be empty");
						continue;
					}

					auto dict_ptr = DictionaryFactory::instance().create(name, *config, key, context);
					if (!dict_ptr->isCached())
					{
						const auto & lifetime = dict_ptr->getLifetime();
						if (lifetime.min_sec != 0 && lifetime.max_sec != 0)
						{
							std::uniform_int_distribution<std::uint64_t> distribution{
								lifetime.min_sec,
								lifetime.max_sec
							};
							update_times[name] = std::chrono::system_clock::now() +
								std::chrono::seconds{distribution(rnd_engine)};
						}
					}

					auto it = dictionaries.find(name);
					/// add new dictionary or update an existing version
					if (it == std::end(dictionaries))
					{
						const std::lock_guard<std::mutex> lock{dictionaries_mutex};
						dictionaries.emplace(name, std::make_shared<MultiVersion<IDictionary>>(dict_ptr.release()));
					}
					else
						it->second->set(dict_ptr.release());
				}
				catch (...)
				{
					handleException();
				}
			}
		}
	}

	/// periodic update
	for (auto & dictionary : dictionaries)
	{
		try
		{
			auto current = dictionary.second->get();
			const auto & lifetime = current->getLifetime();

			/// do not update dictionaries with zero as lifetime
			if (lifetime.min_sec == 0 || lifetime.max_sec == 0)
				continue;

			/// update only non-cached dictionaries
			if (!current->isCached())
			{
				auto & update_time = update_times[current->getName()];

				/// check that timeout has passed
				if (std::chrono::system_clock::now() < update_time)
					continue;

				SCOPE_EXIT(
					/// calculate next update time
					std::uniform_int_distribution<std::uint64_t> distribution{lifetime.min_sec, lifetime.max_sec};
					update_time = std::chrono::system_clock::now() + std::chrono::seconds{distribution(rnd_engine)};
				);

				/// check source modified
				if (current->getSource()->isModified())
				{
					/// create new version of dictionary
					auto new_version = current->clone();
					dictionary.second->set(new_version.release());
				}
			}
		}
		catch (...)
		{
			handleException();
		}
	}
}

}
