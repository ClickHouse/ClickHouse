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
					auto dict_ptr = DictionaryFactory::instance().create(*config, prefix, context);
					external_dictionaries.emplace(name, std::make_shared<MultiVersion<IDictionary>>(dict_ptr.release()));
				}
				else
				{
					auto & current = it->second->get();
					if (current->isComplete())
					{
						/// @todo check that timeout has passed
						auto dict_ptr = DictionaryFactory::instance().create(*config, prefix, context);
						it->second->set(dict_ptr.release());
					}
					else
						const_cast<IDictionary *>(current.get())->reload();
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
		for (auto & dictionary : external_dictionaries)
		{
			try
			{
				auto & current = dictionary.second->get();
				if (current->isComplete())
				{
					/// @todo check that timeout has passed and load new version
				}
				else
				{
					const_cast<IDictionary *>(current.get())->reload();
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
