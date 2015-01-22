#include <DB/Interpreters/Dictionaries.h>
#include <DB/Dictionaries/DictionaryFactory.h>
#include <Poco/Util/XMLConfiguration.h>


namespace DB
{

namespace
{
	template <typename T> struct release
	{
		void operator()(const T * const ptr) { ptr->release(); }
	};
	template <typename T> using config_ptr_t = std::unique_ptr<T, release<T>>;
};

void Dictionaries::reloadExternalDictionaries()
{
	const auto config_path = Poco::Util::Application::instance().config().getString("dictionaries_config");
	if (config_path.empty())
		return;

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
			auto dict_ptr = DictionaryFactory::instance().create(*config, prefix, context);
			const auto it = external_dictionaries.find(name);
			if (it == std::end(external_dictionaries))
				external_dictionaries.emplace(name, std::make_shared<MultiVersion<IDictionary>>(dict_ptr.release()));
			else
				it->second->set(dict_ptr.release());
		}
		catch (const Exception &)
		{
			handleException();
		}
	}
};

}
