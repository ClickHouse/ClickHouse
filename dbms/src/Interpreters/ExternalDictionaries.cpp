#include <DB/Interpreters/ExternalDictionaries.h>
#include <DB/Dictionaries/DictionaryFactory.h>
#include <DB/Dictionaries/DictionaryStructure.h>
#include <DB/Dictionaries/IDictionarySource.h>
#include <statdaemons/ext/scope_guard.hpp>
#include <Poco/Util/Application.h>
#include <Poco/Glob.h>

namespace DB
{

namespace
{
	std::set<std::string> getDictionariesConfigPaths(const Poco::Util::AbstractConfiguration & config)
	{
		auto pattern = config.getString("dictionaries_config", "");
		if (pattern.empty())
			return {};

		std::set<std::string> files;
		if (pattern[0] != '/')
		{
			const auto app_config_path = config.getString("config-file", "config.xml");
			const auto config_dir = Poco::Path{app_config_path}.parent().toString();
			const auto absolute_path = config_dir + pattern;
			Poco::Glob::glob(absolute_path, files, 0);
			if (!files.empty())
				return files;
		}

		Poco::Glob::glob(pattern, files, 0);

		return files;
	}
}

void ExternalDictionaries::reloadImpl(const bool throw_on_error)
{
	const auto config_paths = getDictionariesConfigPaths(Poco::Util::Application::instance().config());

	for (const auto & config_path : config_paths)
		reloadFromFile(config_path, throw_on_error);

	/// periodic update
	for (auto & dictionary : dictionaries)
	{
		const auto & name = dictionary.first;

		try
		{
			auto current = dictionary.second.first->get();
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
					dictionary.second.first->set(new_version.release());
				}
			}

			/// erase stored exception on success
			stored_exceptions.erase(name);
		}
		catch (...)
		{
			stored_exceptions.emplace(name, std::current_exception());

			try
			{
				throw;
			}
			catch (const Poco::Exception & e)
			{
				LOG_ERROR(log, "Cannot update external dictionary '" << name
					<< "'! You must resolve this manually. " << e.displayText());
			}
			catch (const std::exception & e)
			{
				LOG_ERROR(log, "Cannot update external dictionary '" << name
					<< "'! You must resolve this manually. " << e.what());
			}
			catch (...)
			{
				LOG_ERROR(log, "Cannot update external dictionary '" << name
					<< "'! You must resolve this manually.");
			}
		}
	}
}

void ExternalDictionaries::reloadFromFile(const std::string & config_path, const bool throw_on_error)
{
	const Poco::File config_file{config_path};

	if (config_path.empty() || !config_file.exists())
	{
		LOG_WARNING(log, "config file '" + config_path + "' does not exist");
	}
	else
	{
		auto it = last_modification_times.find(config_path);
		if (it == std::end(last_modification_times))
			it = last_modification_times.emplace(config_path, Poco::Timestamp{0}).first;
		auto & config_last_modified = it->second;

		const auto last_modified = config_file.getLastModified();
		if (last_modified > config_last_modified)
		{
			/// definitions of dictionaries may have changed, recreate all of them
			config_last_modified = last_modified;

			const auto config = new Poco::Util::XMLConfiguration{config_path};
			SCOPE_EXIT(config->release());

			/// get all dictionaries' definitions
			Poco::Util::AbstractConfiguration::Keys keys;
			config->keys(keys);

			/// for each dictionary defined in xml config
			for (const auto & key : keys)
			{
				std::string name;

				if (0 != strncmp(key.data(), "dictionary", strlen("dictionary")))
				{
					if (0 != strncmp(key.data(), "comment", strlen("comment")))
						LOG_WARNING(log,
							config_path << ": unknown node in dictionaries file: '" << key + "', 'dictionary'");

					continue;
				}

				try
				{
					name = config->getString(key + ".name");
					if (name.empty())
					{
						LOG_WARNING(log, config_path << ": dictionary name cannot be empty");
						continue;
					}

					auto it = dictionaries.find(name);
					if (it != std::end(dictionaries))
						if (it->second.second != config_path)
							throw std::runtime_error{"Overriding dictionary from file " + it->second.second};

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

					/// add new dictionary or update an existing version
					if (it == std::end(dictionaries))
					{
						const std::lock_guard<std::mutex> lock{dictionaries_mutex};
						dictionaries.emplace(name, dictionary_origin_pair_t{
							std::make_shared<MultiVersion<IDictionary>>(dict_ptr.release()),
							config_path
						});
					}
					else
						it->second.first->set(dict_ptr.release());

					/// erase stored exception on success
					stored_exceptions.erase(name);
				}
				catch (...)
				{
					const auto exception_ptr = std::current_exception();
					if (!name.empty())
						stored_exceptions.emplace(name, exception_ptr);

					try
					{
						throw;
					}
					catch (const Poco::Exception & e)
					{
						LOG_ERROR(log, config_path << ": cannot create external dictionary '" << name
							<< "'! You must resolve this manually. " << e.displayText());
					}
					catch (const std::exception & e)
					{
						LOG_ERROR(log, config_path << ": cannot create external dictionary '" << name
							<< "'! You must resolve this manually. " << e.what());
					}
					catch (...)
					{
						LOG_ERROR(log, config_path << ": cannot create external dictionary '" << name
							<< "'! You must resolve this manually.");
					}

					/// propagate exception
					if (throw_on_error)
						std::rethrow_exception(exception_ptr);
				}
			}
		}
	}
}

}
