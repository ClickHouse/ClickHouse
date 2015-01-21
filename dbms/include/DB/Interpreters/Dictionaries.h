#pragma once

#include <thread>
#include <unordered_map>

#include <Poco/SharedPtr.h>

#include <Yandex/MultiVersion.h>
#include <Yandex/logger_useful.h>
#include <statdaemons/RegionsHierarchies.h>
#include <statdaemons/TechDataHierarchy.h>
#include <statdaemons/CategoriesHierarchy.h>
#include <statdaemons/RegionsNames.h>

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Dictionaries/FlatDictionary.h>


namespace DB
{

using Poco::SharedPtr;

/// Словари Метрики, которые могут использоваться в функциях.

class Dictionaries
{
private:
	MultiVersion<RegionsHierarchies> regions_hierarchies;
	MultiVersion<TechDataHierarchy> tech_data_hierarchy;
	MultiVersion<CategoriesHierarchy> categories_hierarchy;
	MultiVersion<RegionsNames> regions_names;
	std::unordered_map<std::string, std::shared_ptr<MultiVersion<IDictionary>>> external_dictionaries;

	/// Периодичность обновления справочников, в секундах.
	int reload_period;

	std::thread reloading_thread;
	Poco::Event destroy;

	Logger * log;


	void handleException() const
	{
		try
		{
			throw;
		}
		catch (const Poco::Exception & e)
		{
			LOG_ERROR(log, "Cannot load dictionary! You must resolve this manually. " << e.displayText());
			return;
		}
		catch (...)
		{
			LOG_ERROR(log, "Cannot load dictionary! You must resolve this manually.");
			return;
		}
	}


	/// Обновляет справочники.
	void reloadImpl()
	{
		/** Если не удаётся обновить справочники, то несмотря на это, не кидаем исключение (используем старые справочники).
		  * Если старых корректных справочников нет, то при использовании функций, которые от них зависят,
		  *  будет кидаться исключение.
		  * Производится попытка загрузить каждый справочник по-отдельности.
		  */

		LOG_INFO(log, "Loading dictionaries.");

		bool was_exception = false;

		try
		{
			MultiVersion<TechDataHierarchy>::Version new_tech_data_hierarchy = new TechDataHierarchy;
			tech_data_hierarchy.set(new_tech_data_hierarchy);
		}
		catch (...)
		{
			handleException();
			was_exception = true;
		}

		try
		{
			MultiVersion<RegionsHierarchies>::Version new_regions_hierarchies = new RegionsHierarchies;
			new_regions_hierarchies->reload();
			regions_hierarchies.set(new_regions_hierarchies);

		}
		catch (...)
		{
			handleException();
			was_exception = true;
		}

		try
		{
			MultiVersion<CategoriesHierarchy>::Version new_categories_hierarchy = new CategoriesHierarchy;
			new_categories_hierarchy->reload();
			categories_hierarchy.set(new_categories_hierarchy);
		}
		catch (...)
		{
			handleException();
			was_exception = true;
		}

		try
		{
			MultiVersion<RegionsNames>::Version new_regions_names = new RegionsNames;
			new_regions_names->reload();
			regions_names.set(new_regions_names);
		}
		catch (...)
		{
			handleException();
			was_exception = true;
		}

		try
		{
			reloadExternalDictionaries();
		}
		catch (...)
		{
			handleException();
			was_exception = true;
		}


		if (!was_exception)
			LOG_INFO(log, "Loaded dictionaries.");
	}


	void reloadExternalDictionaries();

	/// Обновляет каждые reload_period секунд.
	void reloadPeriodically()
	{
		while (true)
		{
			if (destroy.tryWait(reload_period * 1000))
				return;

			reloadImpl();
		}
	}

public:
	/// Справочники будут обновляться в отдельном потоке, каждые reload_period секунд.
	Dictionaries(int reload_period_ = 3600)
		: reload_period(reload_period_),
		log(&Logger::get("Dictionaries"))
	{
		reloadImpl();
		reloading_thread = std::thread([this] { reloadPeriodically(); });
	}

	~Dictionaries()
	{
		destroy.set();
		reloading_thread.join();
	}

	MultiVersion<RegionsHierarchies>::Version getRegionsHierarchies() const
	{
		return regions_hierarchies.get();
	}

	MultiVersion<TechDataHierarchy>::Version getTechDataHierarchy() const
	{
		return tech_data_hierarchy.get();
	}

	MultiVersion<CategoriesHierarchy>::Version getCategoriesHierarchy() const
	{
		return categories_hierarchy.get();
	}

	MultiVersion<RegionsNames>::Version getRegionsNames() const
	{
		return regions_names.get();
	}

	MultiVersion<IDictionary>::Version getExternalDictionary(const std::string & name) const
	{
		const auto it = external_dictionaries.find(name);
		if (it == std::end(external_dictionaries))
			throw Exception{
				"No such dictionary: " + name,
				ErrorCodes::BAD_ARGUMENTS
			};

		return it->second->get();
	}
};

}
