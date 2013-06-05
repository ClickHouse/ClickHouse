#pragma once

#include <boost/thread.hpp>

#include <Poco/SharedPtr.h>

#include <Yandex/MultiVersion.h>
#include <Yandex/logger_useful.h>
#include <statdaemons/RegionsHierarchy.h>
#include <statdaemons/TechDataHierarchy.h>
#include <statdaemons/CategoriesHierarchy.h>
#include <statdaemons/RegionsNames.h>


namespace DB
{

using Poco::SharedPtr;

/// Словари Метрики, которые могут использоваться в функциях.

class Dictionaries
{
private:
	Yandex::MultiVersion<RegionsHierarchy> regions_hierarchy;
	Yandex::MultiVersion<TechDataHierarchy> tech_data_hierarchy;
	Yandex::MultiVersion<CategoriesHierarchy> categories_hierarchy;
	Yandex::MultiVersion<RegionsNames> regions_names;

	/// Периодичность обновления справочников, в секундах.
	int reload_period;

	boost::thread reloading_thread;
	Poco::Event destroy;

	Logger * log;


	/// Обновляет справочники.
	void reloadImpl()
	{
		/** Если не удаётся обновить справочники, то несмотря на это, не кидаем исключение (используем старые справочники).
		  * Если старых корректных справочников нет, то при использовании функций, которые от них зависят,
		  *  будет кидаться исключение.
		  */
		try
		{
			LOG_INFO(log, "Loading dictionaries.");

			Yandex::MultiVersion<TechDataHierarchy>::Version new_tech_data_hierarchy = new TechDataHierarchy;
			Yandex::MultiVersion<RegionsHierarchy>::Version new_regions_hierarchy = new RegionsHierarchy;
			new_regions_hierarchy->reload();
			Yandex::MultiVersion<CategoriesHierarchy>::Version new_categories_hierarchy = new CategoriesHierarchy;
			new_categories_hierarchy->reload();
			Yandex::MultiVersion<RegionsNames>::Version new_regions_names = new RegionsNames;
			new_regions_names->reload();
			
			tech_data_hierarchy.set(new_tech_data_hierarchy);
			regions_hierarchy.set(new_regions_hierarchy);
			categories_hierarchy.set(new_categories_hierarchy);
			regions_names.set(new_regions_names);
		}
		catch (const Poco::Exception & e)
		{
			LOG_ERROR(log, "Cannot load dictionaries! You must resolve this manually. " << e.displayText());
			return;
		}
		catch (...)
		{
			LOG_ERROR(log, "Cannot load dictionaries! You must resolve this manually.");
			return;
		}

		LOG_INFO(log, "Loaded dictionaries.");
	}

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
		reloading_thread = boost::thread(&Dictionaries::reloadPeriodically, this);
	}

	~Dictionaries()
	{
		destroy.set();
		reloading_thread.join();
	}

	Yandex::MultiVersion<RegionsHierarchy>::Version getRegionsHierarchy() const
	{
		return regions_hierarchy.get();
	}

	Yandex::MultiVersion<TechDataHierarchy>::Version getTechDataHierarchy() const
	{
		return tech_data_hierarchy.get();
	}
	
	Yandex::MultiVersion<CategoriesHierarchy>::Version getCategoriesHierarchy() const
	{
		return categories_hierarchy.get();
	}
	
	Yandex::MultiVersion<RegionsNames>::Version getRegionsNames() const
	{
		return regions_names.get();
	}
};

}
