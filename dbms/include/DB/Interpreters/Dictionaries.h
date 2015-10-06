#pragma once

#include <thread>
#include <common/MultiVersion.h>
#include <common/logger_useful.h>
#include <statdaemons/RegionsHierarchies.h>
#include <statdaemons/TechDataHierarchy.h>
#include <statdaemons/RegionsNames.h>
#include <DB/Common/setThreadName.h>


namespace DB
{

class Context;

/// Словари Метрики, которые могут использоваться в функциях.

class Dictionaries
{
private:
	MultiVersion<RegionsHierarchies> regions_hierarchies;
	MultiVersion<TechDataHierarchy> tech_data_hierarchy;
	MultiVersion<RegionsNames> regions_names;

	/// Периодичность обновления справочников, в секундах.
	int reload_period;

	std::thread reloading_thread;
	Poco::Event destroy;

	Logger * log;



	void handleException(const bool throw_on_error) const
	{
		const auto exception_ptr = std::current_exception();

		try
		{
			throw;
		}
		catch (const Poco::Exception & e)
		{
			LOG_ERROR(log, "Cannot load dictionary! You must resolve this manually. " << e.displayText());
		}
		catch (...)
		{
			LOG_ERROR(log, "Cannot load dictionary! You must resolve this manually.");
		}

		if (throw_on_error)
			std::rethrow_exception(exception_ptr);
	}


	/// Обновляет справочники.
	void reloadImpl(const bool throw_on_error = false)
	{
		/** Если не удаётся обновить справочники, то несмотря на это, не кидаем исключение (используем старые справочники).
		  * Если старых корректных справочников нет, то при использовании функций, которые от них зависят,
		  *  будет кидаться исключение.
		  * Производится попытка загрузить каждый справочник по-отдельности.
		  */

		LOG_INFO(log, "Loading dictionaries.");

		auto & config = Poco::Util::Application::instance().config();

		bool was_exception = false;

		if (config.has(TechDataHierarchy::required_key))
		{
			try
			{
				auto new_tech_data_hierarchy = std::make_unique<TechDataHierarchy>();
				tech_data_hierarchy.set(new_tech_data_hierarchy.release());
			}
			catch (...)
			{
				handleException(throw_on_error);
				was_exception = true;
			}
		}


		if (config.has(RegionsHierarchies::required_key))
		{
			try
			{
				auto new_regions_hierarchies = std::make_unique<RegionsHierarchies>();
				new_regions_hierarchies->reload();
				regions_hierarchies.set(new_regions_hierarchies.release());
			}
			catch (...)
			{
				handleException(throw_on_error);
				was_exception = true;
			}
		}

		if (config.has(RegionsNames::required_key))
		{
			try
			{
				auto new_regions_names = std::make_unique<RegionsNames>();
				new_regions_names->reload();
				regions_names.set(new_regions_names.release());
			}
			catch (...)
			{
				handleException(throw_on_error);
				was_exception = true;
			}
		}

		if (!was_exception)
			LOG_INFO(log, "Loaded dictionaries.");
	}



	/// Обновляет каждые reload_period секунд.
	void reloadPeriodically()
	{
		setThreadName("DictReload");

		while (true)
		{
			if (destroy.tryWait(reload_period * 1000))
				return;

			reloadImpl();
		}
	}

public:
	/// Справочники будут обновляться в отдельном потоке, каждые reload_period секунд.
	Dictionaries(const bool throw_on_error, const int reload_period_)
		: reload_period(reload_period_), log(&Logger::get("Dictionaries"))
	{
		reloadImpl(throw_on_error);
		reloading_thread = std::thread([this] { reloadPeriodically(); });
	}

	Dictionaries(const bool throw_on_error)
		: Dictionaries(throw_on_error,
					   Poco::Util::Application::instance().config()
						   .getInt("builtin_dictionaries_reload_interval", 3600))
	{}

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

	MultiVersion<RegionsNames>::Version getRegionsNames() const
	{
		return regions_names.get();
	}
};

}
