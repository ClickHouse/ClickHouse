#pragma once

#include <thread>
#include <common/MultiVersion.h>
#include <common/logger_useful.h>
#include <DB/Dictionaries/Embedded/RegionsHierarchies.h>
#include <DB/Dictionaries/Embedded/TechDataHierarchy.h>
#include <DB/Dictionaries/Embedded/RegionsNames.h>
#include <DB/Common/setThreadName.h>


namespace DB
{

class Context;

/// Metrica's Dictionaries which can be used in functions.

class Dictionaries
{
private:
	MultiVersion<RegionsHierarchies> regions_hierarchies;
	MultiVersion<TechDataHierarchy> tech_data_hierarchy;
	MultiVersion<RegionsNames> regions_names;

	/// Directories' updating periodicity (in seconds).
	int reload_period;
	int cur_reload_period = 1;
	bool is_fast_start_stage = true;

	std::thread reloading_thread;
	Poco::Event destroy;

	Logger * log = &Logger::get("Dictionaries");


	void handleException(const bool throw_on_error) const
	{
		const auto exception_ptr = std::current_exception();

		tryLogCurrentException(log, "Cannot load dictionary! You must resolve this manually.");

		if (throw_on_error)
			std::rethrow_exception(exception_ptr);
	}


	/// Updates dictionaries.
	bool reloadImpl(const bool throw_on_error)
	{
		/** Если не удаётся обновить справочники, то несмотря на это, не кидаем исключение (используем старые справочники).
		  * Если старых корректных справочников нет, то при использовании функций, которые от них зависят,
		  *  будет кидаться исключение.
		  * Производится попытка загрузить каждый справочник по-отдельности.
		  */

		LOG_INFO(log, "Loading dictionaries.");

		auto & config = Poco::Util::Application::instance().config();

		bool was_exception = false;

		if (config.has(TechDataHierarchy::required_key) && (!is_fast_start_stage || !tech_data_hierarchy.get()))
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

		if (config.has(RegionsHierarchies::required_key) && (!is_fast_start_stage || !regions_hierarchies.get()))
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

		if (config.has(RegionsNames::required_key) && (!is_fast_start_stage || !regions_names.get()))
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

		return !was_exception;
	}


	/** Updates directories (dictionaries) every reload_period seconds.
	 * If all dictionaries are not loaded at least once, try reload them with exponential delay (1, 2, ... reload_period).
	 * If all dictionaries are loaded, update them using constant reload_period delay.
	 */
	void reloadPeriodically()
	{
		setThreadName("DictReload");

		while (true)
		{
			if (destroy.tryWait(cur_reload_period * 1000))
				return;

			if (reloadImpl(false))
			{
				/// Success
				cur_reload_period = reload_period;
				is_fast_start_stage = false;
			}

			if (is_fast_start_stage)
			{
				cur_reload_period = std::min(reload_period, 2 * cur_reload_period); /// exponentially increase delay
				is_fast_start_stage = cur_reload_period < reload_period; /// leave fast start state
			}
		}
	}

public:
	/// Every reload_period seconds directories are updated inside a separate thread.
	Dictionaries(const bool throw_on_error, const int reload_period_)
		: reload_period(reload_period_)
	{
		reloadImpl(throw_on_error);
		reloading_thread = std::thread([this] { reloadPeriodically(); });
	}

	Dictionaries(const bool throw_on_error)
		: Dictionaries(throw_on_error,
			Poco::Util::Application::instance().config().getInt("builtin_dictionaries_reload_interval", 3600))
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
