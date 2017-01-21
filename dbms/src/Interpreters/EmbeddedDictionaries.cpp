#include <common/logger_useful.h>
#include <Poco/Util/Application.h>
#include <DB/Interpreters/EmbeddedDictionaries.h>
#include <DB/Dictionaries/Embedded/RegionsHierarchies.h>
#include <DB/Dictionaries/Embedded/TechDataHierarchy.h>
#include <DB/Dictionaries/Embedded/RegionsNames.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/Exception.h>


namespace DB
{

void EmbeddedDictionaries::handleException(const bool throw_on_error) const
{
	const auto exception_ptr = std::current_exception();

	tryLogCurrentException(log, "Cannot load dictionary! You must resolve this manually.");

	if (throw_on_error)
		std::rethrow_exception(exception_ptr);
}


template <typename Dictionary>
bool EmbeddedDictionaries::reloadDictionary(MultiVersion<Dictionary> & dictionary, const bool throw_on_error)
{
	if (Dictionary::isConfigured() && (!is_fast_start_stage || !dictionary.get()))
	{
		try
		{
			auto new_dictionary = std::make_unique<Dictionary>();
			new_dictionary->reload();
			dictionary.set(new_dictionary.release());
		}
		catch (...)
		{
			handleException(throw_on_error);
			return false;
		}
	}

	return true;
}


bool EmbeddedDictionaries::reloadImpl(const bool throw_on_error)
{
	/** Если не удаётся обновить справочники, то несмотря на это, не кидаем исключение (используем старые справочники).
		* Если старых корректных справочников нет, то при использовании функций, которые от них зависят,
		*  будет кидаться исключение.
		* Производится попытка загрузить каждый справочник по-отдельности.
		*/

	LOG_INFO(log, "Loading dictionaries.");

	bool was_exception = false;

	if (!reloadDictionary<TechDataHierarchy>(tech_data_hierarchy, throw_on_error))
		was_exception = true;

	if (!reloadDictionary<RegionsHierarchies>(regions_hierarchies, throw_on_error))
		was_exception = true;

	if (!reloadDictionary<RegionsNames>(regions_names, throw_on_error))
		was_exception = true;

	if (!was_exception)
		LOG_INFO(log, "Loaded dictionaries.");

	return !was_exception;
}


void EmbeddedDictionaries::reloadPeriodically()
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


EmbeddedDictionaries::EmbeddedDictionaries(const bool throw_on_error, const int reload_period_)
	: reload_period(reload_period_), log(&Logger::get("EmbeddedDictionaries"))
{
	reloadImpl(throw_on_error);
	reloading_thread = std::thread([this] { reloadPeriodically(); });
}


EmbeddedDictionaries::EmbeddedDictionaries(const bool throw_on_error)
	: EmbeddedDictionaries(throw_on_error,
		Poco::Util::Application::instance().config().getInt("builtin_dictionaries_reload_interval", 3600))
{}


EmbeddedDictionaries::~EmbeddedDictionaries()
{
	destroy.set();
	reloading_thread.join();
}


}
