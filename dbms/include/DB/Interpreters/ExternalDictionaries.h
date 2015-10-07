#pragma once

#include <DB/Dictionaries/IDictionary.h>
#include <DB/Common/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Common/setThreadName.h>
#include <common/MultiVersion.h>
#include <common/logger_useful.h>
#include <Poco/Event.h>
#include <unistd.h>
#include <time.h>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <chrono>
#include <random>

namespace DB
{

class Context;

/** Manages user-defined dictionaries.
*	Monitors configuration file and automatically reloads dictionaries in a separate thread.
*	The monitoring thread wakes up every @check_period_sec seconds and checks
*	modification time of dictionaries' configuration file. If said time is greater than
*	@config_last_modified, the dictionaries are created from scratch using configuration file,
*	possibly overriding currently existing dictionaries with the same name (previous versions of
*	overridden dictionaries will live as long as there are any users retaining them).
*
*	Apart from checking configuration file for modifications, each non-cached dictionary
*	has a lifetime of its own and may be updated if it's source reports that it has been
*	modified. The time of next update is calculated by choosing uniformly a random number
*	distributed between lifetime.min_sec and lifetime.max_sec.
*	If either of lifetime.min_sec and lifetime.max_sec is zero, such dictionary is never updated.
*/
class ExternalDictionaries
{
private:
	static const auto check_period_sec = 5;

	friend class StorageSystemDictionaries;

	mutable std::mutex dictionaries_mutex;

	using dictionary_ptr_t = std::shared_ptr<MultiVersion<IDictionaryBase>>;
	struct dictionary_info final
	{
		dictionary_ptr_t dict;
		std::string origin;
		std::exception_ptr exception;
	};

	struct failed_dictionary_info final
	{
		std::unique_ptr<IDictionaryBase> dict;
		std::chrono::system_clock::time_point next_attempt_time;
		std::uint64_t error_count;
	};

	/** Имя словаря -> словарь.
	  */
	std::unordered_map<std::string, dictionary_info> dictionaries;

	/** Здесь находятся словари, которых ещё ни разу не удалось загрузить.
	  * В dictionaries они тоже присутствуют, но с нулевым указателем dict.
	  */
	std::unordered_map<std::string, failed_dictionary_info> failed_dictionaries;

	/** И для обычных и для failed_dictionaries.
	  */
	std::unordered_map<std::string, std::chrono::system_clock::time_point> update_times;

	std::mt19937_64 rnd_engine{getSeed()};

	Context & context;

	std::thread reloading_thread;
	Poco::Event destroy;

	Logger * log;

	std::unordered_map<std::string, Poco::Timestamp> last_modification_times;

	void reloadImpl(bool throw_on_error = false);
	void reloadFromFile(const std::string & config_path, bool throw_on_error);

	void reloadPeriodically()
	{
		setThreadName("ExterDictReload");

		while (true)
		{
			if (destroy.tryWait(check_period_sec * 1000))
				return;

			reloadImpl();
		}
	}

	static std::uint64_t getSeed()
	{
		timespec ts;
		clock_gettime(CLOCK_MONOTONIC, &ts);
		return static_cast<std::uint64_t>(ts.tv_nsec ^ getpid());
	}

public:
	/// Справочники будут обновляться в отдельном потоке, каждые reload_period секунд.
	ExternalDictionaries(Context & context, const bool throw_on_error)
		: context(context), log(&Logger::get("ExternalDictionaries"))
	{
		{
			/** При синхронной загрузки внешних словарей в момент выполнения запроса,
			  *  не нужно использовать ограничение на расход памяти запросом.
			  */
			struct TemporarilyDisableMemoryTracker
			{
				MemoryTracker * memory_tracker;

				TemporarilyDisableMemoryTracker()
				{
					memory_tracker = current_memory_tracker;
					current_memory_tracker = nullptr;
				}

				~TemporarilyDisableMemoryTracker()
				{
					current_memory_tracker = memory_tracker;
				}
			} temporarily_disable_memory_tracker;

			reloadImpl(throw_on_error);
		}

		reloading_thread = std::thread{&ExternalDictionaries::reloadPeriodically, this};
	}

	~ExternalDictionaries()
	{
		destroy.set();
		reloading_thread.join();
	}

	MultiVersion<IDictionaryBase>::Version getDictionary(const std::string & name) const;
};

}
