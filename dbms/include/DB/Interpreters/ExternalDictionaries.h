#pragma once

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <Yandex/MultiVersion.h>
#include <Yandex/logger_useful.h>
#include <Poco/Event.h>
#include <time.h>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <chrono>
#include <random>
#include <unistd.h>

namespace DB
{

class Context;
class IDictionary;

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

	using dictionary_ptr_t = std::shared_ptr<MultiVersion<IDictionary>>;
	using dictionary_origin_pair_t = std::pair<dictionary_ptr_t, std::string>;
	std::unordered_map<std::string, dictionary_origin_pair_t> dictionaries;
	/// exception pointers for notifying user about failures on dictionary creation
	std::unordered_map<std::string, std::exception_ptr> stored_exceptions;
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
		return ts.tv_nsec ^ getpid();
	}

public:
	/// Справочники будут обновляться в отдельном потоке, каждые reload_period секунд.
	ExternalDictionaries(Context & context, const bool throw_on_error)
		: context(context), log(&Logger::get("ExternalDictionaries"))
	{
		reloadImpl(throw_on_error);
		reloading_thread = std::thread{&ExternalDictionaries::reloadPeriodically, this};
	}

	~ExternalDictionaries()
	{
		destroy.set();
		reloading_thread.join();
	}

	MultiVersion<IDictionary>::Version getDictionary(const std::string & name) const
	{
		const std::lock_guard<std::mutex> lock{dictionaries_mutex};
		const auto it = dictionaries.find(name);
		if (it == std::end(dictionaries))
		{
			const auto exception_it = stored_exceptions.find(name);
			if (exception_it != std::end(stored_exceptions))
				std::rethrow_exception(exception_it->second);
			else
				throw Exception{
					"No such dictionary: " + name,
					ErrorCodes::BAD_ARGUMENTS
				};
		}

		return it->second.first->get();
	}
};

}
