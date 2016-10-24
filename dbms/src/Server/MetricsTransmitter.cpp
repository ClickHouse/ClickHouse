#include "MetricsTransmitter.h"

#include <daemon/BaseDaemon.h>
#include <DB/Common/setThreadName.h>
#include <DB/Common/CurrentMetrics.h>


namespace DB
{

MetricsTransmitter::~MetricsTransmitter()
{
	try
	{
		{
			std::lock_guard<std::mutex> lock{mutex};
			quit = true;
		}

		cond.notify_one();

		thread.join();
	}
	catch (...)
	{
		DB::tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}


void MetricsTransmitter::run()
{
	setThreadName("MetricsTransmit");

	/// Next minute at 00 seconds. To avoid time drift and transmit values exactly each minute.
	const auto get_next_minute = []
	{
		return std::chrono::time_point_cast<std::chrono::minutes, std::chrono::system_clock>(
			std::chrono::system_clock::now() + std::chrono::minutes(1));
	};

	std::unique_lock<std::mutex> lock{mutex};

	while (true)
	{
		if (cond.wait_until(lock, get_next_minute(), [this] { return quit; }))
			break;

		transmit();
	}
}


void MetricsTransmitter::transmit()
{
	GraphiteWriter::KeyValueVector<ssize_t> key_vals{};
	key_vals.reserve(ProfileEvents::end() + CurrentMetrics::END);

	for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
	{
		const auto counter = ProfileEvents::counters[i].load(std::memory_order_relaxed);
		const auto counter_increment = counter - prev_counters[i].load(std::memory_order_relaxed);
		prev_counters[i].store(counter, std::memory_order_relaxed);

		std::string key {ProfileEvents::getDescription(static_cast<ProfileEvents::Event>(i))};
		key_vals.emplace_back(profile_events_path_prefix + key, counter_increment);
	}

	for (size_t i = 0; i < CurrentMetrics::END; ++i)
	{
		const auto value = CurrentMetrics::values[i].load(std::memory_order_relaxed);

		std::string key {CurrentMetrics::getDescription(static_cast<CurrentMetrics::Metric>(i))};
		key_vals.emplace_back(current_metrics_path_prefix + key, value);
	}

	BaseDaemon::instance().writeToGraphite(key_vals);
}

}
