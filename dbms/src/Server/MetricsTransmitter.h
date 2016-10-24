#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>

#include <DB/Common/ProfileEvents.h>


namespace DB
{

/**	Automatically sends
  * - difference of ProfileEvents;
  * - values of CurrentMetrics;
  * - values of ActiveMetrics;
  *  to Graphite at beginning of every minute.
  */
class MetricsTransmitter
{
public:
	~MetricsTransmitter();

private:
	void run();
	void transmit();

	/// Values of ProfileEvents counters at previous iteration (or zeros at first time).
	decltype(ProfileEvents::counters) prev_counters{};

	bool quit = false;
	std::mutex mutex;
	std::condition_variable cond;
	std::thread thread {&MetricsTransmitter::run, this};

	static constexpr auto profile_events_path_prefix = "ClickHouse.ProfileEvents.";
	static constexpr auto current_metrics_path_prefix = "ClickHouse.Metrics.";
	static constexpr auto active_metrics_path_prefix = "ClickHouse.ActiveMetrics.";
};

}
