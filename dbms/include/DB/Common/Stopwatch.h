#pragma once

#include <time.h>
#include <mutex>
#include <Poco/ScopedLock.h>
#include <common/Common.h>


/** Отличается от Poco::Stopwatch только тем, что использует clock_gettime вместо gettimeofday,
  * возвращает наносекунды вместо микросекунд, а также другими незначительными отличиями.
  */
class Stopwatch
{
public:
	/** CLOCK_MONOTONIC работает сравнительно эффективно (~15 млн. вызовов в сек.) и не приводит к системному вызову.
	  * Поставьте CLOCK_MONOTONIC_COARSE, если нужна больше производительность, но достаточно погрешности в несколько мс.
	  */
	Stopwatch(clockid_t clock_type_ = CLOCK_MONOTONIC) : clock_type(clock_type_) { restart(); }

	void start() 					{ setStart(); is_running = true; }
	void stop() 					{ updateElapsed(); is_running = false; }
	void restart() 					{ elapsed_ns = 0; start(); }
	UInt64 elapsed() const 			{ updateElapsed(); return elapsed_ns; }
	double elapsedSeconds() const	{ updateElapsed(); return static_cast<double>(elapsed_ns) / 1000000000ULL; }

private:
	mutable UInt64 start_ns;
	mutable UInt64 elapsed_ns;
	clockid_t clock_type;
	bool is_running;

	void setStart()
	{
		struct timespec ts;
		clock_gettime(clock_type, &ts);
		start_ns = ts.tv_sec * 1000000000ULL + ts.tv_nsec;
	}

	void updateElapsed() const
	{
		if (is_running)
		{
			struct timespec ts;
			clock_gettime(clock_type, &ts);
			UInt64 current_ns = ts.tv_sec * 1000000000ULL + ts.tv_nsec;
			elapsed_ns += current_ns - start_ns;
			start_ns = current_ns;
		}
	}
};


class StopwatchWithLock : public Stopwatch
{
public:
	/** Если прошло указанное количество секунд, то перезапускает таймер и возвращает true.
	  * Иначе возвращает false.
	  * thread-safe.
	  */
	bool lockTestAndRestart(double seconds)
	{
		std::lock_guard<std::mutex> lock(mutex);

		if (elapsedSeconds() >= seconds)
		{
			restart();
			return true;
		}
		else
			return false;
	}

private:
	std::mutex mutex;
};
