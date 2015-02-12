#pragma once

#include <mutex>
#include <memory>
#include <statdaemons/Stopwatch.h>


/** Позволяет ограничить скорость чего либо (в штуках в секунду) с помощью sleep.
  * Особенности работы:
  * - считается только средняя скорость, от момента первого вызова функции add;
  *   если были периоды с низкой скоростью, то в течение промежутка времени после них, скорость будет выше;
  */
class Throttler
{
public:
	Throttler(size_t max_speed_) : max_speed(max_speed_) {}

	void add(size_t amount)
	{
		size_t new_count;
		UInt64 elapsed_ns;

		{
			std::lock_guard<std::mutex> lock(mutex);

			if (0 == count)
			{
				watch.start();
				elapsed_ns = 0;
			}
			else
				elapsed_ns = watch.elapsed();

			count += amount;
			new_count = count;
		}

		/// Сколько должно было бы пройти времени, если бы скорость была равна max_speed.
		UInt64 desired_ns = new_count * 1000000000 / max_speed;

		if (desired_ns > elapsed_ns)
		{
			UInt64 sleep_ns = desired_ns - elapsed_ns;
			timespec sleep_ts;
			sleep_ts.tv_sec = sleep_ns / 1000000000;
			sleep_ts.tv_nsec = sleep_ns % 1000000000;
			nanosleep(&sleep_ts, nullptr);	/// NOTE Завершается раньше в случае сигнала. Это считается нормальным.
		}
	}

private:
	size_t max_speed;
	size_t count = 0;
	Stopwatch watch {CLOCK_MONOTONIC_COARSE};
	std::mutex mutex;
};


typedef std::shared_ptr<Throttler> ThrottlerPtr;
