#pragma once

#include <mutex>
#include <memory>
#include <Common/Stopwatch.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}


/** Позволяет ограничить скорость чего либо (в штуках в секунду) с помощью sleep.
  * Особенности работы:
  * - считается только средняя скорость, от момента первого вызова функции add;
  *   если были периоды с низкой скоростью, то в течение промежутка времени после них, скорость будет выше;
  *
  * Также позволяет задать ограничение на максимальное количество в штуках. При превышении кидается исключение.
  */
class Throttler
{
public:
    Throttler(size_t max_speed_, size_t limit_, const char * limit_exceeded_exception_message_)
        : max_speed(max_speed_), limit(limit_), limit_exceeded_exception_message(limit_exceeded_exception_message_) {}

    void add(size_t amount)
    {
        size_t new_count;
        UInt64 elapsed_ns = 0;

        {
            std::lock_guard<std::mutex> lock(mutex);

            if (max_speed)
            {
                if (0 == count)
                {
                    watch.start();
                    elapsed_ns = 0;
                }
                else
                    elapsed_ns = watch.elapsed();
            }

            count += amount;
            new_count = count;
        }

        if (limit && new_count > limit)
            throw Exception(limit_exceeded_exception_message + std::string(" Maximum: ") + toString(limit), ErrorCodes::LIMIT_EXCEEDED);

        if (max_speed)
        {
            /// Сколько должно было бы пройти времени, если бы скорость была равна max_speed.
            UInt64 desired_ns = new_count * 1000000000 / max_speed;

            if (desired_ns > elapsed_ns)
            {
                UInt64 sleep_ns = desired_ns - elapsed_ns;
                timespec sleep_ts;
                sleep_ts.tv_sec = sleep_ns / 1000000000;
                sleep_ts.tv_nsec = sleep_ns % 1000000000;
                nanosleep(&sleep_ts, nullptr);    /// NOTE Завершается раньше в случае сигнала. Это считается нормальным.
            }
        }
    }

private:
    size_t max_speed = 0;
    size_t count = 0;
    size_t limit = 0;        /// 0 - не ограничено.
    const char * limit_exceeded_exception_message = nullptr;
    Stopwatch watch {CLOCK_MONOTONIC_COARSE};
    std::mutex mutex;
};


using ThrottlerPtr = std::shared_ptr<Throttler>;

}
