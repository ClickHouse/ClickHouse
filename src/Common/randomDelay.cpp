#include <Common/randomDelay.h>

#include <Common/logger_useful.h>
#include <Common/randomNumber.h>
#include <base/sleep.h>


void randomDelayForMaxMilliseconds(uint64_t milliseconds, LoggerPtr log, const char * start_of_message)
{
    if (milliseconds)
    {
        auto count = randomNumber() % milliseconds;

        if (log)
        {
            if (start_of_message && !*start_of_message)
                start_of_message = nullptr;

            LOG_TEST(log, "{}{}Sleeping for {} milliseconds",
                     (start_of_message ? start_of_message : ""),
                     (start_of_message ? ": " : ""),
                     count);
        }

        sleepForMilliseconds(count);

        if (log)
        {
            LOG_TEST(log, "{}{}Awaking after sleeping",
                     (start_of_message ? start_of_message : ""),
                     (start_of_message ? ": " : ""));
        }
    }
}

void randomDelayForMaxSeconds(uint64_t seconds, LoggerPtr log, const char * start_of_message)
{
    randomDelayForMaxMilliseconds(seconds * 1000, log, start_of_message);
}
