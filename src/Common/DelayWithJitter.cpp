#include <Common/DelayWithJitter.h>
#include <Common/thread_local_rng.h>

#include <algorithm>
#include <random>

namespace DB
{

DelayWithJitter::DelayWithJitter(int64_t delay_)
    : delay(std::max<int64_t>(0, delay_))
{
}

uint64_t DelayWithJitter::getDelay() const
{
    return delay;
}

uint64_t DelayWithJitter::getDelayWithJitter(int64_t min_deviation, int64_t max_deviation) const
{
    std::uniform_real_distribution<double> shift(static_cast<double>(min_deviation), static_cast<double>(max_deviation));
    return std::max<int64_t>(0, static_cast<int64_t>(static_cast<double>(delay) + shift(thread_local_rng)));
}

}
