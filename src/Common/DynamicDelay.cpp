#include <Common/DynamicDelay.h>
#include <Common/DelayWithJitter.h>

#include <algorithm>

namespace DB
{

namespace
{

double getMinDelay(double min_delay, double max_delay)
{
    min_delay = std::min(min_delay, max_delay);
    return std::max(1.0, min_delay);
}

double getMaxDelay(double min_delay, double max_delay)
{
    max_delay = std::max(min_delay, max_delay);
    return std::max(1.0, max_delay);
}

double calibrateDelay(double delay, double min_delay, double max_delay)
{
    delay = std::max(delay, min_delay);
    delay = std::min(delay, max_delay);
    return std::max(1.0, delay);
}

}

void DynamicDelay::setConfiguration(double min_delay_, double max_delay_, double factor_)
{
    setConfiguration(min_delay_, max_delay_, factor_, factor_);
}

void DynamicDelay::setConfiguration(double min_delay_, double max_delay_, double factor_up_, double factor_lower_)
{
    min_delay = getMinDelay(min_delay_, max_delay_);
    max_delay = getMaxDelay(min_delay_, max_delay_);
    delay = calibrateDelay(delay, min_delay_, max_delay_);
    factor_up = factor_up_;
    factor_lower = factor_lower_;
}

uint64_t DynamicDelay::getCurrentDelay() const
{
    return static_cast<uint64_t>(delay);
}

uint64_t DynamicDelay::getCurrentDelayWithJitter(int64_t min_deviation, int64_t max_deviation) const
{
    return DelayWithJitter(static_cast<int64_t>(delay)).getDelayWithJitter(min_deviation, max_deviation);
}

void DynamicDelay::up()
{
    delay = calibrateDelay(delay * factor_up, min_delay, max_delay);
}

void DynamicDelay::lower()
{
    delay = calibrateDelay(delay / factor_lower, min_delay, max_delay);
}

void DynamicDelay::rotateToMin()
{
    delay = min_delay;
}

void DynamicDelay::rotateToMax()
{
    delay = max_delay;
}

}
