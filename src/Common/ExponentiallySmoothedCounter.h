#pragma once

#include <cmath>
#include <limits>


namespace DB
{

/** https://en.wikipedia.org/wiki/Exponential_smoothing
  *
  * Exponentially smoothed average over time is weighted average with weight proportional to negative exponent of the time passed.
  * For example, the last value is taken with weight 1/2, the value one second ago with weight 1/4, two seconds ago - 1/8, etc.
  * It can be understood as an average over sliding window, but with different kernel.
  *
  * As an advantage, it is easy to update. Instead of collecting values and calculating a series of x1 / 2 + x2 / 4 + x3 / 8...
  * just calculate x_old / 2 + x_new / 2.
  *
  * It is often used for resource usage metrics. For example, "load average" in Linux is exponentially smoothed moving average.
  * We can use exponentially smoothed counters in query scheduler.
  */
struct ExponentiallySmoothedAverage
{
    double value = 0;
    double update_time = 0;

    ExponentiallySmoothedAverage()
    {
    }

    ExponentiallySmoothedAverage(double current_value, double current_time)
        : value(current_value), update_time(current_time)
    {
    }

    static double scale(double time_passed, double half_decay_time)
    {
        return exp2(-time_passed / half_decay_time);
    }

    static double sumWeights(double half_decay_time)
    {
        double k = scale(1.0, half_decay_time);
        return 1 / (1 - k);
    }

    ExponentiallySmoothedAverage remap(double current_time, double half_decay_time) const
    {
        return ExponentiallySmoothedAverage(value * scale(current_time - update_time, half_decay_time), current_time);
    }

    static ExponentiallySmoothedAverage merge(const ExponentiallySmoothedAverage & a, const ExponentiallySmoothedAverage & b, double half_decay_time)
    {
        if (a.update_time > b.update_time)
            return ExponentiallySmoothedAverage(a.value + b.remap(a.update_time, half_decay_time).value, a.update_time);
        if (a.update_time < b.update_time)
            return ExponentiallySmoothedAverage(b.value + a.remap(b.update_time, half_decay_time).value, b.update_time);

        return ExponentiallySmoothedAverage(a.value + b.value, a.update_time);
    }

    void merge(const ExponentiallySmoothedAverage & other, double half_decay_time)
    {
        *this = merge(*this, other, half_decay_time);
    }

    void add(double new_value, double current_time, double half_decay_time)
    {
        merge(ExponentiallySmoothedAverage(new_value, current_time), half_decay_time);
    }

    double get(double half_decay_time) const
    {
        return value / sumWeights(half_decay_time);
    }

    double get(double current_time, double half_decay_time) const
    {
        return remap(current_time, half_decay_time).get(half_decay_time);
    }

    bool less(const ExponentiallySmoothedAverage & other, double half_decay_time) const
    {
        return remap(other.update_time, half_decay_time).value < other.value;
    }
};

}
