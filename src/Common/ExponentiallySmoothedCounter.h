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
  * just calculate x_old * weight + x_new * (1 - weight), where weight is an exponent of (negative) time passed.
  *
  * It is often used for resource usage metrics. For example, "load average" in Linux is exponentially smoothed moving average.
  * We can use exponentially smoothed counters in query scheduler.
  *
  * It is possible to update the value with values in monotonic order of time.
  * If it is updated with non-monotonic order, the calculation becomes non-deterministic.
  */

template <typename Derived>
struct ExponentiallySmoothedBase
{
    double value = 0;
    /// So the first update will have weight near 1. Cannot use -inf becuase subtraction will lead to nan.
    double update_time = std::numeric_limits<double>::lowest();

    ExponentiallySmoothedBase()
    {
    }

    ExponentiallySmoothedBase(double current_value, double current_time)
        : value(current_value), update_time(current_time)
    {
    }

    double decay(double current_time, double prev_time, double half_decay_time) const
    {
        return exp2((prev_time - current_time) / half_decay_time);
    }

    double get(double current_time, double half_decay_time) const
    {
        return value * decay(current_time, update_time, half_decay_time);
    }

    double refresh(double current_time, double half_decay_time)
    {
        value = get(current_time, half_decay_time);
    }

    void merge(const ExponentiallySmoothedBase & other, double half_decay_time)
    {
        static_cast<Derived *>(this)->add(other.value, other.update_time, half_decay_time);
    }

    bool less(const ExponentiallySmoothedBase & other, double half_decay_time) const
    {
        return get(other.update_time, half_decay_time) < other.value;
    }
};

struct ExponentiallySmoothedAverage : ExponentiallySmoothedBase<ExponentiallySmoothedAverage>
{
    void add(double new_value, double current_time, double half_decay_time)
    {
        if (current_time > update_time)
        {
            /// Add newer value.
            double old_value_weight = decay(current_time, update_time, half_decay_time);
            value = value * old_value_weight + new_value * (1 - old_value_weight);
            update_time = current_time;
        }
        else
        {
            /// Add older value.
            double new_value_weight = decay(update_time, current_time, half_decay_time);
            value = value * (1 - new_value_weight) + new_value * new_value_weight;
        }
    }
};

struct ExponentiallySmoothedSum : ExponentiallySmoothedBase<ExponentiallySmoothedSum>
{
    void add(double new_value, double current_time, double half_decay_time)
    {
        if (current_time > update_time)
        {
            /// Add newer value.
            double old_value_weight = decay(current_time, update_time, half_decay_time);
            value = value * old_value_weight + new_value;
            update_time = current_time;
        }
        else
        {
            /// Add older value.
            double new_value_weight = decay(update_time, current_time, half_decay_time);
            value = value + new_value * new_value_weight;
        }
    }
};

}
