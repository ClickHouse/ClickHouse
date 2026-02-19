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
    /// The sum. It contains the last value and all previous values scaled accordingly to the difference of their time to the reference time.
    /// Older values are summed with exponentially smaller coefficients.
    /// To obtain the average, you have to divide this value to the sum of all coefficients (see 'sumWeights').

    double value = 0;

    /// The point of reference. You can translate the value to a different point of reference (see 'remap').
    /// You can imagine that the value exponentially decays over time.
    /// But it is also meaningful to treat the whole counters as constants over time but in another non-linear coordinate system,
    /// that inflates over time, while the counter itself does not change
    /// (it continues to be the same physical quantity, but only changes its representation in the "usual" coordinate system).

    /// Recap: the whole counter is one dimensional and it can be represented as a curve formed by two dependent coordinates in 2d plane,
    /// the space can be represented by (value, time) coordinates, and the curves will be exponentially decaying over time,
    /// alternatively the space can be represented by (exponentially_adjusted_value, time) and then the curves will be constant over time.

    /// Also useful analogy is the exponential representation of a number: x = a * exp(b) = a * e (where e = exp(b))
    /// a number x is represented by a curve in 2d plane that can be parametrized by coordinates (a, b) or (a, e).

    double time = 0;


    ExponentiallySmoothedAverage() = default;

    ExponentiallySmoothedAverage(double current_value, double current_time)
        : value(current_value), time(current_time)
    {
    }

    /// How much value decays after time_passed.
    static double scale(double time_passed, double half_decay_time)
    {
        return exp2(-time_passed / half_decay_time);
    }

    /// Sum of weights of all values. Divide by it to get the average.
    static double sumWeights(double half_decay_time)
    {
        double k = scale(1.0, half_decay_time);
        return 1 / (1 - k);
    }

    /// Obtain the same counter in another point of reference.
    ExponentiallySmoothedAverage remap(double current_time, double half_decay_time) const
    {
        return ExponentiallySmoothedAverage(value * scale(current_time - time, half_decay_time), current_time);
    }

    /// Merge two counters. It is done by moving to the same point of reference and summing the values.
    static ExponentiallySmoothedAverage merge(const ExponentiallySmoothedAverage & a, const ExponentiallySmoothedAverage & b, double half_decay_time)
    {
        if (a.time > b.time)
            return ExponentiallySmoothedAverage(a.value + b.remap(a.time, half_decay_time).value, a.time);
        if (a.time < b.time)
            return ExponentiallySmoothedAverage(b.value + a.remap(b.time, half_decay_time).value, b.time);

        return ExponentiallySmoothedAverage(a.value + b.value, a.time);
    }

    void merge(const ExponentiallySmoothedAverage & other, double half_decay_time)
    {
        *this = merge(*this, other, half_decay_time);
    }

    void add(double new_value, double current_time, double half_decay_time)
    {
        merge(ExponentiallySmoothedAverage(new_value, current_time), half_decay_time);
    }

    /// Calculate the average from the sum.
    double get(double half_decay_time) const
    {
        return value / sumWeights(half_decay_time);
    }

    double get(double current_time, double half_decay_time) const
    {
        return remap(current_time, half_decay_time).get(half_decay_time);
    }

    /// Compare two counters (by moving to the same point of reference and comparing sums).
    /// You can store the counters in container and sort it without changing the stored values over time.
    bool less(const ExponentiallySmoothedAverage & other, double half_decay_time) const
    {
        return remap(other.time, half_decay_time).value < other.value;
    }
};

}
