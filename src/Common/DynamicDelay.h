#pragma once

#include <cstdint>

namespace DB
{

/// Maintains dynamically changing delay, convenient to use in periodic tasks.
/// Not thread safe.
class DynamicDelay
{
public:
    void setConfiguration(double min_delay_, double max_delay_, double factor_);
    void setConfiguration(double min_delay_, double max_delay_, double factor_up_, double factor_lower_);

    /// Returns instant value of delay.
    uint64_t getCurrentDelay() const;
    uint64_t getCurrentDelayWithJitter(int64_t min_deviation, int64_t max_deviation) const;

    void up();
    void lower();

    void rotateToMin();
    void rotateToMax();

private:
    double min_delay = 0;
    double max_delay = 0;
    double delay = 0;
    double factor_up = 1;
    double factor_lower = 1;
};

}
