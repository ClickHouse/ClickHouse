#pragma once

#include <cstdint>

namespace DB
{

class DelayWithJitter
{
public:
    explicit DelayWithJitter(int64_t delay_);

    uint64_t getDelay() const;
    uint64_t getDelayWithJitter(int64_t min_deviation, int64_t max_deviation) const;

private:
    uint64_t delay = 0;
};

}
