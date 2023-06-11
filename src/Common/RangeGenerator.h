#pragma once

#include <optional>
#include <cmath>

namespace DB
{

class RangeGenerator
{
public:
    explicit RangeGenerator(size_t total_size_, size_t range_step_, size_t range_start = 0)
        : from(range_start), range_step(range_step_), total_size(total_size_)
    {
    }

    size_t totalRanges() const { return static_cast<size_t>(ceil(static_cast<float>(total_size - from) / range_step)); }

    using Range = std::pair<size_t, size_t>;

    // return upper exclusive range of values, i.e. [from_range, to_range>
    std::optional<Range> nextRange()
    {
        if (from >= total_size)
        {
            return std::nullopt;
        }

        auto to = from + range_step;
        if (to >= total_size)
        {
            to = total_size;
        }

        Range range{from, to};
        from = to;
        return range;
    }

private:
    size_t from;
    size_t range_step;
    size_t total_size;
};

}
