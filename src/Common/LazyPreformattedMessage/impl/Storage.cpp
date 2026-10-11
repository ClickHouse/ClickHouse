#include <Common/LazyPreformattedMessage/impl/Storage.h>
#include <Common/Exception.h>

#include <array>
#include <cstddef>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}

namespace DB::LazyPreformattedMessageImpl
{

namespace
{

constexpr size_t lanes_count = 16;
constexpr size_t lane_size = 256;

struct Lane
{
    alignas(std::max_align_t) std::byte data[lane_size]{};
    bool in_use = false;
};

thread_local std::array<Lane, lanes_count> lanes;

}

std::pair<size_t, void *> Storage::allocate(uint64_t hint, size_t size, size_t align)
{
    if (size > lane_size || align > alignof(std::max_align_t))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lazy message arguments of size {} and alignment {} do not fit a storage lane of {} bytes", size, align, lane_size);

    for (size_t step = 0; step < lanes_count; ++step)
    {
        size_t i = (hint + step) % lanes_count;
        Lane & lane = lanes[i];
        if (!lane.in_use)
        {
            lane.in_use = true;
            return {i, lane.data};
        }
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "All {} lazy message storage lanes are in use", lanes_count);
}

void Storage::deallocate(size_t lane) noexcept
{
    lanes[lane].in_use = false;
}

}
